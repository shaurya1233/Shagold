import json
import logging
import os
import time
import threading
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

HOST = os.getenv('HOST', '0.0.0.0')
PORT = int(os.getenv('PORT', '8080'))
API_KEY = os.getenv('METALS_DEV_API_KEY', '')
MARKET_SOURCE = os.getenv('MARKET_SOURCE', 'spot').lower()
CACHE_TTL = int(os.getenv('CACHE_TTL_SECONDS', '60'))
UPSTREAM_TIMEOUT = float(os.getenv('UPSTREAM_TIMEOUT_SECONDS', '8'))
RATE_LIMIT = int(os.getenv('RATE_LIMIT_PER_MINUTE', '60'))
CORS_ORIGINS = {x.strip() for x in os.getenv('CORS_ORIGINS', '').split(',') if x.strip()}
LIVE_MAX_AGE = int(os.getenv('LIVE_MAX_AGE_SECONDS', '180'))
RECENT_MAX_AGE = int(os.getenv('RECENT_MAX_AGE_SECONDS', '900'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
cache = None
cache_lock = threading.Lock()
rate_lock = threading.Lock()
rate_state = {}

def now_iso():
    return datetime.now(timezone.utc).isoformat().replace('+00:00','Z')

def valid_positive(v):
    try:
        n = float(v)
        return n if n > 0 else None
    except (TypeError, ValueError):
        return None

def valid_ts(v):
    if not isinstance(v, str): return None
    try:
        s = v.replace('Z', '+00:00')
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace('+00:00','Z')
    except ValueError:
        return None

def classify(provider_ts, retrieved_at):
    try:
        p = datetime.fromisoformat(provider_ts.replace('Z','+00:00')).timestamp()
        r = datetime.fromisoformat(retrieved_at.replace('Z','+00:00')).timestamp()
        age = max(0, r-p)
    except Exception:
        return 'RECENT'
    if age <= LIVE_MAX_AGE: return 'LIVE'
    if age <= RECENT_MAX_AGE: return 'RECENT'
    return 'STALE'

def client_allowed(ip):
    now = time.time()
    with rate_lock:
        arr = [t for t in rate_state.get(ip, []) if now-t < 60]
        if len(arr) >= RATE_LIMIT: return False
        arr.append(now); rate_state[ip] = arr
        if len(rate_state) > 5000:
            for key in list(rate_state)[:1000]: rate_state.pop(key, None)
        return True

def fetch_upstream():
    if not API_KEY: raise RuntimeError('MISSING_PROVIDER_KEY')
    params = {'api_key': API_KEY, 'currency': 'INR', 'unit': 'g'}
    if MARKET_SOURCE == 'ibja':
        url = 'https://api.metals.dev/v1/metal/authority?' + urlencode({**params,'authority':'ibja'})
    else:
        url = 'https://api.metals.dev/v1/metal/spot?' + urlencode({**params,'metal':'gold'})
    req = Request(url, headers={'Accept':'application/json','User-Agent':'GoldRateIndia/1.0'})
    try:
        with urlopen(req, timeout=UPSTREAM_TIMEOUT) as response:
            raw = response.read()
    except HTTPError as e:
        raise RuntimeError(f'UPSTREAM_HTTP_{e.code}')
    except URLError as e:
        raise RuntimeError('UPSTREAM_NETWORK_ERROR') from e
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError('UPSTREAM_INVALID_JSON') from e
    if data.get('status') != 'success': raise RuntimeError('UPSTREAM_REPORTED_FAILURE')
    provider_ts = valid_ts(data.get('timestamp'))
    if not provider_ts: raise RuntimeError('UPSTREAM_TIMESTAMP_MISSING_OR_INVALID')
    if data.get('currency', 'INR') != 'INR': raise RuntimeError('UPSTREAM_CURRENCY_NOT_INR')
    if MARKET_SOURCE == 'ibja':
        price = valid_positive(data.get('rates',{}).get('ibja_gold'))
        source = 'Metals.Dev — IBJA authority feed'; category = 'INDIAN AUTHORITY / REFERENCE FEED'
    else:
        price = valid_positive(data.get('rate',{}).get('price'))
        source = 'Metals.Dev — Gold spot'; category = 'GLOBAL GOLD SPOT PRICE'
    if price is None: raise RuntimeError('UPSTREAM_PRICE_MISSING_OR_INVALID')
    retrieved = now_iso()
    p24 = price; p22 = p24*(916/999); p18 = p24*(750/999)
    return {
        'success': True, 'source': source, 'marketCategory': category,
        'providerTimestamp': provider_ts, 'retrievedAt': retrieved,
        'serverNow': now_iso(), 'timezone': 'Asia/Kolkata', 'currency': 'INR',
        'status': classify(provider_ts, retrieved), 'cacheStatus': 'MISS',
        'methodology': '24K direct from upstream; 22K and 18K derived by fineness ratio only; excludes GST, duty, making charges, wastage, premiums and jeweller margin.',
        'providerDelaySeconds': max(0, (datetime.fromisoformat(retrieved.replace('Z','+00:00')).timestamp() - datetime.fromisoformat(provider_ts.replace('Z','+00:00')).timestamp())),
        'marketState': None,
        'rates': {
            '24k_999': {'perGram': p24, 'per10g': p24*10, 'type':'DIRECT'},
            '22k_916': {'perGram': p22, 'per10g': p22*10, 'type':'DERIVED'},
            '18k_750': {'perGram': p18, 'per10g': p18*10, 'type':'DERIVED'}
        }
    }

class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def _cors(self):
        origin = self.headers.get('Origin')
        if origin and (origin in CORS_ORIGINS or '*' in CORS_ORIGINS):
            self.send_header('Access-Control-Allow-Origin', '*' if '*' in CORS_ORIGINS else origin)
            self.send_header('Vary','Origin')
            self.send_header('Access-Control-Allow-Methods','GET,OPTIONS')
            self.send_header('Access-Control-Allow-Headers','Accept,Content-Type')
    def respond(self, status, body):
        raw = json.dumps(body, separators=(',',':')).encode()
        self.send_response(status); self._cors()
        self.send_header('Content-Type','application/json; charset=utf-8'); self.send_header('Cache-Control','no-store'); self.send_header('X-Content-Type-Options','nosniff'); self.send_header('Content-Length',str(len(raw))); self.end_headers(); self.wfile.write(raw)
    def do_OPTIONS(self): self.send_response(204); self._cors(); self.end_headers()
    def do_GET(self):
        ip = (self.headers.get('X-Forwarded-For') or self.client_address[0]).split(',')[0].strip()
        if not client_allowed(ip): return self.respond(429, {'success':False,'code':'RATE_LIMITED','message':'Too many requests. Retry later.'})
        if self.path.split('?',1)[0] == '/health':
            return self.respond(200, {'success':True,'status':'ok','service':'gold-rate-api','timezone':'Asia/Kolkata','providerConfigured':bool(API_KEY),'marketSource':MARKET_SOURCE,'serverTime':now_iso()})
        if self.path.split('?',1)[0] != '/api/gold': return self.respond(404, {'success':False,'code':'NOT_FOUND','message':'Route not found.'})
        global cache
        with cache_lock:
            current = cache.copy() if cache else None
        if current:
            age = time.time() - datetime.fromisoformat(current['retrievedAt'].replace('Z','+00:00')).timestamp()
            if age <= CACHE_TTL:
                current['cacheStatus']='HIT'; current['status']=classify(current['providerTimestamp'],current['retrievedAt']); current['serverNow']=now_iso(); return self.respond(200,current)
        try:
            fresh = fetch_upstream()
            with cache_lock: cache = fresh.copy()
            return self.respond(200, fresh)
        except Exception as e:
            logging.error('gold request failed: %s', e)
            if current:
                current['cacheStatus']='STALE_FALLBACK'; current['status']='STALE'; current['serverNow']=now_iso(); current['error']=str(e); return self.respond(200,current)
            code = str(e); status = 503 if code == 'MISSING_PROVIDER_KEY' else 502
            msg = 'Provider API key is not configured on the backend.' if code == 'MISSING_PROVIDER_KEY' else 'Gold provider request failed.'
            return self.respond(status, {'success':False,'code':code,'message':msg})
    def log_message(self, format, *args): logging.info('%s - %s', self.address_string(), format%args)

if __name__ == '__main__':
    logging.info('Starting gold-rate-api on %s:%s source=%s', HOST, PORT, MARKET_SOURCE)
    ThreadingHTTPServer((HOST,PORT),Handler).serve_forever()
