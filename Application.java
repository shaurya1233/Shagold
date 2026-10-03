import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

public class Application {
    static final String HOST = env("HOST", "0.0.0.0");
    static final int PORT = ienv("PORT", 8080);
    static final String API_KEY = env("METALS_DEV_API_KEY", "");
    static final String MARKET_SOURCE = env("MARKET_SOURCE", "spot").toLowerCase(Locale.ROOT);
    static final long CACHE_TTL = ienv("CACHE_TTL_SECONDS", 60);
    static final int LIVE_MAX = ienv("LIVE_MAX_AGE_SECONDS", 180);
    static final int RECENT_MAX = ienv("RECENT_MAX_AGE_SECONDS", 900);
    static final int RATE_LIMIT = ienv("RATE_LIMIT_PER_MINUTE", 60);
    static final Set<String> CORS = new HashSet<>(Arrays.asList(env("CORS_ORIGINS", "").split(",")));
    static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(8)).build();
    static final Map<String,Deque<Long>> RL = new ConcurrentHashMap<>();
    static final Object CACHE_LOCK = new Object();
    static volatile Map<String,Object> cache = null;

    static String env(String k,String d){String v=System.getenv(k);return v==null||v.isBlank()?d:v;}
    static int ienv(String k,int d){try{return Integer.parseInt(env(k,String.valueOf(d)));}catch(Exception e){return d;}}
    static String now(){return Instant.now().toString();}
    static double positive(Object v){if(v instanceof Number n && Double.isFinite(n.doubleValue()) && n.doubleValue()>0)return n.doubleValue();throw new IllegalArgumentException("INVALID_PRICE");}
    static Instant parseTs(Object v){try{return Instant.parse(String.valueOf(v));}catch(Exception e){throw new IllegalArgumentException("INVALID_TIMESTAMP");}}
    static String classify(Instant p,Instant r){long age=Math.max(0,r.getEpochSecond()-p.getEpochSecond());return age<=LIVE_MAX?"LIVE":age<=RECENT_MAX?"RECENT":"STALE";}

    static boolean allowed(String ip){
        long now=System.currentTimeMillis(); Deque<Long> q=RL.computeIfAbsent(ip,k->new ArrayDeque<>());
        synchronized(q){while(!q.isEmpty()&&now-q.peekFirst()>60000)q.pollFirst();if(q.size()>=RATE_LIMIT)return false;q.addLast(now);return true;}
    }
    static void cors(HttpExchange ex){String o=ex.getRequestHeaders().getFirst("Origin");if(o!=null&&(CORS.contains("*")||CORS.contains(o))){ex.getResponseHeaders().set("Access-Control-Allow-Origin",CORS.contains("*")?"*":o);ex.getResponseHeaders().set("Vary","Origin");ex.getResponseHeaders().set("Access-Control-Allow-Methods","GET,OPTIONS");ex.getResponseHeaders().set("Access-Control-Allow-Headers","Accept,Content-Type");}}
    static void send(HttpExchange ex,int code,String json)throws IOException{cors(ex);byte[] b=json.getBytes(java.nio.charset.StandardCharsets.UTF_8);Headers h=ex.getResponseHeaders();h.set("Content-Type","application/json; charset=utf-8");h.set("Cache-Control","no-store");h.set("X-Content-Type-Options","nosniff");ex.sendResponseHeaders(code,b.length);try(OutputStream out=ex.getResponseBody()){out.write(b);}}
    static String esc(String s){return s.replace("\\","\\\\").replace("\"","\\\"").replace("\n","\\n").replace("\r","\\r");}
    static String jstr(String s){return "\""+esc(s)+"\"";}
    static String json(boolean success,String source,String category,String pTs,String ret,String cacheStatus,String status,String methodology,Double delay,Double p24){
        if(!success)return "{\"success\":false,\"code\":"+jstr(source)+",\"message\":"+jstr(category)+"}";
        double p22=p24*(916.0/999.0),p18=p24*(750.0/999.0);
        return "{"+"\"success\":true,\"source\":"+jstr(source)+",\"marketCategory\":"+jstr(category)+",\"providerTimestamp\":"+jstr(pTs)+",\"retrievedAt\":"+jstr(ret)+",\"serverNow\":"+jstr(now())+",\"timezone\":\"Asia/Kolkata\",\"currency\":\"INR\",\"status\":"+jstr(status)+",\"cacheStatus\":"+jstr(cacheStatus)+",\"methodology\":"+jstr(methodology)+",\"providerDelaySeconds\":"+delay+",\"marketState\":null,\"rates\":{"+"\"24k_999\":{"+"\"perGram\":"+p24+",\"per10g\":"+(p24*10)+",\"type\":\"DIRECT\"},"+"\"22k_916\":{"+"\"perGram\":"+p22+",\"per10g\":"+(p22*10)+",\"type\":\"DERIVED\"},"+"\"18k_750\":{"+"\"perGram\":"+p18+",\"per10g\":"+(p18*10)+",\"type\":\"DERIVED\"}}}"+"}";
    }

    static Map<String,Object> parseJsonObject(String text){ // Minimal JSON parser: objects, arrays, strings, booleans, numbers, null.
        return new JsonParser(text).parseObject();
    }
    static class JsonParser{
        final String s; int i=0; JsonParser(String s){this.s=s.trim();}
        void ws(){while(i<s.length()&&Character.isWhitespace(s.charAt(i)))i++;}
        Map<String,Object> parseObject(){ws();expect('{');Map<String,Object> m=new LinkedHashMap<>();ws();if(peek()=='}'){i++;return m;}while(true){ws();String k=parseString();ws();expect(':');Object v=parseValue();m.put(k,v);ws();char c=peek();if(c=='}'){i++;break;}expect(',');}return m;}
        List<Object> parseArray(){expect('[');List<Object> a=new ArrayList<>();ws();if(peek()==']'){i++;return a;}while(true){a.add(parseValue());ws();char c=peek();if(c==']'){i++;break;}expect(',');}return a;}
        Object parseValue(){ws();char c=peek();if(c=='{')return parseObject();if(c=='[')return parseArray();if(c=='\"')return parseString();if(s.startsWith("true",i)){i+=4;return Boolean.TRUE;}if(s.startsWith("false",i)){i+=5;return Boolean.FALSE;}if(s.startsWith("null",i)){i+=4;return null;}int st=i;while(i<s.length()&&"-+0123456789.eE".indexOf(s.charAt(i))>=0)i++;String n=s.substring(st,i);return Double.valueOf(n);}
        String parseString(){expect('"');StringBuilder b=new StringBuilder();while(i<s.length()){char c=s.charAt(i++);if(c=='"')break;if(c=='\\'){char e=s.charAt(i++);switch(e){case '"':b.append('"');break;case '\\':b.append('\\');break;case '/':b.append('/');break;case 'b':b.append('\b');break;case 'f':b.append('\f');break;case 'n':b.append('\n');break;case 'r':b.append('\r');break;case 't':b.append('\t');break;case 'u':if(i+4>s.length())throw new IllegalArgumentException("JSON_ESCAPE");b.append((char)Integer.parseInt(s.substring(i,i+4),16));i+=4;break;default:throw new IllegalArgumentException("JSON_ESCAPE");}}else b.append(c);}return b.toString();}
        char peek(){ws();return i<s.length()?s.charAt(i):'\0';}void expect(char c){ws();if(i>=s.length()||s.charAt(i)!=c)throw new IllegalArgumentException("JSON_SYNTAX");i++;}
    }

    static Map<String,Object> fresh() throws Exception{
        if(API_KEY.isBlank())throw new IllegalStateException("MISSING_PROVIDER_KEY");
        String q=MARKET_SOURCE.equals("ibja")?"authority=ibja":"metal=gold";
        String endpoint="https://api.metals.dev/v1/metal/"+(MARKET_SOURCE.equals("ibja")?"authority":"spot")+"?api_key="+java.net.URLEncoder.encode(API_KEY,java.nio.charset.StandardCharsets.UTF_8)+"&"+q+"&currency=INR&unit=g";
        HttpRequest req=HttpRequest.newBuilder(URI.create(endpoint)).timeout(Duration.ofSeconds(8)).header("Accept","application/json").header("User-Agent","GoldRateIndia/1.0").GET().build();
        HttpResponse<String> resp=HTTP.send(req,HttpResponse.BodyHandlers.ofString());
        if(resp.statusCode()<200||resp.statusCode()>=300)throw new IllegalStateException("UPSTREAM_HTTP_"+resp.statusCode());
        Map<String,Object> d=parseJsonObject(resp.body());if(!"success".equals(d.get("status")))throw new IllegalStateException("UPSTREAM_REPORTED_FAILURE");
        String pTs=String.valueOf(d.get("timestamp"));Instant p=parseTs(pTs);Instant r=Instant.now();
        if(!"INR".equals(String.valueOf(d.getOrDefault("currency","INR"))))throw new IllegalStateException("UPSTREAM_CURRENCY_NOT_INR");
        double price;if(MARKET_SOURCE.equals("ibja")){Object rates=d.get("rates");if(!(rates instanceof Map))throw new IllegalStateException("UPSTREAM_PRICE_MISSING_OR_INVALID");price=positive(((Map<?,?>)rates).get("ibja_gold"));}
        else{Object rate=d.get("rate");if(!(rate instanceof Map))throw new IllegalStateException("UPSTREAM_PRICE_MISSING_OR_INVALID");price=positive(((Map<?,?>)rate).get("price"));}
        String source=MARKET_SOURCE.equals("ibja")?"Metals.Dev — IBJA authority feed":"Metals.Dev — Gold spot";
        String cat=MARKET_SOURCE.equals("ibja")?"INDIAN AUTHORITY / REFERENCE FEED":"GLOBAL GOLD SPOT PRICE";
        String ret=r.toString();double delay=Math.max(0,r.getEpochSecond()-p.getEpochSecond());
        Map<String,Object> out=new HashMap<>();out.put("success",true);out.put("source",source);out.put("marketCategory",cat);out.put("providerTimestamp",p.toString());out.put("retrievedAt",ret);out.put("serverNow",now());out.put("timezone","Asia/Kolkata");out.put("currency","INR");out.put("status",classify(p,r));out.put("cacheStatus","MISS");out.put("methodology","24K direct from upstream; 22K and 18K derived by fineness ratio only; excludes GST, duty, making charges, wastage, premiums and jeweller margin.");out.put("providerDelaySeconds",delay);out.put("marketState",null);out.put("price24",price);return out;
    }

    static void handle(HttpExchange ex)throws IOException{
        cors(ex); if("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())){ex.sendResponseHeaders(204,-1);return;}
        String ip=Optional.ofNullable(ex.getRequestHeaders().getFirst("X-Forwarded-For")).orElse(ex.getRemoteAddress().getAddress().getHostAddress()).split(",")[0].trim();if(!allowed(ip)){send(ex,429,"{\"success\":false,\"code\":\"RATE_LIMITED\",\"message\":\"Too many requests. Retry later.\"}");return;}
        String path=ex.getRequestURI().getPath();
        if(path.equals("/health")){send(ex,200,"{\"success\":true,\"status\":\"ok\",\"service\":\"gold-rate-api\",\"timezone\":\"Asia/Kolkata\",\"providerConfigured\":"+(!API_KEY.isBlank())+",\"marketSource\":"+jstr(MARKET_SOURCE)+",\"serverTime\":"+jstr(now())+"}");return;}
        if(!path.equals("/api/gold")||!"GET".equalsIgnoreCase(ex.getRequestMethod())){send(ex,404,"{\"success\":false,\"code\":\"NOT_FOUND\",\"message\":\"Route not found.\"}");return;}
        Map<String,Object> c=cache; if(c!=null){long age=Duration.between(Instant.parse(String.valueOf(c.get("retrievedAt"))),Instant.now()).getSeconds();if(age<=CACHE_TTL){String j=json(true,String.valueOf(c.get("source")),String.valueOf(c.get("marketCategory")),String.valueOf(c.get("providerTimestamp")),String.valueOf(c.get("retrievedAt")),"HIT",classify(Instant.parse(String.valueOf(c.get("providerTimestamp"))),Instant.parse(String.valueOf(c.get("retrievedAt")))),String.valueOf(c.get("methodology")),((Number)c.get("providerDelaySeconds")).doubleValue(),((Number)c.get("price24")).doubleValue());send(ex,200,j);return;}}
        try{Map<String,Object> f=fresh();cache=f;String j=json(true,String.valueOf(f.get("source")),String.valueOf(f.get("marketCategory")),String.valueOf(f.get("providerTimestamp")),String.valueOf(f.get("retrievedAt")),"MISS",String.valueOf(f.get("status")),String.valueOf(f.get("methodology")),((Number)f.get("providerDelaySeconds")).doubleValue(),((Number)f.get("price24")).doubleValue());send(ex,200,j);}catch(Exception e){System.err.println("gold request failed: "+e.getMessage());if(c!=null){String j=json(true,String.valueOf(c.get("source")),String.valueOf(c.get("marketCategory")),String.valueOf(c.get("providerTimestamp")),String.valueOf(c.get("retrievedAt")),"STALE_FALLBACK","STALE",String.valueOf(c.get("methodology")),((Number)c.get("providerDelaySeconds")).doubleValue(),((Number)c.get("price24")).doubleValue());send(ex,200,j);return;}int status="MISSING_PROVIDER_KEY".equals(e.getMessage())?503:502;String body="{\"success\":false,\"code\":"+jstr(e.getMessage()==null?"UPSTREAM_ERROR":e.getMessage())+",\"message\":"+jstr("MISSING_PROVIDER_KEY".equals(e.getMessage())?"Provider API key is not configured on the backend.":"Gold provider request failed.")+"}";send(ex,status,body);}
    }
    public static void main(String[] args)throws Exception{
        HttpServer server=HttpServer.create(new InetSocketAddress(HOST,PORT),0);server.createContext("/",Application::handle);server.setExecutor(Executors.newCachedThreadPool());Runtime.getRuntime().addShutdownHook(new Thread(()->server.stop(0)));server.start();System.out.println("[gold-rate-api] listening on http://"+HOST+":"+PORT+" source="+MARKET_SOURCE);
    }
}
