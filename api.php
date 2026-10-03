<?php
declare(strict_types=1);

header_remove('X-Powered-By');
$allowedOrigins = array_values(array_filter(array_map('trim', explode(',', getenv('CORS_ORIGINS') ?: ''))));
$origin = $_SERVER['HTTP_ORIGIN'] ?? '';
if ($origin !== '' && (in_array('*', $allowedOrigins, true) || in_array($origin, $allowedOrigins, true))) {
    header('Access-Control-Allow-Origin: ' . (in_array('*', $allowedOrigins, true) ? '*' : $origin));
    header('Vary: Origin');
    header('Access-Control-Allow-Methods: GET, OPTIONS');
    header('Access-Control-Allow-Headers: Accept, Content-Type');
}
header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: no-store');
header('X-Content-Type-Options: nosniff');

if ($_SERVER['REQUEST_METHOD'] === 'OPTIONS') { http_response_code(204); exit; }

$apiKey = getenv('METALS_DEV_API_KEY') ?: '';
$marketSource = strtolower(getenv('MARKET_SOURCE') ?: 'spot');
$cacheTtl = max(0, (int)(getenv('CACHE_TTL_SECONDS') ?: 60));
$timeout = max(1, (int)(getenv('UPSTREAM_TIMEOUT_SECONDS') ?: 8));
$liveMax = max(1, (int)(getenv('LIVE_MAX_AGE_SECONDS') ?: 180));
$recentMax = max($liveMax, (int)(getenv('RECENT_MAX_AGE_SECONDS') ?: 900));
$rateLimit = max(1, (int)(getenv('RATE_LIMIT_PER_MINUTE') ?: 60));

function respond(int $status, array $body): never {
    http_response_code($status);
    echo json_encode($body, JSON_UNESCAPED_SLASHES);
    exit;
}
function nowIso(): string { return gmdate('Y-m-d\TH:i:s\Z'); }
function finitePositive(mixed $v): ?float { return is_numeric($v) && (float)$v > 0 ? (float)$v : null; }
function validTs(mixed $v): ?string { if (!is_string($v) || trim($v)==='') return null; try { $d = new DateTimeImmutable($v); return $d->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d\TH:i:s.v\Z'); } catch (Throwable) { return null; } }
function classify(?string $providerTs, string $retrievedAt, int $liveMax, int $recentMax): string {
    if (!$providerTs) return 'RECENT';
    try { $age = max(0, (new DateTimeImmutable($retrievedAt))->getTimestamp() - (new DateTimeImmutable($providerTs))->getTimestamp()); }
    catch (Throwable) { return 'RECENT'; }
    if ($age <= $liveMax) return 'LIVE'; if ($age <= $recentMax) return 'RECENT'; return 'STALE';
}
function cachePath(): string { return rtrim(sys_get_temp_dir(), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . 'gold_rate_cache_v1.json'; }
function loadCache(): ?array { $p=cachePath(); if (!is_file($p)) return null; $raw=@file_get_contents($p); if (!$raw) return null; $data=json_decode($raw,true); return is_array($data) ? $data : null; }
function saveCache(array $data): void { @file_put_contents(cachePath(), json_encode($data, JSON_UNESCAPED_SLASHES), LOCK_EX); }
function allowed(): bool {
    global $rateLimit;
    $ip=$_SERVER['REMOTE_ADDR'] ?? 'unknown'; $path=rtrim(sys_get_temp_dir(),DIRECTORY_SEPARATOR).DIRECTORY_SEPARATOR.'gold_rate_rl_'.hash('sha256',$ip).'.json'; $now=time(); $arr=[];
    if (is_file($path)) { $j=json_decode((string)@file_get_contents($path),true); if(is_array($j))$arr=$j; }
    $arr=array_values(array_filter($arr,fn($t)=>is_int($t)&&$now-$t<60)); if(count($arr)>=$rateLimit)return false; $arr[]=$now; @file_put_contents($path,json_encode($arr),LOCK_EX); return true;
}
function upstreamGet(string $url, int $timeout): array {
    $ch=curl_init($url); curl_setopt_array($ch,[CURLOPT_RETURNTRANSFER=>true,CURLOPT_CONNECTTIMEOUT=>$timeout,CURLOPT_TIMEOUT=>$timeout,CURLOPT_HTTPHEADER=>['Accept: application/json','User-Agent: GoldRateIndia/1.0'],CURLOPT_FOLLOWLOCATION=>false]);
    $body=curl_exec($ch); $errno=curl_errno($ch); $http=(int)curl_getinfo($ch,CURLINFO_HTTP_CODE); curl_close($ch);
    if($errno!==0) throw new RuntimeException('UPSTREAM_NETWORK_ERROR'); if($http<200||$http>=300) throw new RuntimeException('UPSTREAM_HTTP_'.$http);
    $data=json_decode((string)$body,true); if(!is_array($data))throw new RuntimeException('UPSTREAM_INVALID_JSON'); return $data;
}
function fetchFresh(string $apiKey,string $source,int $timeout,int $liveMax,int $recentMax): array {
    if($apiKey==='') throw new RuntimeException('MISSING_PROVIDER_KEY');
    $base='https://api.metals.dev/v1/metal/';
    if($source==='ibja') $url=$base.'authority?'.http_build_query(['api_key'=>$apiKey,'authority'=>'ibja','currency'=>'INR','unit'=>'g']);
    else $url=$base.'spot?'.http_build_query(['api_key'=>$apiKey,'metal'=>'gold','currency'=>'INR','unit'=>'g']);
    $data=upstreamGet($url,$timeout); if(($data['status']??'')!=='success')throw new RuntimeException('UPSTREAM_REPORTED_FAILURE');
    $providerTs=validTs($data['timestamp']??null); if(!$providerTs)throw new RuntimeException('UPSTREAM_TIMESTAMP_MISSING_OR_INVALID'); if(($data['currency']??'INR')!=='INR')throw new RuntimeException('UPSTREAM_CURRENCY_NOT_INR');
    if($source==='ibja'){ $price=finitePositive($data['rates']['ibja_gold']??null); $src='Metals.Dev — IBJA authority feed'; $cat='INDIAN AUTHORITY / REFERENCE FEED'; }
    else { $price=finitePositive($data['rate']['price']??null); $src='Metals.Dev — Gold spot'; $cat='GLOBAL GOLD SPOT PRICE'; }
    if($price===null)throw new RuntimeException('UPSTREAM_PRICE_MISSING_OR_INVALID');
    $retrieved=nowIso(); $p24=$price; $p22=$p24*(916/999); $p18=$p24*(750/999);
    $providerDelay=max(0,(new DateTimeImmutable($retrieved))->getTimestamp()-(new DateTimeImmutable($providerTs))->getTimestamp());
    return ['success'=>true,'source'=>$src,'marketCategory'=>$cat,'providerTimestamp'=>$providerTs,'retrievedAt'=>$retrieved,'serverNow'=>nowIso(),'timezone'=>'Asia/Kolkata','currency'=>'INR','status'=>classify($providerTs,$retrieved,$liveMax,$recentMax),'cacheStatus'=>'MISS','methodology'=>'24K direct from upstream; 22K and 18K derived by fineness ratio only; excludes GST, duty, making charges, wastage, premiums and jeweller margin.','providerDelaySeconds'=>$providerDelay,'marketState'=>null,'rates'=>['24k_999'=>['perGram'=>$p24,'per10g'=>$p24*10,'type'=>'DIRECT'],'22k_916'=>['perGram'=>$p22,'per10g'=>$p22*10,'type'=>'DERIVED'],'18k_750'=>['perGram'=>$p18,'per10g'=>$p18*10,'type'=>'DERIVED']]];
}

if(!allowed())respond(429,['success'=>false,'code'=>'RATE_LIMITED','message'=>'Too many requests. Retry later.']);
$path=parse_url($_SERVER['REQUEST_URI']??'/',PHP_URL_PATH);
if($path==='/health'){respond(200,['success'=>true,'status'=>'ok','service'=>'gold-rate-api','timezone'=>'Asia/Kolkata','providerConfigured'=>$apiKey!=='','marketSource'=>$marketSource,'serverTime'=>nowIso()]);}
if($path!=='/api/gold'||($_SERVER['REQUEST_METHOD']??'GET')!=='GET')respond(404,['success'=>false,'code'=>'NOT_FOUND','message'=>'Route not found.']);
$cache=loadCache();
if(is_array($cache)&&isset($cache['retrievedAt'])){try{$age=time()-(new DateTimeImmutable($cache['retrievedAt']))->getTimestamp();}catch(Throwable){$age=PHP_INT_MAX;} if($age<=$cacheTtl){$cache['cacheStatus']='HIT';$cache['status']=classify($cache['providerTimestamp']??null,$cache['retrievedAt'],$liveMax,$recentMax);$cache['serverNow']=nowIso();respond(200,$cache);}}
try{$fresh=fetchFresh($apiKey,$marketSource,$timeout,$liveMax,$recentMax);saveCache($fresh);respond(200,$fresh);}catch(Throwable $e){error_log('gold request failed: '.$e->getMessage());if(is_array($cache)){$cache['cacheStatus']='STALE_FALLBACK';$cache['status']='STALE';$cache['serverNow']=nowIso();$cache['error']=$e->getMessage();respond(200,$cache);} $code=$e->getMessage()==='MISSING_PROVIDER_KEY'?'MISSING_PROVIDER_KEY':$e->getMessage(); $status=$code==='MISSING_PROVIDER_KEY'?503:502; $message=$code==='MISSING_PROVIDER_KEY'?'Provider API key is not configured on the backend.':'Gold provider request failed.';respond($status,['success'=>false,'code'=>$code,'message'=>$message]);}
