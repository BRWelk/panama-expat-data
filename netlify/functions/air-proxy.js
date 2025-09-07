// netlify/functions/air-proxy.js
// Proxies Airtable shared CSV views with strict CORS + caching.
// Env vars: RENT_CSV_URL, OVERRIDE_CSV_URL
// Optional: CORS_ALLOWLIST (comma-separated), PROXY_S_MAXAGE, PROXY_STALE

const DEFAULT_ALLOWLIST = [
  'https://www.bt-consulting-solutions.com',
  'http://localhost:8888',
  'http://localhost:3000',
  'http://localhost:5173'
];

const SQS_RE = /\.squarespace\.com$/i;

function parseAllowlist(str) {
  if (!str) return new Set(DEFAULT_ALLOWLIST);
  return new Set(str.split(',').map(s => s.trim()).filter(Boolean));
}

function isAllowedOrigin(origin, allowlist) {
  if (!origin) return true;
  try {
    const u = new URL(origin);
    if (SQS_RE.test(u.hostname)) return true;
    return [...allowlist].some(a => origin === a);
  } catch {
    return false;
  }
}

function corsHeaders(origin, allowlist) {
  const allowed = isAllowedOrigin(origin, allowlist) ? origin : '';
  return {
    'Access-Control-Allow-Origin': allowed || 'https://www.bt-consulting-solutions.com',
    'Vary': 'Origin',
    'Access-Control-Allow-Methods': 'GET,HEAD,OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, If-None-Match, If-Modified-Since',
    'Access-Control-Expose-Headers': 'ETag, Last-Modified, Cache-Control, Content-Type'
  };
}

exports.handler = async (event) => {
  const allowlist = parseAllowlist(process.env.CORS_ALLOWLIST);
  const origin = event.headers.origin || event.headers.Origin;

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: corsHeaders(origin, allowlist), body: '' };
  }

  if (!['GET','HEAD'].includes(event.httpMethod)) {
    return { statusCode: 405, headers: corsHeaders(origin, allowlist), body: JSON.stringify({ error: 'Method not allowed' }) };
  }

  const q = event.queryStringParameters || {};
  const t = (q.t || q.type || 'rents').toLowerCase();

  const urlMap = {
    rents: process.env.RENT_CSV_URL,
    overrides: process.env.OVERRIDE_CSV_URL
  };

  const targetUrl = urlMap[t];
  if (!targetUrl) {
    return { statusCode: 400, headers: corsHeaders(origin, allowlist), body: JSON.stringify({ error: 'Missing target URL', type: t }) };
  }

  const fwdHeaders = {};
  const h = event.headers || {};
  if (h['if-none-match']) fwdHeaders['if-none-match'] = h['if-none-match'];
  if (h['if-modified-since']) fwdHeaders['if-modified-since'] = h['if-modified-since'];

  let upstream;
  try {
    upstream = await fetch(targetUrl, { headers: fwdHeaders, redirect: 'follow' });
  } catch {
    return { statusCode: 502, headers: corsHeaders(origin, allowlist), body: JSON.stringify({ error: 'Upstream fetch failed' }) };
  }

  const sMaxAge = Number(process.env.PROXY_S_MAXAGE || 900);
  const stale = Number(process.env.PROXY_STALE || 300);

  const baseHeaders = {
    ...corsHeaders(origin, allowlist),
    'Content-Type': upstream.headers.get('content-type') || 'text/csv; charset=utf-8',
    'Cache-Control': `public, max-age=0, s-maxage=${sMaxAge}, stale-while-revalidate=${stale}`
  };

  const etag = upstream.headers.get('etag');
  const lastMod = upstream.headers.get('last-modified');
  if (etag) baseHeaders['ETag'] = etag;
  if (lastMod) baseHeaders['Last-Modified'] = lastMod;

  if (upstream.status === 304 || event.httpMethod === 'HEAD') {
    return { statusCode: upstream.status === 304 ? 304 : 200, headers: baseHeaders, body: '' };
  }

  const text = await upstream.text();
  return { statusCode: upstream.status, headers: baseHeaders, body: text };
};
