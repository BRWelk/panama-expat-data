// netlify/functions/air-proxy.js
// Proxy Airtable *shared view* CSVs to your frontend with CORS + CDN caching.
//
// Required env vars:
//   RENT_CSV_URL        -> e.g. https://airtable.com/.../shrxJzWbVP9aXD94y?format=csv
//   OVERRIDE_CSV_URL    -> e.g. https://airtable.com/.../shr302wUokCmyHUpa?format=csv
//
// Optional:
//   CORS_ALLOWLIST      -> comma-separated origins (in addition to Squarespace *.squarespace.com)
//   PROXY_S_MAXAGE      -> seconds for CDN cache (default 900)
//   PROXY_STALE         -> seconds for stale-while-revalidate (default 300)

const DEFAULT_ALLOWLIST = [
  'https://www.bt-consulting-solutions.com',
  'http://localhost:8888',
  'http://localhost:3000',
  'http://localhost:5173',
];

const SQS_RE = /\.squarespace\.com$/i;

function parseAllowlist(str) {
  if (!str) return new Set(DEFAULT_ALLOWLIST);
  return new Set(
    str
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
  );
}

function isAllowedOrigin(origin, allowlist) {
  if (!origin) return true; // server-to-server; we'll fall back to default origin below
  try {
    const u = new URL(origin);
    if (SQS_RE.test(u.hostname)) return true;
    return [...allowlist].some((a) => origin === a);
  } catch {
    return false;
  }
}

function corsHeaders(origin, allowlist) {
  const allowed = isAllowedOrigin(origin, allowlist) ? origin : '';
  return {
    'Access-Control-Allow-Origin':
      allowed || 'https://www.bt-consulting-solutions.com',
    Vary: 'Origin',
    'Access-Control-Allow-Methods': 'GET,HEAD,OPTIONS',
    'Access-Control-Allow-Headers':
      'Content-Type, If-None-Match, If-Modified-Since',
    'Access-Control-Expose-Headers':
      'ETag, Last-Modified, Cache-Control, Content-Type',
    'X-Robots-Tag': 'noindex',
  };
}

function pickTypeFromPath(path) {
  // Supports /api/rents and /api/overrides (via redirect :splat)
  if (!path) return '';
  const parts = String(path).split('/').filter(Boolean);
  return parts[parts.length - 1]?.toLowerCase() || '';
}

exports.handler = async (event) => {
  const allowlist = parseAllowlist(process.env.CORS_ALLOWLIST);
  const origin = event.headers.origin || event.headers.Origin;

  // Preflight
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: corsHeaders(origin, allowlist), body: '' };
  }

  if (!['GET', 'HEAD'].includes(event.httpMethod)) {
    return {
      statusCode: 405,
      headers: { ...corsHeaders(origin, allowlist), 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: 'Method not allowed' }),
    };
  }

  // Determine which CSV to fetch
  const q = event.queryStringParameters || {};
  const typeFromQuery = (q.t || q.type || '').toLowerCase();
  const typeFromPath = pickTypeFromPath(event.path || '');
  const type = (typeFromQuery || typeFromPath || 'rents').trim();

  const urlMap = {
    rents: process.env.RENT_CSV_URL,
    overrides: process.env.OVERRIDE_CSV_URL,
  };

  const targetUrl = urlMap[type];
  if (!targetUrl) {
    return {
      statusCode: 400,
      headers: { ...corsHeaders(origin, allowlist), 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: 'Missing target URL', type }),
    };
  }

  // Conditional request headers (ETag / Last-Modified)
  const fwdHeaders = {};
  const h = event.headers || {};
  if (h['if-none-match']) fwdHeaders['if-none-match'] = h['if-none-match'];
  if (h['if-modified-since']) fwdHeaders['if-modified-since'] = h['if-modified-since'];

  let upstream;
  try {
    upstream = await fetch(targetUrl, { headers: fwdHeaders, redirect: 'follow' });
  } catch (e) {
    return {
      statusCode: 502,
      headers: { ...corsHeaders(origin, allowlist), 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: 'Upstream fetch failed' }),
    };
  }

  const sMaxAge = Number(process.env.PROXY_S_MAXAGE || 900);
  const stale = Number(process.env.PROXY_STALE || 300);

  const baseHeaders = {
    ...corsHeaders(origin, allowlist),
    'Content-Type': upstream.headers.get('content-type') || 'text/csv; charset=utf-8',
    'Cache-Control': `public, max-age=0, s-maxage=${sMaxAge}, stale-while-revalidate=${stale}`,
  };

  // Propagate validators when available
  const etag = upstream.headers.get('etag');
  const lastMod = upstream.headers.get('last-modified');
  if (etag) baseHeaders['ETag'] = etag;
  if (lastMod) baseHeaders['Last-Modified'] = lastMod;

  // HEAD or not modified
  if (event.httpMethod === 'HEAD') {
    return { statusCode: 200, headers: baseHeaders, body: '' };
  }
  if (upstream.status === 304) {
    return { statusCode: 304, headers: baseHeaders, body: '' };
  }

  // Pass through body as text (CSV)
  const bodyText = await upstream.text();
  return { statusCode: upstream.status, headers: baseHeaders, body: bodyText };
};

