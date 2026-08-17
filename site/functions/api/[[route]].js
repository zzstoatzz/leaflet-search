// same-origin API proxy with a short edge cache.
//
// The frontend calls /api/* instead of the fly backend directly; this function
// proxies through and caches GET /api/search responses at the edge for
// EDGE_TTL, serving stale (up to STALE_MAX) while revalidating in the
// background. Repeated/popular queries never touch fly, and brief backend
// blips (post-deploy cold boot) are hidden behind the stale copy.
//
// Rollback: the fly URL keeps working directly (nothing backend-side changed);
// reverting the frontend to it is a site redeploy. `?edge=0` on any request
// bypasses the cache entirely for A/B and debugging.

const BACKEND = 'https://leaflet-search-backend.fly.dev';
// The replica behind /search only refreshes on ~2h snapshot adoption (overlay
// adds live rows at the origin, but cached pages tolerating 10min of staleness
// is well inside our own data cadence) — a long fresh window multiplies edge
// capacity on repeated/agent queries for free.
const EDGE_TTL_MS = 600_000; // fresh window
const STALE_MAX_S = 1800; // how long a copy stays servable-while-revalidating
const CACHED_AT = 'x-edge-cached-at';

export async function onRequest(context) {
  const { request } = context;
  const url = new URL(request.url);
  const path = url.pathname.replace(/^\/api/, '') || '/';

  // admin surfaces never go through the public edge proxy
  if (path.startsWith('/admin')) {
    return new Response(JSON.stringify({ error: 'not proxied' }), {
      status: 403,
      headers: { 'content-type': 'application/json' },
    });
  }

  const backendUrl = BACKEND + path + url.search;
  if (request.method !== 'GET') return fetch(backendUrl, request);

  const cacheable =
    path === '/search' && !url.searchParams.has('overlay') && url.searchParams.get('edge') !== '0';
  if (!cacheable) return fetch(backendUrl, request);

  // normalize the key: strip params that don't change the response body
  const keyUrl = new URL(url);
  keyUrl.searchParams.delete('edge');
  const cacheKey = new Request(keyUrl.toString(), { method: 'GET' });
  const cache = caches.default;

  const cached = await cache.match(cacheKey);
  if (cached) {
    const age = Date.now() - Number(cached.headers.get(CACHED_AT) || 0);
    if (age > EDGE_TTL_MS) {
      context.waitUntil(refresh(cache, cacheKey, backendUrl, cached.clone()).catch(() => {}));
    }
    const resp = new Response(cached.body, cached);
    resp.headers.set('x-edge-cache', age > EDGE_TTL_MS ? 'stale' : 'hit');
    return resp;
  }

  try {
    const resp = await refresh(cache, cacheKey, backendUrl);
    resp.headers.set('x-edge-cache', 'miss');
    return resp;
  } catch (e) {
    return new Response(JSON.stringify({ error: 'backend unreachable' }), {
      status: 502,
      headers: { 'content-type': 'application/json' },
    });
  }
}

// fetch from the backend and, on success, store an edge copy. Returns a
// mutable Response for the caller. When `prev` (a clone of the expired copy)
// is given, revalidate with If-None-Match: the origin's ETag encodes
// (snapshot generation, 5min bucket), so an unchanged origin answers 304 and
// the stored body is re-stamped without a search running.
async function refresh(cache, cacheKey, backendUrl, prev) {
  const headers = {};
  const prevEtag = prev && prev.headers.get('etag');
  if (prevEtag) headers['if-none-match'] = prevEtag;
  const r = await fetch(backendUrl, { headers, signal: AbortSignal.timeout(20_000) });
  if (r.status === 304 && prev) {
    const body = await prev.arrayBuffer();
    await cache.put(cacheKey, storeResponse(body, prev.headers.get('content-type'), prevEtag));
    return new Response(body, {
      status: 200,
      headers: {
        'content-type': prev.headers.get('content-type') || 'application/json',
        'access-control-allow-origin': '*',
      },
    });
  }
  if (r.status === 200) {
    const body = await r.arrayBuffer();
    await cache.put(cacheKey, storeResponse(body, r.headers.get('content-type'), r.headers.get('etag')));
    return new Response(body, {
      status: 200,
      headers: {
        'content-type': r.headers.get('content-type') || 'application/json',
        'access-control-allow-origin': '*',
      },
    });
  }
  return r;
}

function storeResponse(body, contentType, etag) {
  const headers = {
    'content-type': contentType || 'application/json',
    'cache-control': `public, max-age=${STALE_MAX_S}`,
    [CACHED_AT]: String(Date.now()),
  };
  if (etag) headers['etag'] = etag;
  return new Response(body, { status: 200, headers });
}
