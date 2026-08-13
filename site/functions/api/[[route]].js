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
const EDGE_TTL_MS = 60_000; // fresh window
const STALE_MAX_S = 600; // how long a copy stays servable-while-revalidating
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
      context.waitUntil(refresh(cache, cacheKey, backendUrl).catch(() => {}));
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
// mutable Response for the caller.
async function refresh(cache, cacheKey, backendUrl) {
  const r = await fetch(backendUrl, { signal: AbortSignal.timeout(20_000) });
  if (r.status === 200) {
    const body = await r.arrayBuffer();
    const store = new Response(body, {
      status: 200,
      headers: {
        'content-type': r.headers.get('content-type') || 'application/json',
        'cache-control': `public, max-age=${STALE_MAX_S}`,
        [CACHED_AT]: String(Date.now()),
      },
    });
    await cache.put(cacheKey, store.clone());
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
