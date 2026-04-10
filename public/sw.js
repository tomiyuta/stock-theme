const CACHE = "st-v4";
const STATIC = ["/styles.css", "/app.js"];

self.addEventListener("install", e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(STATIC)));
  self.skipWaiting();
});

self.addEventListener("activate", e => {
  e.waitUntil(
    caches.keys().then(keys => Promise.all(
      keys.filter(k => k !== CACHE).map(k => caches.delete(k))
    )).then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", e => {
  const url = new URL(e.request.url);
  // HTML pages and API: always network-first
  if (url.pathname.endsWith('.html') || url.pathname.endsWith('/') ||
      url.pathname.startsWith('/api/') ||
      url.pathname === '/prism' || url.pathname === '/prism-r' ||
      url.pathname === '/map' || url.pathname === '/market-movers' ||
      url.pathname === '/infographic') {
    e.respondWith(
      fetch(e.request).then(res => {
        if (res.ok) {
          const clone = res.clone();
          caches.open(CACHE).then(c => c.put(e.request, clone));
        }
        return res;
      }).catch(() => caches.match(e.request))
    );
    return;
  }
  // Static assets: cache-first with network fallback
  e.respondWith(
    caches.match(e.request).then(r => r || fetch(e.request))
  );
});
