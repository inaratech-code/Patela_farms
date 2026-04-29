/* eslint-disable no-restricted-globals */
const CACHE_NAME = "patela-farms-pwa-v1";
const CORE_ASSETS = ["/", "/manifest.json", "/logo.png"];

self.addEventListener("install", (event) => {
  event.waitUntil(
    (async () => {
      const cache = await caches.open(CACHE_NAME);
      try {
        await cache.addAll(CORE_ASSETS);
      } catch {
        // Ignore cache population failures (offline-first may still function).
      }
      self.skipWaiting();
    })(),
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    (async () => {
      const keys = await caches.keys();
      await Promise.all(
        keys.map((key) => {
          if (key !== CACHE_NAME) return caches.delete(key);
          return undefined;
        }),
      );
      await self.clients.claim();
    })(),
  );
});

function shouldHandleAsAsset(url) {
  // Keep this intentionally simple: cache static assets; don't cache API/data.
  if (url.pathname.startsWith("/_next/")) return true;
  if (url.pathname.startsWith("/assets/")) return true;
  return [".png", ".jpg", ".jpeg", ".webp", ".svg", ".css", ".js"].some((ext) => url.pathname.endsWith(ext));
}

self.addEventListener("fetch", (event) => {
  const req = event.request;
  if (req.method !== "GET") return;

  let url;
  try {
    url = new URL(req.url);
  } catch {
    return;
  }

  // Only handle same-origin requests.
  if (url.origin !== self.location.origin) return;

  // For navigation requests, try network first, fallback to cache.
  if (req.mode === "navigate") {
    event.respondWith(
      (async () => {
        try {
          const fresh = await fetch(req);
          const cache = await caches.open(CACHE_NAME);
          cache.put(req, fresh.clone());
          return fresh;
        } catch {
          return (await caches.match(req)) || (await caches.match("/"));
        }
      })(),
    );
    return;
  }

  if (!shouldHandleAsAsset(url)) return;

  event.respondWith(
    (async () => {
      const cache = await caches.open(CACHE_NAME);
      const cached = await cache.match(req);
      if (cached) return cached;

      try {
        const fresh = await fetch(req);
        cache.put(req, fresh.clone());
        return fresh;
      } catch {
        return cached;
      }
    })(),
  );
});

