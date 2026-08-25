module.exports = {
  globDirectory: '.',
  globPatterns: [
    '*.html',
    '*.css',
    '*.js',
    'icons/*.png',
    'platforms/*',
    'favicon.svg',
    'manifest.webmanifest',
    'facts.json',
  ],
  globIgnores: ['sw.js', 'workbox-*.js', 'workbox-config.cjs'],
  swDest: 'sw.js',
  sourcemap: false,
  skipWaiting: true,
  clientsClaim: true,
  cleanupOutdatedCaches: true,
  // pages reference css/js with ?v= cache-busters; match them to the precache
  ignoreURLParametersMatching: [/^v$/],
  runtimeCaching: [
    {
      // atlas datasets are big (atlas.json ~7MB) and rebuilt every 6h — serve
      // cached instantly, refresh in the background
      urlPattern: /\/atlas(-mini|-avatar-cache|-theme-cache)?\.json(\.gz)?$/,
      handler: 'StaleWhileRevalidate',
      options: {
        cacheName: 'atlas-data',
        expiration: { maxEntries: 8 },
      },
    },
  ],
}
