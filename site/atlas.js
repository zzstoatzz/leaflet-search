(function() {
  'use strict';

  // platform identity/colors come from the shared registry (platforms.js)
  var PLATFORM_COLORS = window.PubPlatforms.colors('dark');
  var PLATFORM_COLORS_LIGHT = window.PubPlatforms.colors('light');
  var PLATFORMS = window.PubPlatforms.order;

  // --- atlas tuning --------------------------------------------------------
  // Every label-density / avatar / cluster-pane knob lives here instead of as
  // magic numbers scattered through the render loop. One opinionated default
  // each — no presets, no settings UI; if a value is wrong, fix it here.
  // {s,l} = value for a {small, large} viewport (the W<600 split).
  var ATLAS_TUNE = {
    // cluster nebulae — lanterns. One soft light source per cluster, sitting
    // at the weighted center of its members (a lantern in the middle of a
    // group in a dark forest). A single smooth-falloff sprite per cluster
    // means peak opacity is a bounded constant — wide and translucent at
    // every zoom, never accumulating into an opaque core.
    nebula: {
      alpha: 0.22,         // peak opacity at the lantern's center
      spread: 3.0,         // lantern radius as a multiple of the cluster's RMS spread
      minHaloPx: 44,       // floor so tiny clusters still glow
      maxHaloPx: 380,      // cap so no lantern swallows the screen at high zoom
      varBase: 0.7, varRange: 0.3, // per-cluster brightness variation
      inStart: 2, inRange: 1.5,    // fade in as the coarse halos fade out
      outStart: 45, outRange: 15,  // fade out approaching card zoom
    },
    // max on-screen labels per layer. One shared collision economy, placed in
    // priority order: cluster labels (landmarks) first, then doc titles
    // (recommendation-ranked), then publication names with whatever's left.
    labels:    { titles: { s: 4, l: 12 }, coarse: { s: 5, l: 12 }, fine: { s: 6, l: 16 }, pubNames: { s: 3, l: 8 } },
    avatars:   { budget: { s: 6, l: 22 }, cull: { s: 6, l: 4 }, imgThreshold: { s: 16, l: 11 } },
    recommend: { limit: 250, boostFloor: 6 }, // popularity boost from /recommended
  };

  // --- precomputed color cache (rebuilt once per frame) ---
  var frameColors = null; // current platform colors object
  var frameDark = true;   // current theme
  var frameRgba = null;   // { platform: { core_XX: 'rgba(...)' } } — precomputed rgba strings

  function parseHex(hex) {
    return [parseInt(hex.slice(1, 3), 16), parseInt(hex.slice(3, 5), 16), parseInt(hex.slice(5, 7), 16)];
  }

  // build all rgba strings we'll need this frame
  function cacheFrameColors() {
    var dark = document.documentElement.getAttribute('data-theme') !== 'light';
    frameDark = dark;
    frameColors = dark ? PLATFORM_COLORS : PLATFORM_COLORS_LIGHT;
    frameRgba = {};
    var alphas = [0.03, 0.04, 0.05, 0.06, 0.08, 0.10, 0.12, 0.14, 0.18, 0.25, 0.40, 0.70, 1.0];
    for (var p = 0; p < PLATFORMS.length; p++) {
      var name = PLATFORMS[p];
      var c = frameColors[name];
      var entry = {};
      var parts = { core: parseHex(c.core), mid: parseHex(c.mid), edge: parseHex(c.edge) };
      for (var key in parts) {
        var rgb = parts[key];
        for (var a = 0; a < alphas.length; a++) {
          entry[key + '_' + alphas[a]] = 'rgba(' + rgb[0] + ',' + rgb[1] + ',' + rgb[2] + ',' + alphas[a] + ')';
        }
      }
      frameRgba[name] = entry;
    }
  }

  function hexToRgba(hex, a) {
    var r = parseInt(hex.slice(1, 3), 16);
    var g = parseInt(hex.slice(3, 5), 16);
    var b = parseInt(hex.slice(5, 7), 16);
    return 'rgba(' + r + ',' + g + ',' + b + ',' + a + ')';
  }

  // --- view state ---
  // maxZoom 500 (was 60): past zoom ~45 each document grows into a card
  // (platform, wrapped title, publication avatar) that keeps upsizing as
  // you approach — see the document-card layer in render(). the extra
  // headroom past the card's full size (~250) is for separating docs that
  // sit nearly on top of each other in dense clusters.
  var view = { zoom: 1, panX: 0, panY: 0, minZoom: 0.5, maxZoom: 500, dirty: true };

  // --- demand-driven frame scheduling ---
  var frameRequested = false;

  function scheduleFrame() {
    if (!frameRequested) {
      frameRequested = true;
      requestAnimationFrame(loop);
    }
  }

  function markDirty() {
    view.dirty = true;
    scheduleFrame();
  }

  // --- data ---
  var data = null;
  var pointsX = null;
  var pointsY = null;
  var platformIdx = null;
  var gridIndex = null;
  var uriToIndex = null; // Map<uri, index> for search matching
  var clusterFineArr = null; // Uint8Array of fine cluster IDs per point

  // --- popularity / label ranking ---
  // popScore[i] = how "popular" point i is (publication size + real recommend
  // counts). labelOrder = point indices sorted most-popular-first, so the few
  // labels we draw are the ones that actually matter, not whatever happened to
  // be early in the array. Recompute order when the recommend fetch lands.
  var popScore = null;   // Float32Array
  var labelOrder = null; // Int32Array of indices, popular-first

  // --- publication state ---
  var pubData = null; // array from atlas.json
  var pubByBasePath = null; // Map<basePath, pub> for ?pub=<basePath> deep-links

  // platform logos — drawn next to per-doc titles at high zoom for identity.
  // We snagged the best-available icon per platform (apple-touch-icon /
  // favicon.svg / favicon.ico → png). See site/platforms/.
  var platformLogos = {};
  var PLATFORM_LOGO_EXT = {
    leaflet: 'png',
    whitewind: 'svg',
    pckt: 'png',
    offprint: 'svg',
    greengale: 'png',
    other: 'png',
  };
  function loadPlatformLogos() {
    Object.keys(PLATFORM_LOGO_EXT).forEach(function(p) {
      if (platformLogos[p]) return;
      var img = new Image();
      img.onload = function() { markDirty(); };
      img.onerror = function() { platformLogos[p] = null; };
      img.src = '/platforms/' + p + '.' + PLATFORM_LOGO_EXT[p];
      platformLogos[p] = img;
    });
  }

  var pubImages = {}; // basePath → HTMLImageElement (loaded)
  var pubFailed = {}; // basePath → true (failed to load)
  var pubLoading = {}; // basePath → true (currently loading)
  var PUB_MAX_CONCURRENT = 6;
  var pubLoadCount = 0;

  function pubImageUrl(pub) {
    // prefer cover image, fall back to author avatar
    if (pub.did && pub.coverImage) {
      return 'https://cdn.bsky.app/img/feed_thumbnail/plain/' + pub.did + '/' + pub.coverImage + '@jpeg';
    }
    if (pub.avatar) return pub.avatar;
    return null;
  }

  function loadPubImage(pub) {
    var key = pub.basePath;
    if (pubImages[key] || pubFailed[key] || pubLoading[key]) return;
    if (pubLoadCount >= PUB_MAX_CONCURRENT) return;
    var url = pubImageUrl(pub);
    if (!url) { pubFailed[key] = true; return; }
    pubLoading[key] = true;
    pubLoadCount++;
    var img = new Image();
    img.onload = function() {
      pubImages[key] = img;
      delete pubLoading[key];
      pubLoadCount--;
      markDirty();
    };
    img.onerror = function() {
      pubFailed[key] = true;
      delete pubLoading[key];
      pubLoadCount--;
    };
    img.src = url;
  }

  // --- publication accent colors, sampled from avatar art ---
  // a publication's visual identity comes from its art; we distill it into
  // an accent hue used to style that pub's planets and cards (platform color
  // stays present in rings/rims/beacons). needs a CORS-mode image load so
  // getImageData works — tainted/grayscale/missing art falls back to null.
  var pubAccents = {}; // basePath -> {h, s, key} | null
  var pubAccentLoading = {};

  function rgbToHsl(r, g, b) {
    r /= 255; g /= 255; b /= 255;
    var mx = Math.max(r, g, b), mn = Math.min(r, g, b);
    var h = 0, s = 0, l = (mx + mn) / 2;
    if (mx !== mn) {
      var d = mx - mn;
      s = l > 0.5 ? d / (2 - mx - mn) : d / (mx + mn);
      h = mx === r ? (g - b) / d + (g < b ? 6 : 0) : mx === g ? (b - r) / d + 2 : (r - g) / d + 4;
      h /= 6;
    }
    return [h, s, l];
  }

  function hslToRgb(h, s, l) {
    if (s === 0) { var v = Math.round(l * 255); return [v, v, v]; }
    function f(p, q, t) {
      if (t < 0) t += 1;
      if (t > 1) t -= 1;
      if (t < 1 / 6) return p + (q - p) * 6 * t;
      if (t < 1 / 2) return q;
      if (t < 2 / 3) return p + (q - p) * (2 / 3 - t) * 6;
      return p;
    }
    var q = l < 0.5 ? l * (1 + s) : l + s - l * s;
    var p = 2 * l - q;
    return [Math.round(f(p, q, h + 1 / 3) * 255), Math.round(f(p, q, h) * 255), Math.round(f(p, q, h - 1 / 3) * 255)];
  }

  function accentCss(accent, l) {
    var rgb = hslToRgb(accent.h, accent.s, l);
    return 'rgb(' + rgb[0] + ',' + rgb[1] + ',' + rgb[2] + ')';
  }

  // prefer the publication's own theme colors (baked into atlas.json from
  // pub.leaflet.publication records) — that's the author's actual palette;
  // avatar sampling is the fallback for pubs without one.
  function resolvePubAccent(pub) {
    var key = pub && pub.basePath;
    if (!key || pubAccents[key] !== undefined) return;
    if (pub.themeAccent) {
      var rgb = parseHex(pub.themeAccent);
      var hsl = rgbToHsl(rgb[0], rgb[1], rgb[2]);
      var bg = pub.themeBg ? parseHex(pub.themeBg) : null;
      pubAccents[key] = {
        h: hsl[0],
        s: Math.max(0.08, Math.min(0.85, hsl[1])),
        key: 'theme' + pub.themeAccent,
        rgb: [rgb[0] / 255, rgb[1] / 255, rgb[2] / 255],
        bgRgb: bg ? [bg[0] / 255, bg[1] / 255, bg[2] / 255] : null,
      };
      return;
    }
    extractPubAccent(pub);
  }

  function extractPubAccent(pub) {
    var key = pub && pub.basePath;
    if (!key || pubAccents[key] !== undefined || pubAccentLoading[key]) return;
    var url = pubImageUrl(pub);
    if (!url) { pubAccents[key] = null; return; }
    pubAccentLoading[key] = true;
    var img = new Image();
    // bsky CDN art goes through our same-origin /img-proxy (the CDN sends no
    // CORS headers, which would taint the canvas); other hosts get a direct
    // CORS attempt and fall back to no accent
    if (url.indexOf('https://cdn.bsky.app/') === 0) {
      url = '/img-proxy?u=' + encodeURIComponent(url);
    } else {
      img.crossOrigin = 'anonymous';
    }
    img.onload = function() {
      delete pubAccentLoading[key];
      try {
        var s = 12;
        var cv = document.createElement('canvas');
        cv.width = s; cv.height = s;
        var c = cv.getContext('2d');
        c.drawImage(img, 0, 0, s, s);
        var px = c.getImageData(0, 0, s, s).data;
        var r = 0, g = 0, b = 0, wsum = 0;
        for (var i = 0; i < px.length; i += 4) {
          if (px[i + 3] < 200) continue;
          var mx = Math.max(px[i], px[i + 1], px[i + 2]);
          var mn = Math.min(px[i], px[i + 1], px[i + 2]);
          var sat = mx === 0 ? 0 : (mx - mn) / mx;
          var w = 0.05 + sat * sat; // favor the saturated pixels — that's the "theme"
          r += px[i] * w; g += px[i + 1] * w; b += px[i + 2] * w; wsum += w;
        }
        if (!wsum) { pubAccents[key] = null; return; }
        var hsl = rgbToHsl(r / wsum, g / wsum, b / wsum);
        if (hsl[1] < 0.15) { pubAccents[key] = null; return; } // grayscale art — no accent
        var sat2 = Math.min(0.85, Math.max(0.45, hsl[1]));
        pubAccents[key] = { h: hsl[0], s: sat2, key: Math.round(hsl[0] * 360) + ',' + Math.round(sat2 * 100) };
        markDirty();
      } catch (err) {
        pubAccents[key] = null; // tainted canvas — host without CORS
      }
    };
    img.onerror = function() {
      delete pubAccentLoading[key];
      pubAccents[key] = null;
    };
    img.src = url;
  }

  // --- search state ---
  var searchMatches = null; // Set of point indices matching current search
  var searchCenter = null; // {x, y} weighted centroid of matches
  var searchQuery = '';

  // --- animation state ---
  var animating = false;
  var animFrom = null;
  var animTo = null;
  var animStart = 0;
  var ANIM_DURATION = 600; // ms

  // --- canvas ---
  var canvas = document.getElementById('canvas');
  var ctx = canvas.getContext('2d');
  var dpr = window.devicePixelRatio || 1;
  var W, H;

  // WebGL planet renderer (planet-gl.js) — null falls back to 2D strips
  var planetGL = window.PlanetGL ? window.PlanetGL.create() : null;

  // --- sprite cache ---
  var sprites = null;
  var spriteSize = 0;
  var spriteTheme = null;
  var spriteStarness = -1;

  function mixHex(hex, target, t) {
    var a = parseHex(hex), b = parseHex(target);
    return 'rgb(' + Math.round(a[0] + (b[0] - a[0]) * t) + ',' +
      Math.round(a[1] + (b[1] - a[1]) * t) + ',' +
      Math.round(a[2] + (b[2] - a[2]) * t) + ')';
  }

  function buildSprites(radius, starness) {
    // quantize radius (0.5px) and starness (0.25) to avoid rebuilding
    // during smooth zoom
    radius = Math.round(radius * 2) / 2;
    starness = Math.round(starness * 4) / 4;
    var size = Math.ceil(radius * 2.8 * dpr) + 2;
    if (size < 4) size = 4;
    var theme = frameDark ? 'dark' : 'light';
    if (sprites && spriteSize === size && spriteTheme === theme && spriteStarness === starness) return;
    spriteSize = size;
    spriteTheme = theme;
    spriteStarness = starness;
    sprites = [];
    for (var p = 0; p < PLATFORMS.length; p++) {
      var c = frameColors[PLATFORMS[p]];
      sprites.push({
        normal: makeSprite(size, radius * dpr, c, 0.95, false, starness),
        hover:  makeSprite(Math.ceil(size * 1.5), radius * dpr * 1.3, c, 1.0, true, starness * 0.5),
      });
    }
  }

  // progressive disclosure of a world: at distance a document is a point of
  // starlight (hot pinpoint, tight falloff, translucent), resolving into a
  // shaded mini-sphere (same upper-left light + limb treatment as the WebGL
  // planets) as zoom rises. starness 1 = star, 0 = sphere.
  function makeSprite(size, r, colors, alpha, emphasized, starness) {
    var cv = document.createElement('canvas');
    cv.width = size; cv.height = size;
    var c = cv.getContext('2d');
    var half = size / 2;
    // sphere body — ephemeral at distance
    var sphereAlpha = alpha * (1 - starness * 0.55);
    if (sphereAlpha > 0.02) {
      c.globalAlpha = sphereAlpha;
      var grad = c.createRadialGradient(half - r * 0.35, half - r * 0.38, r * 0.1, half, half, r);
      grad.addColorStop(0, colors.core);
      grad.addColorStop(0.55, colors.mid);
      grad.addColorStop(1, colors.edge);
      c.fillStyle = grad;
      c.beginPath();
      c.arc(half, half, r, 0, Math.PI * 2);
      c.fill();
      var rim = c.createRadialGradient(half, half, r * 0.6, half, half, r);
      rim.addColorStop(0, 'rgba(0,0,0,0)');
      rim.addColorStop(0.85, 'rgba(0,0,0,0.25)');
      rim.addColorStop(1, 'rgba(0,0,0,0.55)');
      c.fillStyle = rim;
      c.beginPath();
      c.arc(half, half, r, 0, Math.PI * 2);
      c.fill();
      // atmosphere ring — the "ball outline" reads stodgy at star distance
      c.strokeStyle = hexToRgba(colors.core, (emphasized ? 0.8 : 0.35) * (1 - starness));
      c.lineWidth = Math.max(1, r * 0.06);
      c.beginPath();
      c.arc(half, half, r - c.lineWidth / 2, 0, Math.PI * 2);
      c.stroke();
    }
    // starlight core — a pinpoint, not the old wide glow-orb halo
    if (starness > 0.01) {
      var coreCol = frameDark ? mixHex(colors.core, '#ffffff', 0.42) : colors.mid;
      var sg = c.createRadialGradient(half, half, 0, half, half, r * 1.4);
      sg.addColorStop(0, coreCol);
      sg.addColorStop(0.3, hexToRgba(colors.core, 0.5));
      sg.addColorStop(1, hexToRgba(colors.core, 0));
      c.globalAlpha = alpha * starness * 0.78;
      c.fillStyle = sg;
      c.beginPath();
      c.arc(half, half, r * 1.4, 0, Math.PI * 2);
      c.fill();
    }
    return cv;
  }

  var dotSprites = null;
  var dotTheme = null;

  function buildDotSprites() {
    // the whole-map view: each document is a faint star, not a solid disc
    var theme = frameDark ? 'dark' : 'light';
    if (dotSprites && dotTheme === theme) return;
    dotTheme = theme;
    dotSprites = [];
    var s = Math.max(4, Math.ceil(2.6 * dpr));
    for (var p = 0; p < PLATFORMS.length; p++) {
      var colors = frameColors[PLATFORMS[p]];
      var cv = document.createElement('canvas');
      cv.width = s; cv.height = s;
      var c = cv.getContext('2d');
      var half = s / 2;
      var coreCol = frameDark ? mixHex(colors.core, '#ffffff', 0.38) : colors.mid;
      var sg = c.createRadialGradient(half, half, 0, half, half, half);
      sg.addColorStop(0, coreCol);
      sg.addColorStop(0.4, hexToRgba(colors.mid, 0.5));
      sg.addColorStop(1, hexToRgba(colors.mid, 0));
      c.globalAlpha = 0.55;
      c.fillStyle = sg;
      c.beginPath();
      c.arc(half, half, half, 0, Math.PI * 2);
      c.fill();
      dotSprites.push(cv);
    }
  }

  // --- nebula halo sprite cache ---
  var haloSprites = null; // { theme, entries: { 'platform_bucket': canvas } }
  var HALO_BUCKETS = [20, 50, 100, 200, 400]; // radius pixel buckets

  function getHaloBucket(radiusPx) {
    for (var i = 0; i < HALO_BUCKETS.length; i++) {
      if (radiusPx <= HALO_BUCKETS[i]) return HALO_BUCKETS[i];
    }
    return HALO_BUCKETS[HALO_BUCKETS.length - 1];
  }

  function buildHaloSprite(core, mid, edge, bucket) {
    var size = bucket * 2 + 4;
    var cv = document.createElement('canvas');
    cv.width = size; cv.height = size;
    var c = cv.getContext('2d');
    var half = size / 2;
    // gaussian-style falloff sampled densely — no piecewise tiers, the light
    // just dissipates. Hue drifts core -> mid -> edge as it fades.
    var K = 3.0, floor = Math.exp(-K);
    var grad = c.createRadialGradient(half, half, 0, half, half, bucket);
    for (var s = 0; s <= 10; s++) {
      var t = s / 10;
      var a = (Math.exp(-K * t * t) - floor) / (1 - floor);
      var from = t < 0.5 ? core : mid;
      var to = t < 0.5 ? mid : edge;
      var f = t < 0.5 ? t * 2 : (t - 0.5) * 2;
      var rr = Math.round(from[0] + (to[0] - from[0]) * f);
      var gg = Math.round(from[1] + (to[1] - from[1]) * f);
      var bb = Math.round(from[2] + (to[2] - from[2]) * f);
      grad.addColorStop(t, 'rgba(' + rr + ',' + gg + ',' + bb + ',' + a.toFixed(4) + ')');
    }
    c.fillStyle = grad;
    c.fillRect(0, 0, size, size);
    return cv;
  }

  // 'other' covers ~70% of the corpus, so platform hue makes most of the map
  // one gray fog. Those clusters instead get a deterministic hue of their own
  // (seeded by cluster id, quantized to bound the sprite cache) — different
  // lanterns, not one gray floodlight. Real platform majorities keep their
  // platform color: that's meaningful signal.
  var HUE_STEPS = 24;
  function getHaloSprite(platform, radiusPx, clusterId) {
    var theme = frameDark ? 'dark' : 'light';
    if (!haloSprites || haloSprites.theme !== theme) {
      haloSprites = { theme: theme, entries: {} };
    }
    var bucket = getHaloBucket(radiusPx);
    var key, core, mid, edge;
    if (platform === 'other' && clusterId != null) {
      var hueStep = Math.floor(hash01(clusterId) * HUE_STEPS) % HUE_STEPS;
      key = 'hue' + hueStep + '_' + bucket;
      if (!haloSprites.entries[key]) {
        var h = hueStep / HUE_STEPS;
        // muted saturation: colored, but still atmospheric
        if (frameDark) {
          core = hslToRgb(h, 0.5, 0.72);
          mid = hslToRgb(h, 0.45, 0.52);
          edge = hslToRgb(h, 0.4, 0.34);
        } else {
          core = hslToRgb(h, 0.5, 0.5);
          mid = hslToRgb(h, 0.45, 0.62);
          edge = hslToRgb(h, 0.4, 0.75);
        }
      }
    } else {
      key = platform + '_' + bucket;
      if (!haloSprites.entries[key]) {
        var nc = frameColors[platform];
        core = parseHex(nc.core); mid = parseHex(nc.mid); edge = parseHex(nc.edge);
      }
    }
    if (!haloSprites.entries[key]) {
      haloSprites.entries[key] = buildHaloSprite(core, mid, edge, bucket);
    }
    return { sprite: haloSprites.entries[key], bucket: bucket };
  }

  function resizeCanvas() {
    W = window.innerWidth;
    H = window.innerHeight;
    canvas.width = W * dpr;
    canvas.height = H * dpr;
    canvas.style.width = W + 'px';
    canvas.style.height = H + 'px';
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    sprites = null;
    dotSprites = null;
    haloSprites = null;
    markDirty();
  }

  // --- coordinate transforms (inlined for hot path) ---
  var scale = 1; // cached per frame
  var cx, cy;

  function cacheTransform() {
    scale = Math.min(W, H) * 0.42 * view.zoom;
    cx = W / 2 + view.panX * scale;
    cy = H / 2 + view.panY * scale;
  }

  function screenToData(sx, sy) {
    return [(sx - W / 2) / scale - view.panX, (sy - H / 2) / scale - view.panY];
  }

  // --- spatial index ---
  function buildSpatialIndex() {
    if (!data) return;
    var cellSize = 0.02;
    gridIndex = { cellSize: cellSize, cells: {} };
    for (var i = 0; i < data.points.length; i++) {
      var key = Math.floor(pointsX[i] / cellSize) + ',' + Math.floor(pointsY[i] / cellSize);
      if (!gridIndex.cells[key]) gridIndex.cells[key] = [];
      gridIndex.cells[key].push(i);
    }
  }

  function findNearest(sx, sy, maxDist) {
    if (!gridIndex) return -1;
    var d = screenToData(sx, sy);
    var dx = d[0], dy = d[1];
    var searchRadius = maxDist / scale;
    var cs = gridIndex.cellSize;
    var gxMin = Math.floor((dx - searchRadius) / cs);
    var gxMax = Math.floor((dx + searchRadius) / cs);
    var gyMin = Math.floor((dy - searchRadius) / cs);
    var gyMax = Math.floor((dy + searchRadius) / cs);
    var bestIdx = -1, bestDist = searchRadius * searchRadius;
    for (var gx = gxMin; gx <= gxMax; gx++) {
      for (var gy = gyMin; gy <= gyMax; gy++) {
        var cell = gridIndex.cells[gx + ',' + gy];
        if (!cell) continue;
        for (var k = 0; k < cell.length; k++) {
          var i = cell[k];
          var ddx = pointsX[i] - dx, ddy = pointsY[i] - dy;
          var dist2 = ddx * ddx + ddy * ddy;
          if (dist2 < bestDist) { bestDist = dist2; bestIdx = i; }
        }
      }
    }
    return bestIdx;
  }

  // --- document planets + hover cards (high-zoom close-up view) ---
  // past CARD_START each doc's dot grows into a rotating planet with the
  // document's info projected onto its surface (vegas-sphere style); hovering
  // (or tapping, on mobile) unfurls a flat card. CARD_FULL bounds the card's
  // size growth.
  var CARD_START = 45, CARD_RANGE = 20, CARD_FULL = 250;
  var cardHitRects = null; // [{x, y, w, h, i}] rebuilt each frame, for hover/click
  var planetsActive = false; // true while planets render → continuous frames
  var unfurlFor = -1, unfurlStart = 0; // hover-card unfurl animation

  function planetRadiusFor(z) {
    var rmax = W < 600 ? 56 : 84;
    return Math.max(4, Math.min(rmax, 7 + (z - CARD_START) * 0.3));
  }

  // --- planet surface textures ---
  // one offscreen canvas per doc: title marquee + basePath, tiled so the
  // wrap at texW is seamless, plus a bleed strip past texW because edge
  // strips sample up to ~6% of the wrap width past their u origin.
  var PLANET_TEX_W = 1024, PLANET_TEX_BLEED = 256, PLANET_TEX_H = 112;
  var planetTex = new Map(); // point index -> {canvas, theme, speed, phase}

  function getPlanetTexture(i) {
    var theme = frameDark ? 'dark' : 'light';
    var p = data.points[i];
    var pub = pubByBasePath && p.basePath ? pubByBasePath.get(p.basePath) : null;
    if (pub) resolvePubAccent(pub);
    var accent = p.basePath ? pubAccents[p.basePath] : null;
    var accentKey = accent ? accent.key : 'none';
    var e = planetTex.get(i);
    if (e && e.theme === theme && e.accentKey === accentKey) return e;
    if (planetTex.size > 96) planetTex.delete(planetTex.keys().next().value);
    var platform = PLATFORMS[platformIdx[i]];
    var c = frameColors[platform];
    var cv = document.createElement('canvas');
    cv.width = PLANET_TEX_W + PLANET_TEX_BLEED;
    cv.height = PLANET_TEX_H;
    var g = cv.getContext('2d');
    // the texture is emissive-only (info on transparent): the WebGL path
    // shades the surface itself from baseRGB/accentRGB, and the 2D fallback
    // fills the disc with baseColor before stamping the strips.
    // base surface: the publication's accent hue when we have it,
    // otherwise a tint of the platform color
    var baseRGB, accentRGB;
    if (accent && accent.rgb) {
      // literal author theme colors, exactly as they styled their publication
      baseRGB = accent.bgRgb
        ? [Math.round(accent.bgRgb[0] * 255), Math.round(accent.bgRgb[1] * 255), Math.round(accent.bgRgb[2] * 255)]
        : hslToRgb(accent.h, accent.s, frameDark ? 0.30 : 0.62);
      accentRGB = [Math.round(accent.rgb[0] * 255), Math.round(accent.rgb[1] * 255), Math.round(accent.rgb[2] * 255)];
    } else if (accent) {
      baseRGB = hslToRgb(accent.h, accent.s, frameDark ? 0.30 : 0.62);
      accentRGB = hslToRgb(accent.h, accent.s, frameDark ? 0.55 : 0.45);
    } else {
      baseRGB = parseHex(c.edge);
      accentRGB = parseHex(c.mid);
    }
    // faint latitude rings
    g.fillStyle = hexToRgba(c.mid, 0.35);
    g.fillRect(0, 10, cv.width, 2);
    g.fillRect(0, PLANET_TEX_H - 12, cv.width, 2);
    g.textBaseline = 'middle';
    g.textAlign = 'left';
    // title marquee — tile period must divide texW exactly or the wrap seam jumps
    var title = p.title || '(untitled)';
    if (title.length > 41) title = title.slice(0, 40) + '…';
    g.font = 'bold 40px monospace';
    var tw = g.measureText(title).width;
    var m = Math.max(1, Math.floor(PLANET_TEX_W / (tw + 100)));
    var period = PLANET_TEX_W / m;
    g.fillStyle = frameDark ? 'rgba(255,255,255,0.95)' : 'rgba(0,0,0,0.85)';
    for (var k = 0; k * period < cv.width; k++) {
      g.fillText(title, k * period, 46);
      // platform-colored beacon in the gap between copies
      if (period - tw > 40) {
        g.save();
        g.fillStyle = c.core;
        g.beginPath();
        g.arc(k * period + tw + (period - tw) / 2, 46, 7, 0, Math.PI * 2);
        g.fill();
        g.restore();
      }
    }
    // meta band near the south pole
    var meta = p.basePath || (p.uri.split('/')[2] || '');
    if (meta) {
      if (meta.length > 46) meta = meta.slice(0, 45) + '…';
      g.font = '21px monospace';
      var mw = g.measureText(meta).width;
      var m2 = Math.max(1, Math.floor(PLANET_TEX_W / (mw + 80)));
      var period2 = PLANET_TEX_W / m2;
      if (accent) g.fillStyle = accentCss(accent, frameDark ? 0.70 : 0.30);
      else g.fillStyle = frameDark ? hexToRgba(c.core, 0.9) : 'rgba(0,0,0,0.6)';
      for (var k2 = 0; k2 * period2 < cv.width; k2++) {
        g.fillText(meta, k2 * period2, 79);
      }
    }
    e = {
      canvas: cv,
      theme: theme,
      accentKey: accentKey,
      speed: 0.18 + (i % 7) * 0.02,
      phase: (i % 31) * 0.45,
      baseColor: 'rgb(' + baseRGB[0] + ',' + baseRGB[1] + ',' + baseRGB[2] + ')',
      baseRGB: [baseRGB[0] / 255, baseRGB[1] / 255, baseRGB[2] / 255],
      accentRGB: [accentRGB[0] / 255, accentRGB[1] / 255, accentRGB[2] / 255],
    };
    planetTex.set(i, e);
    return e;
  }

  // shading overlay (highlight upper-left, darkened limb) per radius bucket
  var planetShadeCache = {};

  function getPlanetShade(R) {
    var bucket = Math.max(8, Math.round(R / 8) * 8);
    if (planetShadeCache[bucket]) return planetShadeCache[bucket];
    var size = bucket * 2;
    var cv = document.createElement('canvas');
    cv.width = size; cv.height = size;
    var g = cv.getContext('2d');
    var hl = g.createRadialGradient(size * 0.35, size * 0.32, bucket * 0.1, size * 0.35, size * 0.32, bucket * 1.1);
    hl.addColorStop(0, 'rgba(255,255,255,0.30)');
    hl.addColorStop(0.4, 'rgba(255,255,255,0.06)');
    hl.addColorStop(1, 'rgba(255,255,255,0)');
    g.fillStyle = hl;
    g.fillRect(0, 0, size, size);
    var rim = g.createRadialGradient(bucket, bucket, bucket * 0.6, bucket, bucket, bucket);
    rim.addColorStop(0, 'rgba(0,0,0,0)');
    rim.addColorStop(0.85, 'rgba(0,0,0,0.2)');
    rim.addColorStop(1, 'rgba(0,0,0,0.5)');
    g.fillStyle = rim;
    g.fillRect(0, 0, size, size);
    planetShadeCache[bucket] = cv;
    return cv;
  }

  // sphere projection: vertical strips, longitude per column via asin,
  // column height from the circle chord — a wrapped cylinder squashed into
  // the silhouette, which reads as a rotating globe.
  function drawPlanet(i, sx, sy, R, alpha, tSec) {
    var tex = getPlanetTexture(i);
    var rot = tSec * tex.speed + tex.phase;
    var TWO_PI = Math.PI * 2;
    rot = ((rot % TWO_PI) + TWO_PI) % TWO_PI;
    var texW = PLANET_TEX_W, texH = PLANET_TEX_H;
    ctx.save();
    ctx.globalAlpha = alpha;
    ctx.beginPath();
    ctx.arc(sx, sy, R, 0, TWO_PI);
    ctx.clip();
    // the texture is emissive-only (transparent bg) — lay down the surface
    ctx.fillStyle = tex.baseColor;
    ctx.fillRect(sx - R, sy - R, R * 2, R * 2);
    // TILT: we view each planet from slightly north of its equator, so the
    // text rows dip at the center of the face and curl up toward the limb —
    // approximated by stretching each strip downward in proportion to its
    // chord height (max at center, zero at the edges).
    var TILT = 0.22;
    var step = R > 50 ? 3 : 2;
    var prevLam = -Math.PI / 2;
    for (var x = -R; x < R; x += step) {
      var x2 = Math.min(R, x + step);
      var s2 = Math.max(-1, Math.min(1, x2 / R));
      var lam2 = Math.asin(s2);
      var u0 = ((rot + prevLam) % TWO_PI + TWO_PI) % TWO_PI / TWO_PI * texW;
      var du = Math.max(0.5, (lam2 - prevLam) / TWO_PI * texW);
      var midx = (x + x2) / 2;
      var h = Math.sqrt(Math.max(1, R * R - midx * midx));
      var dip = TILT * h;
      // strips overlap by ~0.7px (with matching source widening) to kill
      // the vertical seam striping that fractional-width strips produce
      var destW = (x2 - x) + 0.7;
      var srcW = du * destW / (x2 - x);
      ctx.drawImage(tex.canvas, u0, 0, srcW, texH, sx + x, sy - h, destW, h * 2 + dip);
      prevLam = lam2;
    }
    var shade = getPlanetShade(R);
    ctx.drawImage(shade, sx - R, sy - R, R * 2, R * 2);
    ctx.restore();
    // rim
    var platform = PLATFORMS[platformIdx[i]];
    ctx.globalAlpha = alpha * (i === hoveredIndex || i === selectedIndex ? 0.95 : 0.5);
    ctx.beginPath();
    ctx.arc(sx, sy, R, 0, TWO_PI);
    ctx.strokeStyle = frameColors[platform].core;
    ctx.lineWidth = i === hoveredIndex || i === selectedIndex ? 2 : 1.25;
    ctx.stroke();
    ctx.globalAlpha = 1;
  }

  function findCardAt(sx, sy) {
    if (!cardHitRects) return -1;
    for (var k = cardHitRects.length - 1; k >= 0; k--) {
      var r = cardHitRects[k];
      if (sx >= r.x && sx <= r.x + r.w && sy >= r.y && sy <= r.y + r.h) return r.i;
    }
    return -1;
  }

  function roundRectPath(x, y, w, h, r) {
    ctx.beginPath();
    if (ctx.roundRect) { ctx.roundRect(x, y, w, h, r); return; }
    ctx.moveTo(x + r, y);
    ctx.arcTo(x + w, y, x + w, y + h, r);
    ctx.arcTo(x + w, y + h, x, y + h, r);
    ctx.arcTo(x, y + h, x, y, r);
    ctx.arcTo(x, y, x + w, y, r);
    ctx.closePath();
  }

  // measurement-based word-wrap: returns up to maxLines lines, ellipsis on
  // overflow. char-count heuristics break on CJK — those glyphs render ~2x
  // the width of a latin monospace cell — so every line is fit by actual
  // measured pixel width. hard-breaks unbroken runs (URLs, CJK) too.
  function wrapText(text, maxW, maxLines) {
    var lines = [], cur = '', truncated = false;
    var words = text.split(/\s+/);
    for (var w = 0; w < words.length && !truncated; w++) {
      var word = words[w];
      if (!word) continue;
      var attempt = cur ? cur + ' ' + word : word;
      if (ctx.measureText(attempt).width <= maxW) { cur = attempt; continue; }
      if (cur) {
        lines.push(cur);
        cur = '';
        if (lines.length >= maxLines) { truncated = true; break; }
      }
      // a single word wider than the line — break it by measured fit
      while (ctx.measureText(word).width > maxW) {
        var k = word.length - 1;
        while (k > 1 && ctx.measureText(word.slice(0, k)).width > maxW) k--;
        lines.push(word.slice(0, k));
        word = word.slice(k);
        if (lines.length >= maxLines) { truncated = true; break; }
      }
      if (!truncated) cur = word;
    }
    if (!truncated && cur) {
      if (lines.length < maxLines) lines.push(cur);
      else truncated = true;
    }
    if (truncated && lines.length > 0) {
      var last = lines[lines.length - 1] + '…';
      while (last.length > 2 && ctx.measureText(last).width > maxW) {
        last = last.slice(0, -2) + '…';
      }
      lines[lines.length - 1] = last;
    }
    return lines;
  }

  function truncToChars(text, maxW) {
    if (ctx.measureText(text).width <= maxW) return text;
    var t = text;
    while (t.length > 1 && ctx.measureText(t + '…').width > maxW) t = t.slice(0, -1);
    return t + '…';
  }

  // --- label helper: strokeText outline instead of shadowBlur ---
  function drawLabel(text, x, y, dark) {
    ctx.strokeStyle = dark ? 'rgba(0,0,0,0.88)' : 'rgba(255,255,255,0.88)';
    ctx.lineWidth = 4;
    ctx.lineJoin = 'round';
    ctx.strokeText(text, x, y);
    ctx.fillText(text, x, y);
  }

  // --- smooth transition helpers ---
  function clamp01(x) { return x < 0 ? 0 : x > 1 ? 1 : x; }
  function fadeIn(zoom, start, range) { return clamp01((zoom - start) / range); }
  function fadeOut(zoom, start, range) { return 1 - clamp01((zoom - start) / range); }

  // Deterministic [0,1) hash of an integer id — varies each cluster nebula's
  // opacity a touch so neighbouring clouds read as distinct rather than one
  // flat wash. Transparency variation only, no hue shift.
  function hash01(id) {
    var x = (id * 2654435761) >>> 0; // Knuth multiplicative
    x ^= x >>> 15; x = (x * 2246822519) >>> 0; x ^= x >>> 13;
    return (x >>> 0) / 4294967296;
  }

  // --- connection line buffers (pre-allocated, reused each frame) ---
  var connBufSize = 6000;
  var connBufs = null; // [platform][bucket] = Float32Array
  var connBufLens = null; // [platform][bucket] = current length

  function initConnBuffers() {
    connBufs = [];
    connBufLens = [];
    for (var p = 0; p < PLATFORMS.length; p++) {
      connBufs.push([
        new Float32Array(connBufSize),
        new Float32Array(connBufSize),
        new Float32Array(connBufSize)
      ]);
      connBufLens.push([0, 0, 0]);
    }
  }

  function resetConnBuffers() {
    for (var p = 0; p < PLATFORMS.length; p++) {
      connBufLens[p][0] = 0;
      connBufLens[p][1] = 0;
      connBufLens[p][2] = 0;
    }
  }

  // --- rendering ---
  function render() {
    if (!data || !view.dirty) return;
    view.dirty = false;

    // cache theme + colors once per frame
    cacheFrameColors();
    var dark = frameDark;
    var zoom = view.zoom;
    var n = data.points.length;

    cacheTransform();

    // background
    ctx.globalAlpha = 1;
    ctx.fillStyle = dark ? '#050505' : '#f5f5f0';
    ctx.fillRect(0, 0, W, H);

    // visible bounds in data space
    var tl = screenToData(0, 0);
    var br = screenToData(W, H);
    var pad = 0.05;
    var xMin = tl[0] - pad, xMax = br[0] + pad;
    var yMin = tl[1] - pad, yMax = br[1] + pad;

    var smallViewport = W < 600;
    var haloShrink = smallViewport ? 0.6 : 1.0;

    // --- coarse cluster nebulae (the zoomed-OUT view) ---
    // Soft platform-tinted clouds at region centroids. Full when zoomed all the
    // way out, then hand off to the FINE nebulae below as you zoom in — same
    // visual language, finer granularity, so the experience is continuous.
    // gone by ~2.8 — once fine cluster labels are readable, the giant
    // region-scale flares read as extreme rather than atmospheric
    var coarseHaloAlpha = fadeOut(zoom, 1.8, 1.0);
    if (coarseHaloAlpha > 0.01) {
      var coarse = data.clusters.coarse;
      var baseAlpha = (dark ? 0.06 : 0.05) * coarseHaloAlpha;
      for (var c = 0; c < coarse.length; c++) {
        var cl = coarse[c];
        var r = (cl.radius || 0.05) * scale;
        if (r < 2) continue;
        var sx = cx + cl.cx * scale, sy = cy + cl.cy * scale;
        if (sx + r < 0 || sx - r > W || sy + r < 0 || sy - r > H) continue;
        var halo = getHaloSprite(cl.dominantPlatform || 'other', r, cl.id);
        var drawSize = halo.sprite.width * (r / halo.bucket) * haloShrink;
        ctx.globalAlpha = baseAlpha * 2;
        ctx.drawImage(halo.sprite, sx - drawSize / 2, sy - drawSize / 2, drawSize, drawSize);
      }
      ctx.globalAlpha = 1;
    }

    // --- fine cluster nebulae: lanterns (the zoomed-IN view) ---
    // One soft light per cluster at the weighted center of its members. A
    // single smooth-falloff sprite means the center never exceeds neb.alpha —
    // wide, translucent, and smooth at every zoom. Fades in as the coarse
    // nebulae fade out; the fine cluster labels (below) name each glow.
    var neb = ATLAS_TUNE.nebula;
    var nebAlpha = fadeIn(zoom, neb.inStart, neb.inRange) * fadeOut(zoom, neb.outStart, neb.outRange);
    if (nebAlpha > 0.01 && data.clusters.fine) {
      var fine = data.clusters.fine;
      for (var c = 0; c < fine.length; c++) {
        var cl = fine[c];
        var L = cl.lantern;
        if (!L) continue;
        // the floor grows with sqrt(zoom): walking toward a lantern makes its
        // aura wider, but never balloons the way linear scaling did
        var rFloor = neb.minHaloPx * Math.sqrt(Math.max(1, zoom / neb.inStart));
        var rPx = Math.min(neb.maxHaloPx,
          Math.max(rFloor, L.r * neb.spread * scale)) * (smallViewport ? 0.85 : 1);
        var nsx = cx + L.x * scale, nsy = cy + L.y * scale;
        if (nsx + rPx < 0 || nsx - rPx > W || nsy + rPx < 0 || nsy - rPx > H) continue;
        var halo2 = getHaloSprite(cl.dominantPlatform || 'other', rPx, cl.id);
        var drawSize2 = halo2.sprite.width * (rPx / halo2.bucket);
        var v = neb.varBase + hash01(cl.id) * neb.varRange; // per-lantern brightness variation
        ctx.globalAlpha = neb.alpha * v * nebAlpha;
        ctx.drawImage(halo2.sprite, nsx - drawSize2 / 2, nsy - drawSize2 / 2, drawSize2, drawSize2);
      }
      ctx.globalAlpha = 1;
    }

    // --- connection lines (intra-cluster, colored by platform) ---
    // smooth fade-in over zoom 2.5–3.5
    var connAlphaFactor = fadeIn(zoom, 2.5, 1.0);
    if (connAlphaFactor > 0 && gridIndex && clusterFineArr) {
      if (!connBufs) initConnBuffers();
      resetConnBuffers();

      var connRadius = 0.025;
      var cs = gridIndex.cellSize;
      var maxLines = 1500;
      var lineCount = 0;

      for (var i = 0; i < n && lineCount < maxLines; i++) {
        var px = pointsX[i], py = pointsY[i];
        if (px < xMin || px > xMax || py < yMin || py > yMax) continue;

        var sx1 = cx + px * scale, sy1 = cy + py * scale;
        var ci = clusterFineArr[i];
        var gxMin2 = Math.floor((px - connRadius) / cs);
        var gxMax2 = Math.floor((px + connRadius) / cs);
        var gyMin2 = Math.floor((py - connRadius) / cs);
        var gyMax2 = Math.floor((py + connRadius) / cs);

        for (var gx = gxMin2; gx <= gxMax2 && lineCount < maxLines; gx++) {
          for (var gy = gyMin2; gy <= gyMax2 && lineCount < maxLines; gy++) {
            var cell = gridIndex.cells[gx + ',' + gy];
            if (!cell) continue;
            for (var k = 0; k < cell.length && lineCount < maxLines; k++) {
              var j = cell[k];
              if (j <= i) continue;
              if (clusterFineArr[j] !== ci) continue;
              var dx = pointsX[j] - px, dy = pointsY[j] - py;
              var dist2 = dx * dx + dy * dy;
              if (dist2 > connRadius * connRadius || dist2 < 0.0001) continue;
              var t = Math.sqrt(dist2) / connRadius;
              var bucket = t < 0.33 ? 0 : t < 0.66 ? 1 : 2;
              var pi = platformIdx[i];
              var buf = connBufs[pi][bucket];
              var len = connBufLens[pi][bucket];
              if (len + 4 <= buf.length) {
                buf[len] = sx1;
                buf[len + 1] = sy1;
                buf[len + 2] = cx + pointsX[j] * scale;
                buf[len + 3] = cy + pointsY[j] * scale;
                connBufLens[pi][bucket] = len + 4;
              }
              lineCount++;
            }
          }
        }
      }

      // draw each platform × distance bucket
      var connAlphas = dark ? [0.18, 0.10, 0.05] : [0.14, 0.08, 0.03];
      ctx.lineWidth = 0.5;
      for (var p = 0; p < PLATFORMS.length; p++) {
        var cc = frameColors[PLATFORMS[p]];
        for (var b = 0; b < 3; b++) {
          var len = connBufLens[p][b];
          if (!len) continue;
          var buf = connBufs[p][b];
          ctx.beginPath();
          for (var l = 0; l < len; l += 4) {
            ctx.moveTo(buf[l], buf[l + 1]);
            ctx.lineTo(buf[l + 2], buf[l + 3]);
          }
          ctx.strokeStyle = hexToRgba(cc.mid, connAlphas[b] * connAlphaFactor);
          ctx.globalAlpha = 1;
          ctx.stroke();
        }
      }
    }

    // --- publication circles ---
    // radius = sqrt(count) * zoom * 0.35, capped at 28px
    // at zoom 1: sqrt(236)≈15 → 5.4px (visible), sqrt(30)≈5.5 → 1.9px (culled)
    // smaller than before — publications accent the map, not dominate it
    var pubLabelCands = []; // deferred to the label economy below
    if (pubData && pubData.length > 0) {
      var pubLabelZoom = 3;
      var pubRendered = 0;
      // Avatar imagery is the loudest thing on the map — spend it sparingly.
      // pubData is sorted biggest-first, so this budget lands on the most
      // prominent publications; everyone else reads as a quiet ring/letter.
      var avKey = smallViewport ? 's' : 'l';
      var avatarBudget = ATLAS_TUNE.avatars.budget[avKey];
      var pubCull = ATLAS_TUNE.avatars.cull[avKey];
      var imgThreshold = ATLAS_TUNE.avatars.imgThreshold[avKey];
      for (var pi2 = 0; pi2 < pubData.length; pi2++) {
        var pub = pubData[pi2];
        var pr = Math.min(28, Math.sqrt(pub.count) * zoom * 0.35);
        if (pr < pubCull) continue; // natural culling — small pubs disappear
        var psx = cx + pub.cx * scale, psy = cy + pub.cy * scale;
        // cull off-screen (with padding for labels)
        if (psx < -60 || psx > W + 60 || psy < -60 || psy > H + 60) continue;
        pubRendered++;
        var pPlatform = pub.platform || 'other';
        var pColors = frameColors[pPlatform] || frameColors.other;

        // only the budgeted, large-enough publications get avatar imagery
        var wantAvatar = pr >= imgThreshold && avatarBudget > 0;
        if (wantAvatar) { loadPubImage(pub); avatarBudget--; }

        var img = wantAvatar ? pubImages[pub.basePath] : null;
        if (img) {
          // clipped circle with cover image
          ctx.save();
          ctx.globalAlpha = 0.9;
          ctx.beginPath();
          ctx.arc(psx, psy, pr, 0, Math.PI * 2);
          ctx.clip();
          ctx.drawImage(img, psx - pr, psy - pr, pr * 2, pr * 2);
          ctx.restore();
        } else {
          // fallback: filled circle with first letter
          ctx.globalAlpha = 0.7;
          ctx.beginPath();
          ctx.arc(psx, psy, pr, 0, Math.PI * 2);
          ctx.fillStyle = pColors.edge;
          ctx.fill();
          // letter only once the circle is a real landmark — smaller pubs
          // stay quiet rings so dense regions read as dots, not glyphs
          if (pr >= 14) {
            var letter = (pub.name || '?').charAt(0).toUpperCase();
            var letterSize = Math.max(8, pr * 0.9);
            ctx.font = 'bold ' + Math.round(letterSize) + 'px monospace';
            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';
            ctx.fillStyle = pColors.core;
            ctx.globalAlpha = 0.9;
            ctx.fillText(letter, psx, psy);
          }
        }

        // border ring — quiet on small circles, assertive only as they grow
        ctx.globalAlpha = 0.2 + Math.min(0.4, pr / 35);
        ctx.beginPath();
        ctx.arc(psx, psy, pr, 0, Math.PI * 2);
        ctx.strokeStyle = pColors.mid;
        ctx.lineWidth = 1.5;
        ctx.stroke();

        // name labels no longer draw here \u2014 they queue for the shared label
        // economy below, where cluster labels and doc titles place first
        if (zoom >= pubLabelZoom && pr >= 10 && pubLabelCands.length < 40) {
          pubLabelCands.push({ name: pub.name, x: psx, y: psy + pr + 9 });
        }
      }
      ctx.globalAlpha = 1;
    }

    // --- points: sprite-stamped ---
    ctx.globalAlpha = 1;
    var useGlow = zoom >= 2;
    if (useGlow) {
      // gentle size curve: starlight points stay small through the
      // midrange, reaching 7px right where the planets take over (the
      // planet radius starts at 7 at CARD_START, so the handoff is smooth)
      var pointR = Math.min(7, 1.0 + zoom * 0.135);
      // starness: pure starlight below zoom ~8, fully a sphere by ~22
      var starness = fadeOut(zoom, 8, 14);
      buildSprites(pointR, starness);
    } else {
      buildDotSprites();
    }

    var filtering = activePlatforms !== null;
    // draw dimmed points first, then active points on top
    for (var pass = 0; pass < (filtering ? 2 : 1); pass++) {
      if (filtering && pass === 0) ctx.globalAlpha = 0.12;
      else ctx.globalAlpha = 1;
      for (var i = 0; i < n; i++) {
        var px = pointsX[i], py = pointsY[i];
        if (px < xMin || px > xMax || py < yMin || py > yMax) continue;
        var pi = platformIdx[i];
        var isActive = !filtering || activePlatforms.has(PLATFORMS[pi]);
        // pass 0 = dimmed (inactive), pass 1 = bright (active)
        if (filtering && ((pass === 0 && isActive) || (pass === 1 && !isActive))) continue;
        if (!filtering && pass === 1) continue;
        var sx = cx + px * scale, sy = cy + py * scale;
        if (i === hoveredIndex && useGlow) {
          var spr = sprites[pi].hover;
          ctx.drawImage(spr, sx - spr.width / (2 * dpr), sy - spr.height / (2 * dpr), spr.width / dpr, spr.height / dpr);
        } else if (useGlow) {
          var spr = sprites[pi].normal;
          ctx.drawImage(spr, sx - spr.width / (2 * dpr), sy - spr.height / (2 * dpr), spr.width / dpr, spr.height / dpr);
        } else {
          var dot = dotSprites[pi];
          ctx.drawImage(dot, sx - dot.width / (2 * dpr), sy - dot.height / (2 * dpr), dot.width / dpr, dot.height / dpr);
        }
      }
    }
    ctx.globalAlpha = 1;

    // --- search highlights ---
    if (searchMatches && searchMatches.size > 0) {
      // dim non-matching points by drawing a semi-transparent overlay
      ctx.globalAlpha = dark ? 0.6 : 0.5;
      ctx.fillStyle = dark ? '#050505' : '#f5f5f0';
      ctx.fillRect(0, 0, W, H);

      // redraw matched points brighter
      ctx.globalAlpha = 1;
      searchMatches.forEach(function(i) {
        var px = pointsX[i], py = pointsY[i];
        if (px < xMin || px > xMax || py < yMin || py > yMax) return;
        var sx = cx + px * scale, sy = cy + py * scale;
        var pi = platformIdx[i];
        if (useGlow) {
          var spr = sprites[pi].hover;
          ctx.drawImage(spr, sx - spr.width / (2 * dpr), sy - spr.height / (2 * dpr), spr.width / dpr, spr.height / dpr);
        } else {
          var dot = dotSprites[pi];
          ctx.drawImage(dot, sx - dot.width / (2 * dpr), sy - dot.height / (2 * dpr), dot.width / dpr, dot.height / dpr);
        }
      });

      // draw search centroid marker — not at planet zoom, where the matches
      // themselves are unmistakable
      if (searchCenter && fadeIn(zoom, CARD_START, CARD_RANGE) < 0.5) {
        var mx = cx + searchCenter.x * scale, my = cy + searchCenter.y * scale;
        var accent = dark ? 'rgba(250,200,80,' : 'rgba(200,120,0,';
        // outer ring — pulsing glow
        ctx.beginPath();
        ctx.arc(mx, my, 18, 0, Math.PI * 2);
        ctx.strokeStyle = accent + '0.25)';
        ctx.lineWidth = 3;
        ctx.stroke();
        // crosshair lines
        ctx.strokeStyle = accent + '0.8)';
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        ctx.moveTo(mx - 20, my); ctx.lineTo(mx - 8, my);
        ctx.moveTo(mx + 8, my); ctx.lineTo(mx + 20, my);
        ctx.moveTo(mx, my - 20); ctx.lineTo(mx, my - 8);
        ctx.moveTo(mx, my + 8); ctx.lineTo(mx, my + 20);
        ctx.stroke();
        // center dot
        ctx.beginPath();
        ctx.arc(mx, my, 3, 0, Math.PI * 2);
        ctx.fillStyle = accent + '0.9)';
        ctx.fill();
        // label with outline
        ctx.font = '12px monospace';
        ctx.textAlign = 'left';
        ctx.textBaseline = 'middle';
        ctx.fillStyle = accent + '0.9)';
        drawLabel('"' + searchQuery + '"', mx + 24, my, dark);
      }
    }

    // --- labels with collision avoidance ---
    ctx.globalAlpha = 1;
    ctx.fillStyle = dark ? 'rgba(255,255,255,0.75)' : 'rgba(0,0,0,0.65)';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';

    var small = W < 600;
    // placed label bounding boxes for collision detection
    var placed = []; // [{x, y, hw, hh}] — center + half-width/half-height
    var PAD = small ? 2 : 4; // padding between labels

    function canPlace(lx, ly, tw, th) {
      var hw = tw / 2 + PAD, hh = th / 2 + PAD;
      for (var k = 0; k < placed.length; k++) {
        var p = placed[k];
        if (Math.abs(lx - p.x) < hw + p.hw && Math.abs(ly - p.y) < hh + p.hh) return false;
      }
      placed.push({ x: lx, y: ly, hw: hw, hh: hh });
      return true;
    }

    // label margin: keep labels inside viewport with some padding
    var LABEL_MARGIN = small ? 8 : 12;

    // smooth label transitions:
    // coarse labels: full opacity zoom<1.7, fade out 1.7–2.3
    // fine labels: fade in 1.7–2.3, then PERSIST through the zoomed-in range so
    //   every fine nebula stays named (continuous with the zoomed-out regions);
    //   they ride the same fade-out as the nebulae, near card zoom.
    // titles: fade in 4.5–5.5, hold until planet surface text is readable
    // planets: fade in over CARD_START..CARD_START+CARD_RANGE, keep growing
    // cards: hover/selection only — unfurl next to the planet
    var coarseAlpha = fadeOut(zoom, 1.7, 0.6);
    var fineAlpha = fadeIn(zoom, 1.7, 0.6) * fadeOut(zoom, 45, 15);
    var titleAlpha = fadeIn(zoom, 4.5, 1.0) * fadeOut(zoom, 110, 40);
    var planetAlpha = fadeIn(zoom, CARD_START, CARD_RANGE);
    var cardAlpha = planetAlpha;

    // --- document planets: info projected onto rotating orbs ---
    planetsActive = false;
    var planetR = 0;
    if (planetAlpha > 0.01) {
      planetR = planetRadiusFor(zoom);
      var tSec = performance.now() / 1000;
      // nearest-to-viewport-center docs win the planet slots
      var maxPlanets = small ? 20 : 36;
      var cands = [];
      var vcx = W / 2, vcy = H / 2;
      for (var i = 0; i < n; i++) {
        var px = pointsX[i], py = pointsY[i];
        if (px < xMin || px > xMax || py < yMin || py > yMax) continue;
        if (filtering && !activePlatforms.has(PLATFORMS[platformIdx[i]])) continue;
        // while searching, only matches become planets — the rest stay dimmed dots
        if (searchMatches && searchMatches.size > 0 && !searchMatches.has(i)) continue;
        var sx = cx + px * scale, sy = cy + py * scale;
        if (sx + planetR < 0 || sx - planetR > W || sy + planetR < 0 || sy - planetR > H) continue;
        var ddx = sx - vcx, ddy = sy - vcy;
        cands.push({ i: i, sx: sx, sy: sy, d: ddx * ddx + ddy * ddy });
      }
      cands.sort(function(a, b) { return a.d - b.d; });
      if (cands.length > maxPlanets) cands.length = maxPlanets;
      // adaptive radius: shrink to local spacing so dense clusters read as
      // distinct globes instead of overlapping mud
      var rMin = Math.min(9, planetR);
      for (var c = 0; c < cands.length; c++) {
        var best = Infinity;
        for (var c2 = 0; c2 < cands.length; c2++) {
          if (c2 === c) continue;
          var dx2 = cands[c].sx - cands[c2].sx, dy2 = cands[c].sy - cands[c2].sy;
          var dd = dx2 * dx2 + dy2 * dy2;
          if (dd < best) best = dd;
        }
        cands[c].r = best === Infinity ? planetR
          : Math.max(rMin, Math.min(planetR, Math.sqrt(best) * 0.52));
      }
      if (planetGL) {
        planetGL.begin(W, H, dpr, dark);
        var texSpan = PLANET_TEX_W / (PLANET_TEX_W + PLANET_TEX_BLEED);
        for (var c = 0; c < cands.length; c++) {
          var pcI = cands[c].i;
          var pcT = getPlanetTexture(pcI);
          var pcRot = (tSec * pcT.speed + pcT.phase) % (Math.PI * 2);
          planetGL.draw(pcT.canvas, cands[c].sx, cands[c].sy, cands[c].r, planetAlpha, pcRot, {
            base: pcT.baseRGB,
            accent: pcT.accentRGB,
            seed: (pcI % 97) * 1.3,
            texSpan: texSpan,
            hover: pcI === hoveredIndex || pcI === selectedIndex,
            dpr: dpr,
          });
        }
        if (cands.length > 0) ctx.drawImage(planetGL.canvas, 0, 0, W, H);
      } else {
        for (var c = 0; c < cands.length; c++) {
          drawPlanet(cands[c].i, cands[c].sx, cands[c].sy, cands[c].r, planetAlpha, tSec);
        }
      }
      planetsActive = cands.length > 0;
    }

    // --- hover/selection card: unfurled flat view of one document ---
    cardHitRects = null;
    var focusIdx = hoveredIndex >= 0 ? hoveredIndex : selectedIndex;
    if (cardAlpha > 0.01 && focusIdx >= 0 && focusIdx < n) {
      cardHitRects = [];
      if (focusIdx !== unfurlFor) { unfurlFor = focusIdx; unfurlStart = performance.now(); }
      var unfurl = planetsActive ? easeOutCubic(clamp01((performance.now() - unfurlStart) / 180)) : 1;
      // growth factor: cards upsize from the moment they're fully formed
      // (CARD_START+CARD_RANGE) until CARD_FULL
      var g = clamp01((zoom - (CARD_START + CARD_RANGE)) / (CARD_FULL - CARD_START - CARD_RANGE));
      function grow(a, b) { return a + (b - a) * g; }
      var cardW = small ? Math.min(W - 32, grow(160, 300)) : grow(190, 360);
      var padC = Math.round(grow(9, 16));
      var titleFont = Math.round(grow(11, 19));
      var metaFont = Math.round(grow(9, 13));
      var headFont = Math.round(grow(9, 12));
      var logoS = Math.round(grow(13, 20));
      var avatarS = Math.round(grow(20, 48));
      var lineH = Math.round(titleFont * 1.35);
      var maxTitleLines = g < 0.4 ? 2 : 3;
      var anchorGap = Math.round(planetR + 10);
      var innerW = cardW - padC * 2;

      ctx.textAlign = 'left';
      ctx.textBaseline = 'middle';
      (function() {
        var idx = focusIdx;
        var p = data.points[idx];
        var sx = cx + pointsX[idx] * scale, sy = cy + pointsY[idx] * scale;
        var platform = PLATFORMS[platformIdx[idx]];
        var colors = frameColors[platform];
        var title = p.title || '(untitled)';

        ctx.font = titleFont + 'px monospace';
        var lines = wrapText(title, innerW, maxTitleLines);

        var pub = pubByBasePath ? pubByBasePath.get(p.basePath) : null;
        if (pub) resolvePubAccent(pub);
        var cardAccent = p.basePath ? pubAccents[p.basePath] : null;
        var showAvatar = !!pub;
        var headH = showAvatar ? Math.max(logoS, avatarS) : logoS;
        var cardH = padC + headH + 7 + lines.length * lineH + 5 + metaFont + padC;

        var cardX = sx - cardW / 2;
        var cardY = sy - anchorGap - cardH - (1 - unfurl) * 10;
        var below = false;
        if (cardY < 8) { cardY = sy + anchorGap + (1 - unfurl) * 10; below = true; }
        if (cardX > W || cardX + cardW < 0 || cardY > H || cardY + cardH < 0) return;
        canPlace(sx, cardY + cardH / 2, cardW, cardH); // reserve so labels avoid the card

        if (showAvatar) loadPubImage(pub);

        var cardAlphaNow = cardAlpha * unfurl;

        // connector from card edge to the planet it describes
        var dotEdge = Math.max(6, planetR);
        ctx.globalAlpha = cardAlphaNow * 0.5;
        ctx.strokeStyle = colors.mid;
        ctx.lineWidth = 1;
        ctx.beginPath();
        if (below) { ctx.moveTo(sx, cardY); ctx.lineTo(sx, sy + dotEdge); }
        else { ctx.moveTo(sx, cardY + cardH); ctx.lineTo(sx, sy - dotEdge); }
        ctx.stroke();

        // card body
        ctx.globalAlpha = cardAlphaNow * 0.96;
        roundRectPath(cardX, cardY, cardW, cardH, 8);
        ctx.fillStyle = dark ? 'rgba(10,12,16,0.88)' : 'rgba(255,255,255,0.93)';
        ctx.fill();
        ctx.strokeStyle = idx === hoveredIndex ? colors.core : hexToRgba(colors.mid, 0.55);
        ctx.lineWidth = idx === hoveredIndex ? 2 : 1.5;
        ctx.stroke();

        // header: platform logo + name (left), publication avatar (right)
        var headCY = cardY + padC + headH / 2;
        var logo = platformLogos[platform];
        var hasLogo = logo && logo.complete && logo.naturalWidth > 0;
        var tx = cardX + padC;
        if (hasLogo) {
          ctx.drawImage(logo, tx, headCY - logoS / 2, logoS, logoS);
          tx += logoS + 5;
        }
        ctx.font = headFont + 'px monospace';
        ctx.fillStyle = colors.core;
        ctx.fillText(platform, tx, headCY);

        if (showAvatar) {
          var ax = cardX + cardW - padC - avatarS / 2;
          var img = pubImages[pub.basePath];
          if (img) {
            ctx.save();
            ctx.beginPath();
            ctx.arc(ax, headCY, avatarS / 2, 0, Math.PI * 2);
            ctx.clip();
            ctx.drawImage(img, ax - avatarS / 2, headCY - avatarS / 2, avatarS, avatarS);
            ctx.restore();
          } else {
            ctx.beginPath();
            ctx.arc(ax, headCY, avatarS / 2, 0, Math.PI * 2);
            ctx.fillStyle = colors.edge;
            ctx.fill();
            ctx.font = 'bold ' + Math.round(avatarS * 0.5) + 'px monospace';
            ctx.textAlign = 'center';
            ctx.fillStyle = colors.core;
            ctx.fillText(((pub.name || p.basePath || '?').charAt(0)).toUpperCase(), ax, headCY);
            ctx.textAlign = 'left';
          }
          ctx.beginPath();
          ctx.arc(ax, headCY, avatarS / 2, 0, Math.PI * 2);
          ctx.strokeStyle = cardAccent ? accentCss(cardAccent, dark ? 0.62 : 0.38) : hexToRgba(colors.mid, 0.6);
          ctx.lineWidth = cardAccent ? 1.5 : 1;
          ctx.stroke();
        }

        // title lines
        ctx.font = titleFont + 'px monospace';
        ctx.fillStyle = dark ? 'rgba(255,255,255,0.92)' : 'rgba(0,0,0,0.85)';
        var ty = cardY + padC + headH + 7 + lineH / 2;
        for (var l = 0; l < lines.length; l++) {
          ctx.fillText(lines[l], cardX + padC, ty);
          ty += lineH;
        }

        // meta: where it lives
        var meta = p.basePath || (p.uri.split('/')[2] || '');
        if (meta) {
          ctx.font = metaFont + 'px monospace';
          ctx.fillStyle = cardAccent ? accentCss(cardAccent, dark ? 0.66 : 0.34)
            : (dark ? 'rgba(255,255,255,0.45)' : 'rgba(0,0,0,0.45)');
          ctx.fillText(truncToChars(meta, innerW), cardX + padC, ty - lineH / 2 + 5 + metaFont / 2);
        }

        cardHitRects.push({ x: cardX, y: cardY, w: cardW, h: cardH, i: idx });
      })();
      ctx.globalAlpha = 1;
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
    } else {
      unfurlFor = -1;
    }

    // CULLING RULE: labels stay anchored above their dot/cluster. If the
    // label would extend past the viewport edge, we CULL it rather than
    // shifting it inward — shifting created a label-at-edge / dot-elsewhere
    // disconnect where you couldn't tell which dot a label described.
    function fitsHoriz(lx, halfW) {
      return lx - halfW >= LABEL_MARGIN && lx + halfW <= W - LABEL_MARGIN;
    }

    if (coarseAlpha > 0.01) {
      ctx.font = (small ? '9px' : '12px') + ' monospace';
      ctx.globalAlpha = 0.95 * coarseAlpha;
      ctx.fillStyle = dark ? 'rgba(255,255,255,0.95)' : 'rgba(0,0,0,0.88)';
      var fontSize = small ? 9 : 12;
      // Cap region labels hard — a handful of the biggest regions is enough to
      // orient; more just clutters, especially on a phone.
      var maxCoarse = ATLAS_TUNE.labels.coarse[small ? 's' : 'l'];
      var shownCoarse = 0;
      var sorted = data.clusters.coarse.slice().sort(function(a, b) { return b.count - a.count; });
      for (var c = 0; c < sorted.length && shownCoarse < maxCoarse; c++) {
        var cl = sorted[c];
        var sx = cx + cl.cx * scale, sy = cy + cl.cy * scale - Math.sqrt(cl.count) * 1.5;
        if (sy < LABEL_MARGIN || sy > H - 40) continue;
        var tw = ctx.measureText(cl.label).width;
        if (!fitsHoriz(sx, tw / 2)) continue;
        if (canPlace(sx, sy, tw, fontSize)) { drawLabel(cl.label, sx, sy, dark); shownCoarse++; }
      }
    }

    if (fineAlpha > 0.01) {
      // bold: cluster labels are the landmarks of this zoom range — they must
      // read above the per-node text, not blend into it
      ctx.font = 'bold ' + (small ? '9px' : '12px') + ' monospace';
      ctx.globalAlpha = 0.95 * fineAlpha;
      ctx.fillStyle = dark ? 'rgba(255,255,255,0.95)' : 'rgba(0,0,0,0.88)';
      var fontSize = small ? 9 : 12;
      var maxFine = ATLAS_TUNE.labels.fine[small ? 's' : 'l'];
      var shownFine = 0;
      var sorted = data.clusters.fine.slice().sort(function(a, b) { return b.count - a.count; });
      for (var c = 0; c < sorted.length && shownFine < maxFine; c++) {
        var cl = sorted[c];
        if (cl.cx < xMin || cl.cx > xMax || cl.cy < yMin || cl.cy > yMax) continue;
        var sx = cx + cl.cx * scale, sy = cy + cl.cy * scale - 14;
        if (sy < LABEL_MARGIN || sy > H - 40) continue;
        var tw = ctx.measureText(cl.label).width;
        if (!fitsHoriz(sx, tw / 2)) continue;
        if (canPlace(sx, sy, tw, fontSize)) { drawLabel(cl.label, sx, sy, dark); shownFine++; }
      }
    }

    if (titleAlpha > 0.01) {
      // Title cap stays at 20 mobile / 50 desktop regardless of zoom.
      // A previous attempt to lift these caps at high zoom flooded the
      // small viewport — the *screen size* dictates how many titles fit,
      // not how many dots are theoretically on screen. Font growth is
      // desktop-only and capped tight.
      var baseFont = small ? 9 : 11;
      var fontSize = baseFont + (small ? 0 : Math.min(2, Math.max(0, Math.floor((zoom - 25) / 10))));
      ctx.font = fontSize + 'px monospace';
      ctx.globalAlpha = 0.9 * titleAlpha;
      ctx.fillStyle = dark ? 'rgba(255,255,255,0.95)' : 'rgba(0,0,0,0.88)';
      // Far fewer titles than before, and the ones we DO show are the most
      // popular (publication size + real recommend counts) rather than whoever
      // sorted early in the array \u2014 see labelOrder. On a phone a small handful
      // is all that stays legible.
      var maxLabels = ATLAS_TUNE.labels.titles[small ? 's' : 'l'];
      var truncLen = small ? 22 : 45;
      var iconSize = small ? 12 : 14;
      var iconGap = 4;
      var shown = 0;
      var order = labelOrder; // popular-first; falls back to array order if unset

      for (var oi = 0; oi < n && shown < maxLabels; oi++) {
        var i = order ? order[oi] : oi;
        var px = pointsX[i], py = pointsY[i];
        if (px < xMin || px > xMax || py < yMin || py > yMax) continue;
        var title = data.points[i].title;
        if (!title) continue;
        var sx = cx + px * scale, sy = cy + py * scale - (planetR > 0 ? planetR + 8 : 10);
        if (sy < LABEL_MARGIN || sy > H - 40) continue;
        if (title.length > truncLen) title = title.substring(0, truncLen - 2) + '\u2026';
        var tw = ctx.measureText(title).width;

        // include the platform icon in the bounding box. If the logo isn't
        // ready yet, fall back to text-only positioning so the layout
        // doesn't jitter when icons load in.
        var platform = PLATFORMS[platformIdx[i]];
        var logo = platformLogos[platform];
        var hasLogo = logo && logo.complete && logo.naturalWidth > 0;
        var iconW = hasLogo ? (iconSize + iconGap) : 0;
        var contentW = iconW + tw;
        var halfW = contentW / 2;

        // CULL if the (icon + title) would extend past the viewport edge.
        // Shifting the label inward disconnects it from its dot and makes
        // it unclear which dot the label describes.
        if (!fitsHoriz(sx, halfW)) continue;
        if (canPlace(sx, sy, contentW, fontSize)) {
          if (hasLogo) {
            // draw the icon at the left, then shift the text center right so
            // the (icon + text) combo is centered on sx.
            ctx.drawImage(logo, sx - halfW, sy - iconSize / 2, iconSize, iconSize);
            drawLabel(title, sx - halfW + iconW + tw / 2, sy, dark);
          } else {
            drawLabel(title, sx, sy, dark);
          }
          shown++;
        }
      }
    }

    // --- publication name labels: last claim on the label economy ---
    // candidates were collected biggest-first in the pub circle pass; they
    // only place where cluster labels and doc titles left room
    if (pubLabelCands.length) {
      var maxPubNames = ATLAS_TUNE.labels.pubNames[small ? 's' : 'l'];
      var pubFont = small ? 8 : 10;
      ctx.font = pubFont + 'px monospace';
      ctx.globalAlpha = Math.min(0.8, fadeIn(zoom, 3, 1.0));
      ctx.fillStyle = dark ? 'rgba(255,255,255,0.85)' : 'rgba(0,0,0,0.75)';
      var shownPub = 0;
      for (var pc = 0; pc < pubLabelCands.length && shownPub < maxPubNames; pc++) {
        var cand = pubLabelCands[pc];
        if (cand.y < LABEL_MARGIN || cand.y > H - 40) continue;
        var pubName = cand.name.length > 20 ? cand.name.substring(0, 18) + '…' : cand.name;
        var pw = ctx.measureText(pubName).width;
        if (!fitsHoriz(cand.x, pw / 2)) continue;
        if (canPlace(cand.x, cand.y, pw, pubFont)) { drawLabel(pubName, cand.x, cand.y, dark); shownPub++; }
      }
    }

    ctx.globalAlpha = 1;
  }

  // --- animation loop ---
  function easeOutCubic(t) { return 1 - Math.pow(1 - t, 3); }

  function tickAnimation() {
    if (!animating) return;
    var t = Math.min(1, (Date.now() - animStart) / ANIM_DURATION);
    var e = easeOutCubic(t);
    view.zoom = animFrom.zoom + (animTo.zoom - animFrom.zoom) * e;
    view.panX = animFrom.panX + (animTo.panX - animFrom.panX) * e;
    view.panY = animFrom.panY + (animTo.panY - animFrom.panY) * e;
    view.dirty = true;
    if (t >= 1) animating = false;
  }

  function animateTo(targetX, targetY, targetZoom) {
    animFrom = { zoom: view.zoom, panX: view.panX, panY: view.panY };
    animTo = { zoom: targetZoom, panX: -targetX, panY: -targetY };
    animStart = Date.now();
    animating = true;
    scheduleFrame();
  }

  function loop() {
    frameRequested = false;
    tickAnimation();
    render();
    // keep looping while animating, or while rotating planets are on screen
    if (animating) {
      scheduleFrame();
    } else if (planetsActive) {
      view.dirty = true;
      scheduleFrame();
    }
  }

  // --- hover state ---
  var hoveredIndex = -1;
  var hoveredPub = -1; // index into pubData
  var mouseX = 0, mouseY = 0;

  function findNearestPub(sx, sy) {
    if (!pubData || pubData.length === 0) return -1;
    cacheTransform();
    var z = view.zoom;
    for (var i = 0; i < pubData.length; i++) {
      var pub = pubData[i];
      var pr = Math.min(28, Math.sqrt(pub.count) * z * 0.35);
      if (pr < 4) continue;
      var psx = cx + pub.cx * scale, psy = cy + pub.cy * scale;
      var dx = sx - psx, dy = sy - psy;
      if (dx * dx + dy * dy <= pr * pr) return i;
    }
    return -1;
  }

  function pubUrl(pub) {
    if (pub.basePath) return 'https://' + pub.basePath;
    return null;
  }

  function showPubTooltip(pubIdx, sx, sy) {
    var pub = pubData[pubIdx];
    tooltipTitle.textContent = pub.name || pub.basePath;
    tooltipMeta.textContent = pub.count + ' documents';
    tooltipPlatform.textContent = pub.platform || 'other';
    var c = frameColors[pub.platform] || frameColors.other;
    tooltipPlatform.style.background = c.edge;
    tooltipPlatform.style.color = c.core;
    tooltip.style.display = 'block';
    var tw = tooltip.offsetWidth, th = tooltip.offsetHeight;
    if (isMobile) {
      var tx = Math.max(8, Math.min(W - tw - 8, (W - tw) / 2));
      tooltip.style.left = tx + 'px';
      tooltip.style.top = '48px';
    } else {
      var tx = sx + 16, ty = sy - th - 8;
      if (tx + tw > W - 10) tx = sx - tw - 16;
      if (ty < 10) ty = sy + 16;
      tooltip.style.left = tx + 'px';
      tooltip.style.top = ty + 'px';
    }
    canvas.style.cursor = 'pointer';
  }

  // --- mobile detection ---
  var isMobile = 'ontouchstart' in window || navigator.maxTouchPoints > 0;
  var HIT_RADIUS = isMobile ? 40 : 20;

  // --- interaction state ---
  var dragging = false;
  var dragStartX, dragStartY, dragStartPanX, dragStartPanY;
  var pinchStartDist = 0, pinchStartZoom = 1;
  var pinchMidX = 0, pinchMidY = 0, pinchStartPanX = 0, pinchStartPanY = 0;

  canvas.addEventListener('wheel', function(e) {
    e.preventDefault();
    // scale zoom proportionally to deltaY — gentle for trackpad, snappy for mouse wheel
    // deltaMode 1 = lines (mouse wheel): multiply by 40 to approximate pixels
    var dy = e.deltaMode === 1 ? e.deltaY * 40 : e.deltaY;
    var factor = Math.pow(0.995, dy); // balanced: smooth trackpad, snappy mouse wheel
    var newZoom = Math.max(view.minZoom, Math.min(view.maxZoom, view.zoom * factor));
    cacheTransform();
    var d = screenToData(e.clientX, e.clientY);
    view.zoom = newZoom;
    cacheTransform();
    var d2 = screenToData(e.clientX, e.clientY);
    view.panX += d2[0] - d[0];
    view.panY += d2[1] - d[1];
    markDirty();
  }, { passive: false });

  canvas.addEventListener('mousedown', function(e) {
    if (e.button !== 0) return;
    dragging = true;
    dragStartX = e.clientX; dragStartY = e.clientY;
    dragStartPanX = view.panX; dragStartPanY = view.panY;
  });

  window.addEventListener('mousemove', function(e) {
    mouseX = e.clientX; mouseY = e.clientY;
    if (dragging) {
      cacheTransform();
      view.panX = dragStartPanX + (e.clientX - dragStartX) / scale;
      view.panY = dragStartPanY + (e.clientY - dragStartY) / scale;
      markDirty();
      hideTooltip();
      return;
    }
    cacheTransform();
    // document cards are topmost — they show everything the tooltip would,
    // so hovering one just highlights it and arms the click
    var cardIdx = findCardAt(mouseX, mouseY);
    if (cardIdx >= 0) {
      if (hoveredIndex !== cardIdx || hoveredPub !== -1) {
        hoveredPub = -1;
        hoveredIndex = cardIdx;
        tooltip.style.display = 'none';
        markDirty();
      }
      canvas.style.cursor = 'pointer';
      return;
    }
    // planets render above publication circles, so they get the next hit check;
    // hovering one unfurls its card (no tooltip — the card shows everything)
    if (planetsActive) {
      var plIdx = findNearest(mouseX, mouseY, Math.max(HIT_RADIUS, planetRadiusFor(view.zoom) + 4));
      if (plIdx >= 0) {
        if (hoveredIndex !== plIdx || hoveredPub !== -1) {
          hoveredPub = -1;
          hoveredIndex = plIdx;
          tooltip.style.display = 'none';
          markDirty();
        }
        canvas.style.cursor = 'pointer';
        return;
      }
    }
    // check publications next (rendered on top of points)
    var pi = findNearestPub(mouseX, mouseY);
    if (pi >= 0) {
      if (hoveredPub !== pi) {
        hoveredPub = pi;
        hoveredIndex = -1;
        markDirty();
        showPubTooltip(pi, mouseX, mouseY);
      }
      return;
    }
    hoveredPub = -1;
    var idx = findNearest(mouseX, mouseY, HIT_RADIUS);
    if (idx !== hoveredIndex) {
      hoveredIndex = idx;
      markDirty();
      if (idx >= 0 && !planetsActive) showTooltip(idx, mouseX, mouseY);
      else if (idx >= 0) { tooltip.style.display = 'none'; canvas.style.cursor = 'pointer'; }
      else hideTooltip();
    }
  });

  window.addEventListener('mouseup', function(e) {
    if (dragging) {
      if (Math.abs(e.clientX - dragStartX) < 4 && Math.abs(e.clientY - dragStartY) < 4) {
        // check publication click first
        if (hoveredPub >= 0) {
          var url = pubUrl(pubData[hoveredPub]);
          if (url) window.open(url, '_blank');
        } else if (hoveredIndex >= 0) {
          var p = data.points[hoveredIndex];
          var url = atUriToUrl(p.uri, p.basePath, p.platform, p.path);
          if (url) window.open(url, '_blank');
        } else if (selectedIndex >= 0) {
          // clicked empty space — dismiss a deep-link-selected card
          selectedIndex = -1;
          markDirty();
        }
      }
      dragging = false;
    }
  });

  // --- touch ---
  var touches = {};
  var touchMoved = false;
  var selectedIndex = -1; // for tap-to-select, tap-again-to-open

  canvas.addEventListener('touchstart', function(e) {
    e.preventDefault();
    for (var i = 0; i < e.changedTouches.length; i++) {
      var t = e.changedTouches[i];
      touches[t.identifier] = { x: t.clientX, y: t.clientY };
    }
    touchMoved = false;
    var ids = Object.keys(touches);
    if (ids.length === 1) {
      dragging = true;
      dragStartX = touches[ids[0]].x; dragStartY = touches[ids[0]].y;
      dragStartPanX = view.panX; dragStartPanY = view.panY;
    } else if (ids.length === 2) {
      dragging = false;
      var a = touches[ids[0]], b = touches[ids[1]];
      pinchStartDist = Math.hypot(a.x - b.x, a.y - b.y);
      pinchStartZoom = view.zoom;
      pinchMidX = (a.x + b.x) / 2;
      pinchMidY = (a.y + b.y) / 2;
      pinchStartPanX = view.panX;
      pinchStartPanY = view.panY;
    }
  }, { passive: false });

  canvas.addEventListener('touchmove', function(e) {
    e.preventDefault();
    for (var i = 0; i < e.changedTouches.length; i++) {
      var t = e.changedTouches[i];
      touches[t.identifier] = { x: t.clientX, y: t.clientY };
    }
    touchMoved = true;
    var ids = Object.keys(touches);
    if (ids.length === 1 && dragging) {
      cacheTransform();
      view.panX = dragStartPanX + (touches[ids[0]].x - dragStartX) / scale;
      view.panY = dragStartPanY + (touches[ids[0]].y - dragStartY) / scale;
      markDirty();
      hideTooltip();
      selectedIndex = -1;
    } else if (ids.length === 2) {
      var a = touches[ids[0]], b = touches[ids[1]];
      var dist = Math.hypot(a.x - b.x, a.y - b.y);
      var newZoom = Math.max(view.minZoom, Math.min(view.maxZoom, pinchStartZoom * (dist / pinchStartDist)));
      // zoom toward pinch midpoint
      var midDataOld = screenToData(pinchMidX, pinchMidY);
      view.zoom = newZoom;
      cacheTransform();
      var midDataNew = screenToData(pinchMidX, pinchMidY);
      view.panX += midDataNew[0] - midDataOld[0];
      view.panY += midDataNew[1] - midDataOld[1];
      markDirty();
    }
  }, { passive: false });

  canvas.addEventListener('touchend', function(e) {
    var endedTouches = [];
    for (var i = 0; i < e.changedTouches.length; i++) {
      endedTouches.push(e.changedTouches[i]);
      delete touches[e.changedTouches[i].identifier];
    }
    var remaining = Object.keys(touches).length;
    if (remaining === 0) {
      dragging = false;
      // tap detection — didn't drag significantly
      if (!touchMoved || (endedTouches.length === 1 &&
          Math.abs(endedTouches[0].clientX - dragStartX) < 10 &&
          Math.abs(endedTouches[0].clientY - dragStartY) < 10)) {
        var tx = endedTouches[0].clientX, ty = endedTouches[0].clientY;
        cacheTransform();
        // document cards are topmost: first tap selects (highlight border),
        // second tap opens — same pattern as dots, minus the redundant tooltip
        var cardIdx = findCardAt(tx, ty);
        if (cardIdx >= 0) {
          if (cardIdx === selectedIndex) {
            var cp = data.points[cardIdx];
            var cu = atUriToUrl(cp.uri, cp.basePath, cp.platform, cp.path);
            if (cu) window.open(cu, '_blank');
            selectedIndex = -1;
            hideTooltip();
          } else {
            selectedIndex = cardIdx;
            hoveredIndex = cardIdx;
            tooltip.style.display = 'none';
            markDirty();
          }
          return;
        }
        // planets win over publication circles (they render above them):
        // first tap selects + unfurls the card, second tap opens
        if (planetsActive) {
          var plIdx = findNearest(tx, ty, Math.max(HIT_RADIUS, planetRadiusFor(view.zoom) + 4));
          if (plIdx >= 0) {
            if (plIdx === selectedIndex) {
              var pp = data.points[plIdx];
              var pu = atUriToUrl(pp.uri, pp.basePath, pp.platform, pp.path);
              if (pu) window.open(pu, '_blank');
              selectedIndex = -1;
              hideTooltip();
            } else {
              selectedIndex = plIdx;
              hoveredIndex = plIdx;
              tooltip.style.display = 'none';
              markDirty();
            }
            return;
          }
        }
        // check publications next
        var pi = findNearestPub(tx, ty);
        if (pi >= 0) {
          var url = pubUrl(pubData[pi]);
          if (url) window.open(url, '_blank');
          selectedIndex = -1;
          hideTooltip();
        } else {
          var idx = findNearest(tx, ty, HIT_RADIUS);
          if (idx >= 0) {
            if (idx === selectedIndex) {
              var p = data.points[idx];
              var url = atUriToUrl(p.uri, p.basePath, p.platform, p.path);
              if (url) window.open(url, '_blank');
              selectedIndex = -1;
              hideTooltip();
            } else {
              selectedIndex = idx;
              hoveredIndex = idx;
              showTooltip(idx, tx, ty);
              markDirty();
            }
          } else {
            selectedIndex = -1;
            hideTooltip();
            markDirty();
          }
        }
      }
    }
  });

  // --- tooltip ---
  var tooltip = document.getElementById('tooltip');
  var tooltipTitle = document.getElementById('tooltip-title');
  var tooltipMeta = document.getElementById('tooltip-meta');
  var tooltipPlatform = document.getElementById('tooltip-platform');

  function showTooltip(idx, sx, sy) {
    var p = data.points[idx];
    tooltipTitle.textContent = p.title || '(untitled)';
    tooltipMeta.textContent = p.basePath || p.uri;
    tooltipPlatform.textContent = p.platform;
    var c = frameColors[p.platform] || frameColors.other;
    tooltipPlatform.style.background = c.edge;
    tooltipPlatform.style.color = c.core;
    tooltip.style.display = 'block';
    var tw = tooltip.offsetWidth, th = tooltip.offsetHeight;
    if (isMobile) {
      // on mobile, anchor tooltip at top center of screen
      var tx = Math.max(8, Math.min(W - tw - 8, (W - tw) / 2));
      tooltip.style.left = tx + 'px';
      tooltip.style.top = '48px';
    } else {
      var tx = sx + 16, ty = sy - th - 8;
      if (tx + tw > W - 10) tx = sx - tw - 16;
      if (ty < 10) ty = sy + 16;
      tooltip.style.left = tx + 'px';
      tooltip.style.top = ty + 'px';
    }
    canvas.style.cursor = 'pointer';
  }

  function hideTooltip() {
    tooltip.style.display = 'none';
    hoveredIndex = -1;
    hoveredPub = -1;
    canvas.style.cursor = dragging ? 'grabbing' : 'grab';
  }

  function atUriToUrl(uri, basePath, platform, path) {
    var m = uri.match(/^at:\/\/(did:[^/]+)\/([^/]+)\/(.+)$/);
    if (!m) return null;
    var did = m[1], collection = m[2], rkey = m[3];
    if (platform === 'whitewind' || collection.startsWith('com.whtwnd.')) return 'https://whtwnd.com/' + did + '/' + rkey;
    // skip non-document-serving hosts (blento is a card portal, not a document platform)
    var usableBase = basePath && !basePath.startsWith('blento.app');
    // explicit path wins — the rkey form below is a leaflet.pub convention and must
    // not override an author-set path (site.standard.document records embedding
    // pub.leaflet.content get tagged platform=leaflet but are served by their path)
    if (usableBase && path) {
      var sep = path.charAt(0) === '/' ? '' : '/';
      return 'https://' + basePath + sep + path;
    }
    // leaflet uses rkey directly
    if (platform === 'leaflet' && usableBase) return 'https://' + basePath + '/' + rkey;
    // leaflet without basePath
    if (platform === 'leaflet') return 'https://leaflet.pub/p/' + did + '/' + rkey;
    if (usableBase) return 'https://' + basePath + '/' + rkey;
    // universal fallback — AT Protocol record viewer
    return 'https://pdsls.dev/at/' + did + '/' + collection + '/' + rkey;
  }

  // --- platform filter state ---
  var activePlatforms = null; // null = all visible, Set = only these

  function renderLegend() {
    var el = document.getElementById('legend');
    if (!frameColors) cacheFrameColors();
    var html = '';
    for (var i = 0; i < PLATFORMS.length; i++) {
      var p = PLATFORMS[i];
      var dimmed = activePlatforms && !activePlatforms.has(p) ? ' dimmed' : '';
      var icon = window.PubPlatforms.iconUrl(p);
      var logo = icon ? '<img class="legend-logo" src="' + icon + '" alt="" loading="lazy" onerror="this.remove()">' : '';
      html += '<div class="legend-item' + dimmed + '" data-platform="' + p + '"><span class="legend-dot" style="background:' + frameColors[p].mid + '"></span>' + logo + p + '</div>';
    }
    el.innerHTML = html;
    // attach click handlers
    var items = el.querySelectorAll('.legend-item');
    for (var i = 0; i < items.length; i++) {
      items[i].addEventListener('click', onLegendClick);
    }
  }

  function onLegendClick(e) {
    var item = e.currentTarget;
    var platform = item.getAttribute('data-platform');
    if (!activePlatforms) {
      // first click: select only this platform
      activePlatforms = new Set([platform]);
    } else if (activePlatforms.has(platform)) {
      activePlatforms.delete(platform);
      // if nothing selected, show all
      if (activePlatforms.size === 0) activePlatforms = null;
    } else {
      activePlatforms.add(platform);
      // if all selected, reset to null
      if (activePlatforms.size === PLATFORMS.length) activePlatforms = null;
    }
    renderLegend();
    markDirty();
  }

  // Sort labelOrder (point indices) most-popular-first from popScore. Cheap
  // enough to redo when the recommend boost arrives.
  function rebuildLabelOrder() {
    if (!labelOrder || !popScore) return;
    var arr = Array.prototype.slice.call(labelOrder);
    arr.sort(function(a, b) { return popScore[b] - popScore[a]; });
    for (var i = 0; i < arr.length; i++) labelOrder[i] = arr[i];
  }

  // Layer real recommend counts onto popScore — "the best information about
  // what is actually popular." One cached backend call returns the top-N
  // recommended docs network-wide; we boost any that exist in the atlas so
  // their titles win label slots. Best-effort: failure just leaves the
  // publication-size baseline in place.
  function fetchRecommendBoost() {
    fetch(API_URL + '/recommended?since=all&limit=' + ATLAS_TUNE.recommend.limit + '&sort=top')
      .then(function(r) { return r.ok ? r.json() : null; })
      .then(function(rows) {
        if (!rows || !rows.length || !uriToIndex) return;
        var bumped = 0;
        for (var k = 0; k < rows.length; k++) {
          var idx = uriToIndex.get(rows[k].uri);
          if (idx === undefined) continue;
          // big additive floor so any recommended doc outranks pure pub-size,
          // plus a log term so heavily-recommended docs sort among themselves.
          popScore[idx] += ATLAS_TUNE.recommend.boostFloor + Math.log(1 + (rows[k].totalCount || rows[k].recommendCount || 1));
          bumped++;
        }
        if (bumped > 0) { rebuildLabelOrder(); markDirty(); }
      })
      .catch(function() {});
  }

  function loadData() {
    // start logo prefetch in parallel — they're small (<60KB total) and we
    // want them ready by the time the user zooms in far enough for titles.
    loadPlatformLogos();
    fetch('atlas.json')
      .then(function(r) {
        if (!r.ok) throw new Error('failed to load atlas.json: ' + r.status);
        return r.json();
      })
      .then(function(d) {
        data = d;
        var n = d.points.length;
        pointsX = new Float32Array(n);
        pointsY = new Float32Array(n);
        platformIdx = new Uint8Array(n);
        var platMap = {};
        for (var p = 0; p < PLATFORMS.length; p++) platMap[PLATFORMS[p]] = p;
        var otherIdx = platMap.other;
        for (var i = 0; i < n; i++) {
          pointsX[i] = d.points[i].x;
          pointsY[i] = d.points[i].y;
          platformIdx[i] = platMap[d.points[i].platform] !== undefined ? platMap[d.points[i].platform] : otherIdx;
        }
        // build URI → index map for search matching
        uriToIndex = new Map();
        for (var i = 0; i < n; i++) {
          uriToIndex.set(d.points[i].uri, i);
        }
        // build cluster metadata: fine cluster array, dominant platform, spatial radius
        // Uint16, not Uint8: fine cluster ids run into the hundreds, and
        // truncation aliased distant clusters together (id 300 → 44), which
        // drew connection lines between unrelated documents.
        clusterFineArr = new Uint16Array(n);
        var coarsePlatCounts = {};
        var finePlatCounts = {};
        for (var i = 0; i < n; i++) {
          var cc = d.points[i].clusterCoarse;
          var cf = d.points[i].clusterFine;
          clusterFineArr[i] = cf;
          if (!coarsePlatCounts[cc]) coarsePlatCounts[cc] = new Uint16Array(PLATFORMS.length);
          if (!finePlatCounts[cf]) finePlatCounts[cf] = new Uint16Array(PLATFORMS.length);
          coarsePlatCounts[cc][platformIdx[i]]++;
          finePlatCounts[cf][platformIdx[i]]++;
        }
        function dominantPlatform(counts) {
          if (!counts) return 'other';
          var best = 0, bestP = 0;
          for (var p = 0; p < PLATFORMS.length; p++) {
            if (counts[p] > best) { best = counts[p]; bestP = p; }
          }
          return PLATFORMS[bestP];
        }
        var coarseById = {};
        for (var c = 0; c < d.clusters.coarse.length; c++) {
          var cl = d.clusters.coarse[c];
          cl.dominantPlatform = dominantPlatform(coarsePlatCounts[cl.id]);
          cl._distSum = 0; cl._distN = 0;
          coarseById[cl.id] = cl;
        }
        var fineById = {};
        for (var c = 0; c < d.clusters.fine.length; c++) {
          var cl = d.clusters.fine[c];
          cl.dominantPlatform = dominantPlatform(finePlatCounts[cl.id]);
          cl._distSum = 0; cl._distN = 0;
          fineById[cl.id] = cl;
        }
        for (var i = 0; i < n; i++) {
          var ccl = coarseById[d.points[i].clusterCoarse];
          if (ccl) {
            var dx = pointsX[i] - ccl.cx, dy = pointsY[i] - ccl.cy;
            ccl._distSum += Math.sqrt(dx * dx + dy * dy);
            ccl._distN++;
          }
          var fcl = fineById[d.points[i].clusterFine];
          if (fcl) {
            var dx = pointsX[i] - fcl.cx, dy = pointsY[i] - fcl.cy;
            fcl._distSum += Math.sqrt(dx * dx + dy * dy);
            fcl._distN++;
          }
        }
        for (var c = 0; c < d.clusters.coarse.length; c++) {
          var cl = d.clusters.coarse[c];
          cl.radius = cl._distN > 0 ? (cl._distSum / cl._distN) * 2 : 0.05;
        }
        for (var c = 0; c < d.clusters.fine.length; c++) {
          var cl = d.clusters.fine[c];
          cl.radius = cl._distN > 0 ? (cl._distSum / cl._distN) * 2 : 0.02;
        }
        // load publication data
        pubData = d.publications || [];
        pubByBasePath = new Map();
        for (var pi = 0; pi < pubData.length; pi++) {
          if (pubData[pi].basePath) pubByBasePath.set(pubData[pi].basePath, pubData[pi]);
        }
        // Render the biggest publications first so the per-frame avatar budget
        // (see render) spends on the most prominent ones — the rest stay quiet.
        pubData.sort(function(a, b) { return (b.count || 0) - (a.count || 0); });

        // --- fine-cluster lanterns ---
        // Each cluster's glow is a single light source at the weighted center
        // of its members (outliers beyond cl.radius trimmed), sized by the RMS
        // spread of the group — so the lantern sits where the people are and
        // reaches as far as they do, not as far as the farthest straggler.
        var finePts = {};
        for (var i = 0; i < n; i++) {
          var cf = d.points[i].clusterFine;
          (finePts[cf] || (finePts[cf] = [])).push([pointsX[i], pointsY[i]]);
        }
        for (var c = 0; c < d.clusters.fine.length; c++) {
          var cl = d.clusters.fine[c];
          var pts = finePts[cl.id];
          if (!pts) { cl.lantern = null; continue; }
          var maxR = cl.radius || 0.05;
          var kept = [];
          for (var k = 0; k < pts.length; k++) {
            var ddx = pts[k][0] - cl.cx, ddy = pts[k][1] - cl.cy;
            if (ddx * ddx + ddy * ddy <= maxR * maxR) kept.push(pts[k]);
          }
          if (!kept.length) kept = pts;
          var mx = 0, my = 0;
          for (var k = 0; k < kept.length; k++) { mx += kept[k][0]; my += kept[k][1]; }
          mx /= kept.length; my /= kept.length;
          var sum2 = 0;
          for (var k = 0; k < kept.length; k++) {
            var dx2 = kept[k][0] - mx, dy2 = kept[k][1] - my;
            sum2 += dx2 * dx2 + dy2 * dy2;
          }
          cl.lantern = { x: mx, y: my, r: Math.max(0.008, Math.sqrt(sum2 / kept.length)) };
        }

        // --- popularity score per point ---
        // base = log publication size (offline, always available). The
        // recommend fetch below layers real endorsement counts on top.
        popScore = new Float32Array(n);
        labelOrder = new Int32Array(n);
        for (var i = 0; i < n; i++) {
          var pub = pubByBasePath.get(d.points[i].basePath);
          popScore[i] = pub ? Math.log(1 + (pub.count || 0)) : 0;
          labelOrder[i] = i;
        }
        rebuildLabelOrder();
        fetchRecommendBoost();

        buildSpatialIndex();
        renderLegend();
        var statsText = n.toLocaleString() + ' documents \u00B7 ' +
          d.clusters.coarse.length + ' regions \u00B7 ' +
          d.clusters.fine.length + ' clusters';
        if (pubData.length > 0) statsText += ' \u00B7 ' + pubData.length + ' publications';
        document.getElementById('stats').textContent = statsText;
        document.getElementById('loading').classList.add('hidden');
        markDirty();
        // jump to specific document by URI (from "view on atlas" links)
        if (pendingUri) {
          var idx = uriToIndex.get(pendingUri);
          if (idx !== undefined) {
            searchMatches = new Set([idx]);
            searchCenter = { x: pointsX[idx], y: pointsY[idx] };
            searchQuery = d.points[idx].title || '';
            setSearchStatus('1 document');
            var targetZ = pendingZoom || 12;
            animateTo(searchCenter.x, searchCenter.y, targetZ);
            // show tooltip after animation — unless we land at card zoom,
            // where the unfurled card already shows everything
            setTimeout(function() {
              cacheTransform();
              var sx = cx + pointsX[idx] * scale;
              var sy = cy + pointsY[idx] * scale;
              hoveredIndex = idx;
              selectedIndex = idx;
              if (targetZ < CARD_START) showTooltip(idx, sx, sy);
              else markDirty();
            }, ANIM_DURATION + 50);
          }
          pendingUri = null;
        }
        // jump to publication centroid (from "view publication on atlas" links)
        else if (pendingPub) {
          var pub = pubByBasePath && pubByBasePath.get(pendingPub);
          if (pub) {
            setSearchStatus(pub.name || pub.basePath);
            animateTo(pub.cx, pub.cy, 7);
          } else {
            setSearchStatus('publication not on atlas');
          }
          pendingPub = null;
        }
        // bare ?x=&y=&z= — jump straight to a spot (debug / sharing)
        else if (pendingZoom) {
          var pjx = parseFloat(urlParams.get('x')) || 0;
          var pjy = parseFloat(urlParams.get('y')) || 0;
          animateTo(pjx, pjy, pendingZoom);
        }
        // apply prefetched search results (fired in parallel with atlas.json)
        else if (pendingSearchResults) {
          pendingSearchResults.then(function(resp) {
            pendingSearchResults = null;
            if (resp) applySearchResults(resp, searchInput.value);
          });
        }
      })
      .catch(function(err) {
        document.getElementById('loading').querySelector('.spinner').textContent = 'error: ' + err.message;
        console.error(err);
      });
  }

  // --- search ---
  var API_URL = 'https://leaflet-search-backend.fly.dev';
  var searchInput = document.getElementById('search-input');
  var searchForm = document.getElementById('search-form');
  var searchStatusEl = null;
  var pendingSearchResults = null; // promise for prefetched search results
  var pendingUri = null; // URI to jump to after data loads (from "view on atlas" links)
  var pendingPub = null; // basePath to jump to (from "view publication on atlas" links)
  var pendingZoom = null; // ?z= zoom override for uri deep-links

  window.addEventListener('resize', resizeCanvas);
  resizeCanvas();
  // jump to specific document by URI, publication by basePath, or prefetch search results
  var urlParams = new URLSearchParams(window.location.search);
  var urlUri = urlParams.get('uri');
  var urlPub = urlParams.get('pub');
  var urlQ = urlParams.get('q');
  // optional zoom override for ?uri= deep-links (also handy for debugging)
  var urlZ = parseFloat(urlParams.get('z'));
  if (urlZ > 0) pendingZoom = Math.max(view.minZoom, Math.min(view.maxZoom, urlZ));
  if (urlUri) {
    pendingUri = urlUri;
  } else if (urlPub) {
    pendingPub = urlPub;
  } else if (urlQ) {
    searchInput.value = urlQ;
    pendingSearchResults = fetch(API_URL + buildSearchUrl(urlQ))
      .then(function(r) { return r.ok ? r.json() : null; })
      .catch(function() { return null; });
  }
  loadData();
  scheduleFrame();

  function setSearchStatus(msg) {
    if (!searchStatusEl) {
      searchStatusEl = document.createElement('span');
      searchStatusEl.className = 'search-status';
      searchForm.appendChild(searchStatusEl);
    }
    searchStatusEl.textContent = msg;
  }

  function clearSearch() {
    searchMatches = null;
    searchCenter = null;
    searchQuery = '';
    setSearchStatus('');
    var url = new URL(window.location);
    if (url.searchParams.has('q')) {
      url.searchParams.delete('q');
      history.replaceState(null, '', url);
    }
    markDirty();
  }

  function applySearchResults(resp, query) {
    searchQuery = query;
    var results = (resp && resp.results) || [];
    if (results.length === 0) {
      setSearchStatus('no results');
      searchMatches = null;
      searchCenter = null;
      markDirty();
      return;
    }

    // match result URIs to atlas points
    var matches = new Set();
    var weightedX = 0, weightedY = 0, totalWeight = 0;
    for (var i = 0; i < results.length; i++) {
      var uri = results[i].uri;
      if (uriToIndex && uriToIndex.has(uri)) {
        var idx = uriToIndex.get(uri);
        matches.add(idx);
        var w = results.length - i;
        weightedX += pointsX[idx] * w;
        weightedY += pointsY[idx] * w;
        totalWeight += w;
      }
    }

    if (matches.size === 0) {
      setSearchStatus(results.length + ' results, 0 on map');
      searchMatches = null;
      searchCenter = null;
      markDirty();
      return;
    }

    searchMatches = matches;
    searchCenter = { x: weightedX / totalWeight, y: weightedY / totalWeight };
    setSearchStatus(matches.size + ' of ' + results.length + ' on map');

    var maxDist = 0;
    matches.forEach(function(idx) {
      var dx = pointsX[idx] - searchCenter.x;
      var dy = pointsY[idx] - searchCenter.y;
      var d = Math.sqrt(dx * dx + dy * dy);
      if (d > maxDist) maxDist = d;
    });

    var targetZoom = maxDist > 0 ? Math.min(view.maxZoom, 0.3 / maxDist) : 6;
    targetZoom = Math.max(4, Math.min(15, targetZoom));
    animateTo(searchCenter.x, searchCenter.y, targetZoom);
  }

  // mirrors index.html's `@handle.tld` extraction so the syntax is
  // consistent across pages. unquoted `@handle.tld` or `@did:plc:...`
  // becomes an `author=` filter; the rest is the q text.
  function parseSearchQuery(raw) {
    var unquoted = raw.replace(/"[^"]*"/g, '');
    var didMatch = unquoted.match(/(?:^|\s)@(did:[a-z]+:[A-Za-z0-9._:-]+)/);
    var handleMatch = !didMatch && unquoted.match(/(?:^|\s)@([\w.-]+\.\w+)/);
    var text = raw.trim();
    var author = null;
    if (didMatch) {
      author = didMatch[1];
      text = raw.replace(new RegExp('\\s*@' + author.replace(/[.:]/g, '\\$&') + '\\s*'), ' ').trim();
    } else if (handleMatch) {
      author = handleMatch[1];
      text = raw.replace(new RegExp('\\s*@' + author.replace(/\./g, '\\.') + '\\s*'), ' ').trim();
    }
    return { text: text, author: author };
  }

  // when filter-only (author set, no text), use keyword mode — semantic
  // needs a query to embed and would return nothing.
  function buildSearchUrl(raw) {
    var parsed = parseSearchQuery(raw);
    var mode = (parsed.text.length === 0 && parsed.author) ? 'keyword' : 'semantic';
    var url = '/search?mode=' + mode + '&limit=20&format=v2&q=' + encodeURIComponent(parsed.text);
    if (parsed.author) url += '&author=' + encodeURIComponent(parsed.author);
    return url;
  }

  function doSearch(query, skipPush) {
    if (!query || !data || !uriToIndex) return;
    setSearchStatus('searching...');
    if (!skipPush) {
      var url = new URL(window.location);
      url.searchParams.set('q', query);
      history.replaceState(null, '', url);
    }

    fetch(API_URL + buildSearchUrl(query))
      .then(function(r) {
        if (!r.ok) throw new Error('search failed: ' + r.status);
        return r.json();
      })
      .then(function(resp) { applySearchResults(resp, query); })
      .catch(function(err) {
        setSearchStatus('error');
        console.error(err);
      });
  }

  // register typeahead FIRST so its keydown handler intercepts Enter/Escape
  // before the form submit / clear-search handlers below see them.
  if (window.LeafletUI) {
    window.LeafletUI.setupTypeahead(searchInput, {
      onPick: function() {
        if (searchForm.requestSubmit) searchForm.requestSubmit();
        else searchForm.dispatchEvent(new Event('submit', { cancelable: true }));
      },
    });
  }

  searchForm.addEventListener('submit', function(e) {
    e.preventDefault();
    var q = searchInput.value.trim();
    if (q) doSearch(q);
    else clearSearch();
  });

  // bare Escape (when typeahead is closed) clears the search. typeahead
  // intercepts Escape via stopImmediatePropagation when its dropdown is
  // open, so this only fires in the closed state.
  searchInput.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
      searchInput.value = '';
      searchInput.blur();
      clearSearch();
    }
  });

  // cmd+k / ctrl+k to focus search
  window.addEventListener('keydown', function(e) {
    if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
      e.preventDefault();
      searchInput.focus();
      searchInput.select();
    }
  });

  window.atlas = {
    setDirty: function() {
      sprites = null;
      dotSprites = null;
      haloSprites = null;
      renderLegend();
      markDirty();
    },
    _debug: function() {
      return {
        zoom: view.zoom, panX: view.panX, panY: view.panY,
        animating: animating, planetsActive: planetsActive,
        hovered: hoveredIndex, selected: selectedIndex,
      };
    }
  };
})();
