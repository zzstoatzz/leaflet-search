// atlas-gl.js — WebGL renderer for the atlas scene.
//
// Grew out of planet-gl.js (planet quads only) into the whole GL layer:
// the 83k document points, the intra-cluster connection lines, and the
// rotating planets all render here in one pass on a dedicated canvas that
// sits between the background canvas (nebulae) and the top canvas (labels,
// cards). atlas.js stays a plain 2D app for everything typographic.
//
// Why: the old path looped over every point in JS with one ctx.drawImage
// each, per frame. Here point positions live in a GPU buffer uploaded once;
// pan/zoom is a handful of uniform updates and a single draw call. Point
// glyphs are drawn analytically in the fragment shader (same light model as
// the old canvas sprites: upper-left key light, limb darkening, specular
// kiss, starlight core) so they're antialiased at any DPR and their
// star→sphere progression is a continuous function of zoom, not a swap
// between quantized pre-rasterized sprites.
(function() {
  'use strict';

  // --- planet shaders (unchanged from planet-gl.js) ---
  var PLANET_VERT = [
    'attribute vec2 aPos;',
    'uniform vec2 uRes;',
    'uniform vec2 uCenter;',
    'uniform float uRadius;',
    'uniform float uMargin;',
    'varying vec2 vP;',
    'void main() {',
    '  vP = aPos * uMargin;',
    // +y up in sphere space; screen y grows down
    '  vec2 px = uCenter + vec2(aPos.x, -aPos.y) * uRadius * uMargin;',
    '  vec2 clip = (px / uRes) * 2.0 - 1.0;',
    '  gl_Position = vec4(clip.x, -clip.y, 0.0, 1.0);',
    '}',
  ].join('\n');

  var PLANET_FRAG = [
    '#ifdef GL_FRAGMENT_PRECISION_HIGH',
    'precision highp float;',
    '#else',
    'precision mediump float;',
    '#endif',
    'varying vec2 vP;',
    'uniform sampler2D uTex;',
    'uniform float uRot, uTilt, uAlpha, uSeed, uTexSpan, uHover, uDark, uPx, uMargin, uLift;',
    'uniform vec3 uBase, uAccent;',

    'float hash3(vec3 p) {',
    '  p = fract(p * 0.3183099 + 0.1) + uSeed * 0.013;',
    '  p *= 17.0;',
    '  return fract(p.x * p.y * p.z * (p.x + p.y + p.z));',
    '}',
    'float noise3(vec3 x) {',
    '  vec3 i = floor(x), f = fract(x);',
    '  f = f * f * (3.0 - 2.0 * f);',
    '  return mix(',
    '    mix(mix(hash3(i), hash3(i + vec3(1,0,0)), f.x),',
    '        mix(hash3(i + vec3(0,1,0)), hash3(i + vec3(1,1,0)), f.x), f.y),',
    '    mix(mix(hash3(i + vec3(0,0,1)), hash3(i + vec3(1,0,1)), f.x),',
    '        mix(hash3(i + vec3(0,1,1)), hash3(i + vec3(1,1,1)), f.x), f.y), f.z);',
    '}',
    'float fbm(vec3 p) {',
    '  float v = 0.5 * noise3(p);',
    '  p = p * 2.03 + 11.3; v += 0.275 * noise3(p);',
    '  p = p * 2.03 + 11.3; v += 0.151 * noise3(p);',
    '  p = p * 2.03 + 11.3; v += 0.083 * noise3(p);',
    '  return v;',
    '}',

    'void main() {',
    '  float r = length(vP);',
    '  vec4 outc = vec4(0.0);',
    '  float ct = cos(uTilt), st = sin(uTilt);',
    // the info shell: billboards in orbit, a few percent above the surface —
    // they float over the terrain and hang past the limb into space
    '  vec4 em = vec4(0.0);',
    '  if (r < uLift) {',
    '    float zt = sqrt(uLift * uLift - r * r);',
    '    vec3 Nb = vec3(vP, zt) / uLift;',
    '    vec3 Nbt = vec3(Nb.x, Nb.y * ct + Nb.z * st, -Nb.y * st + Nb.z * ct);',
    '    float latB = asin(clamp(Nbt.y, -1.0, 1.0));',
    '    float lonB = atan(Nbt.x, Nbt.z) + uRot;',
    '    em = texture2D(uTex, vec2(fract(lonB * 0.15915494) * uTexSpan, clamp(0.5 - latB * 0.31830988, 0.0, 1.0)));',
    '    em.a *= smoothstep(uLift, uLift - 6.0 * uPx, r);',
    '  }',
    '  if (r < 1.0) {',
    '    float z = sqrt(1.0 - r * r);',
    '    vec3 N = vec3(vP, z);',
    // tilt: we orbit slightly north of the equator, so the equator dips
    '    vec3 Nt = vec3(N.x, N.y * ct + N.z * st, -N.y * st + N.z * ct);',
    '    float lat = asin(clamp(Nt.y, -1.0, 1.0));',
    '    float lon = atan(Nt.x, Nt.z) + uRot;',
    // terrain in the planet-fixed frame so it spins with the surface
    '    vec3 P = vec3(cos(lat) * sin(lon), sin(lat), cos(lat) * cos(lon));',
    '    float terr = fbm(P * 2.8);',
    '    float terr2 = fbm(P * 6.1 + 31.7);',
    '    vec3 land = mix(uBase * 0.45, uBase * 1.4, smoothstep(0.32, 0.68, terr));',
    '    land = mix(land, uAccent * 0.6, smoothstep(0.56, 0.78, terr2) * 0.5);',
    '    float cap = smoothstep(0.78, 0.94, abs(sin(lat)) + (terr2 - 0.5) * 0.15);',
    '    land = mix(land, vec3(0.82, 0.88, 0.94), cap * 0.65);',
    // lighting: sun upper-left, soft terminator
    '    vec3 L = normalize(vec3(-0.5, 0.45, 0.62));',
    '    float ndl = dot(N, L);',
    '    float day = smoothstep(-0.12, 0.3, ndl);',
    '    float ambient = mix(0.55, 0.10, uDark);',
    '    vec3 surf = land * (ambient + (1.0 - ambient) * max(ndl, 0.0));',
    '    vec3 H = normalize(L + vec3(0.0, 0.0, 1.0));',
    '    surf += uAccent * pow(max(dot(N, H), 0.0), 70.0) * 0.4 * day;',
    // atmosphere hugging the limb
    '    float fres = pow(1.0 - z, 2.2);',
    '    surf += uAccent * fres * (0.55 + uHover * 0.5);',
    // the orbital billboards over the surface: emissive, brightest at night
    '    float emBoost = mix(1.0, mix(1.7, 1.1, day), uDark);',
    '    surf = mix(surf, em.rgb * emBoost, em.a * 0.95);',
    '    surf += em.rgb * em.a * 0.4 * (1.0 - day) * uDark;',
    '    float edge = smoothstep(1.0, 1.0 - 3.0 * uPx, r);',
    '    outc = vec4(surf, edge);',
    '  } else {',
    // past the limb: atmosphere halo, with billboards hanging into space
    '    float d = (r - 1.0) / (uMargin - 1.0);',
    '    float glow = pow(max(1.0 - d, 0.0), 2.6);',
    '    vec3 col = uAccent * (0.8 + 0.4 * glow);',
    '    float a = glow * (0.32 + uHover * 0.28);',
    '    float billBoost = mix(1.0, 1.45, uDark);',
    '    col = mix(col, em.rgb * billBoost, em.a);',
    '    a = max(a, em.a * 0.95);',
    '    outc = vec4(col, a);',
    '  }',
    '  gl_FragColor = vec4(outc.rgb, outc.a * uAlpha);',
    '}',
  ].join('\n');

  // --- point shaders ---
  // one gl.POINTS draw over the whole corpus. Position/color/state live in
  // GPU buffers; zoom-derived look (radius, starness, alpha) arrives as
  // uniforms so every zoom transition is continuous.
  var PALETTE_SIZE = 32;

  var POINT_VERT = [
    'attribute vec2 aPos;',       // data-space coords
    'attribute float aColor;',    // palette index
    'attribute float aState;',    // 0 normal, 1 dim, 2 highlight
    'uniform vec2 uRes;',
    'uniform float uScale;',      // css px per data unit
    'uniform vec2 uCenter;',      // css px offset of data origin
    'uniform float uDpr;',
    'uniform float uRadius;',     // glyph radius, css px
    'uniform float uEmph;',       // 1 when re-drawing the hovered point
    'uniform vec3 uCore[' + PALETTE_SIZE + '];',
    'uniform vec3 uMid[' + PALETTE_SIZE + '];',
    'uniform vec3 uEdge[' + PALETTE_SIZE + '];',
    'varying vec3 vCore, vMid, vEdge;',
    'varying float vState, vAA;',
    'void main() {',
    '  vec2 px = uCenter + aPos * uScale;',
    '  vec2 clip = (px / uRes) * 2.0 - 1.0;',
    '  gl_Position = vec4(clip.x, -clip.y, 0.0, 1.0);',
    '  float boost = max(uEmph, step(1.5, aState));',
    // 2.8x: the glyph needs headroom for the star halo (1.4 * radius)
    '  float size = uRadius * 2.8 * uDpr * mix(1.0, 1.4, boost);',
    '  gl_PointSize = size;',
    '  vAA = 2.0 / max(size, 1.0);',
    '  int ci = int(aColor + 0.5);',
    '  vCore = uCore[ci]; vMid = uMid[ci]; vEdge = uEdge[ci];',
    '  vState = aState + uEmph * 2.0;',
    '}',
  ].join('\n');

  var POINT_FRAG = [
    'precision mediump float;',
    'varying vec3 vCore, vMid, vEdge;',
    'varying float vState, vAA;',
    'uniform float uStarness, uAlpha, uDark, uDim;',
    'void main() {',
    '  vec2 q = gl_PointCoord * 2.0 - 1.0;',
    '  float d = length(q);',
    '  float S = 0.714;',    // sphere edge; star halo reaches d = 1
    // sphere body — same look as the old makeSprite: radial gradient from an
    // upper-left light center, limb darkening, specular kiss, AA edge
    '  float t = clamp(d / S, 0.0, 1.0);',
    '  float g = clamp(length(q - vec2(-0.3, -0.36) * S) / S, 0.0, 1.0);',
    '  vec3 body = g < 0.5 ? mix(vCore, vMid, g * 2.0) : mix(vMid, vEdge, (g - 0.5) * 2.0);',
    '  body *= 1.0 - smoothstep(0.55, 1.0, t) * 0.55;',
    '  float sp = 1.0 - clamp(length(q - vec2(-0.38, -0.42) * S) / (0.5 * S), 0.0, 1.0);',
    '  body += vec3(1.0) * sp * sp * 0.5 * (1.0 - uStarness);',
    '  float sphereA = uAlpha * (1.0 - uStarness * 0.42) * (1.0 - smoothstep(S - vAA * S, S, d));',
    // starlight core — hot pinpoint with soft falloff to the halo edge
    '  vec3 starCol = mix(vMid, mix(vCore, vec3(1.0), 0.42), uDark);',
    '  float sf = d < 0.35 ? mix(1.0, 0.55, d / 0.35) : mix(0.55, 0.0, (d - 0.35) / 0.65);',
    '  vec3 sCol = d < 0.35 ? mix(starCol, vCore, d / 0.35) : vCore;',
    '  float starA = uAlpha * uStarness * 0.92 * clamp(sf, 0.0, 1.0);',
    // star over sphere (non-premultiplied out, matching the canvas blend)
    '  float a = starA + sphereA * (1.0 - starA);',
    '  vec3 rgb = a > 0.001 ? (sCol * starA + body * sphereA * (1.0 - starA)) / a : vec3(0.0);',
    '  float stateMul = vState > 2.5 ? 1.0 : (vState > 1.5 ? 1.0 : (vState > 0.5 ? uDim : 1.0));',
    '  gl_FragColor = vec4(rgb, a * stateMul);',
    '}',
  ].join('\n');

  // --- line shaders ---
  var LINE_VERT = [
    'attribute vec2 aPos;',
    'attribute float aColor;',   // palette index
    'attribute float aBucket;',  // distance bucket 0/1/2
    'uniform vec2 uRes;',
    'uniform float uScale;',
    'uniform vec2 uCenter;',
    'uniform vec3 uBucketAlpha;',
    'uniform vec3 uMid[' + PALETTE_SIZE + '];',
    'varying vec4 vColor;',
    'void main() {',
    '  vec2 px = uCenter + aPos * uScale;',
    '  vec2 clip = (px / uRes) * 2.0 - 1.0;',
    '  gl_Position = vec4(clip.x, -clip.y, 0.0, 1.0);',
    '  int ci = int(aColor + 0.5);',
    '  float a = aBucket < 0.5 ? uBucketAlpha.x : (aBucket < 1.5 ? uBucketAlpha.y : uBucketAlpha.z);',
    '  vColor = vec4(uMid[ci], a);',
    '}',
  ].join('\n');

  var LINE_FRAG = [
    'precision mediump float;',
    'varying vec4 vColor;',
    'uniform float uFade;',
    'void main() { gl_FragColor = vec4(vColor.rgb, vColor.a * uFade); }',
  ].join('\n');

  function compile(gl, type, src) {
    var sh = gl.createShader(type);
    gl.shaderSource(sh, src);
    gl.compileShader(sh);
    if (!gl.getShaderParameter(sh, gl.COMPILE_STATUS)) {
      throw new Error(gl.getShaderInfoLog(sh) || 'shader compile failed');
    }
    return sh;
  }

  function link(gl, vert, frag) {
    var prog = gl.createProgram();
    gl.attachShader(prog, compile(gl, gl.VERTEX_SHADER, vert));
    gl.attachShader(prog, compile(gl, gl.FRAGMENT_SHADER, frag));
    gl.linkProgram(prog);
    if (!gl.getProgramParameter(prog, gl.LINK_STATUS)) {
      throw new Error(gl.getProgramInfoLog(prog) || 'link failed');
    }
    return prog;
  }

  function uniforms(gl, prog, names) {
    var U = {};
    names.forEach(function(n) { U[n] = gl.getUniformLocation(prog, n); });
    return U;
  }

  window.AtlasGL = {
    // el: an existing <canvas> to render into (the middle DOM layer)
    create: function(el) {
      try {
        var canvas = el || document.createElement('canvas');
        var gl = canvas.getContext('webgl', { alpha: true, premultipliedAlpha: false, antialias: false })
          || canvas.getContext('experimental-webgl', { alpha: true, premultipliedAlpha: false, antialias: false });
        if (!gl) return null;

        // planet program
        var planetProg = link(gl, PLANET_VERT, PLANET_FRAG);
        var PU = uniforms(gl, planetProg, ['uRes', 'uCenter', 'uRadius', 'uMargin', 'uTex',
          'uRot', 'uTilt', 'uAlpha', 'uSeed', 'uTexSpan', 'uHover', 'uDark', 'uPx', 'uBase', 'uAccent', 'uLift']);
        var planetAPos = gl.getAttribLocation(planetProg, 'aPos');
        var quadBuf = gl.createBuffer();
        gl.bindBuffer(gl.ARRAY_BUFFER, quadBuf);
        gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([-1, -1, 1, -1, -1, 1, 1, 1]), gl.STATIC_DRAW);
        var MARGIN = 1.35;

        // point program
        var pointProg = link(gl, POINT_VERT, POINT_FRAG);
        var TU = uniforms(gl, pointProg, ['uRes', 'uScale', 'uCenter', 'uDpr', 'uRadius', 'uEmph',
          'uCore[0]', 'uMid[0]', 'uEdge[0]', 'uStarness', 'uAlpha', 'uDark', 'uDim']);
        var pA = {
          aPos: gl.getAttribLocation(pointProg, 'aPos'),
          aColor: gl.getAttribLocation(pointProg, 'aColor'),
          aState: gl.getAttribLocation(pointProg, 'aState'),
        };
        var pointPosBuf = gl.createBuffer();
        var pointColorBuf = gl.createBuffer();
        var pointStateBuf = gl.createBuffer();
        var pointCount = 0;
        var hasState = false;

        // line program
        var lineProg = link(gl, LINE_VERT, LINE_FRAG);
        var LU = uniforms(gl, lineProg, ['uRes', 'uScale', 'uCenter', 'uBucketAlpha', 'uMid[0]', 'uFade']);
        var lA = {
          aPos: gl.getAttribLocation(lineProg, 'aPos'),
          aColor: gl.getAttribLocation(lineProg, 'aColor'),
          aBucket: gl.getAttribLocation(lineProg, 'aBucket'),
        };
        var lineBuf = gl.createBuffer();
        var lineVerts = 0;

        // palette: flat Float32Arrays of PALETTE_SIZE rgb triples
        var palCore = new Float32Array(PALETTE_SIZE * 3);
        var palMid = new Float32Array(PALETTE_SIZE * 3);
        var palEdge = new Float32Array(PALETTE_SIZE * 3);

        // GL textures keyed by their source canvas — atlas.js rebuilds the
        // canvas object on theme/accent change, so stale entries just get
        // garbage-collected with the old canvas
        var texCache = new WeakMap();
        function getTex(cv) {
          var t = texCache.get(cv);
          if (t) return t;
          t = gl.createTexture();
          gl.bindTexture(gl.TEXTURE_2D, t);
          gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, cv);
          gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
          gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
          gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
          gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
          texCache.set(cv, t);
          return t;
        }

        function setBlend() {
          gl.enable(gl.BLEND);
          gl.blendFuncSeparate(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA, gl.ONE, gl.ONE_MINUS_SRC_ALPHA);
        }

        // GL validates EVERY enabled attribute array against the draw size,
        // not just the ones the bound program reads — so an attribute left
        // enabled by another pass (e.g. the 4-vertex planet quad during the
        // 83k-point draw) kills the call. Each pass starts from zero.
        var maxAttribs = gl.getParameter(gl.MAX_VERTEX_ATTRIBS);
        function disableAllAttribs() {
          for (var i = 0; i < maxAttribs; i++) gl.disableVertexAttribArray(i);
        }

        return {
          canvas: canvas,

          resize: function(W, H, dpr) {
            var bw = Math.round(W * dpr), bh = Math.round(H * dpr);
            if (canvas.width !== bw || canvas.height !== bh) {
              canvas.width = bw;
              canvas.height = bh;
            }
          },

          // one-time upload (positions and palette index never change)
          uploadPoints: function(n, xArr, yArr, colorIdxArr) {
            pointCount = n;
            var pos = new Float32Array(n * 2);
            for (var i = 0; i < n; i++) { pos[i * 2] = xArr[i]; pos[i * 2 + 1] = yArr[i]; }
            gl.bindBuffer(gl.ARRAY_BUFFER, pointPosBuf);
            gl.bufferData(gl.ARRAY_BUFFER, pos, gl.STATIC_DRAW);
            gl.bindBuffer(gl.ARRAY_BUFFER, pointColorBuf);
            gl.bufferData(gl.ARRAY_BUFFER, colorIdxArr, gl.STATIC_DRAW);
            // state buffer starts all-normal
            gl.bindBuffer(gl.ARRAY_BUFFER, pointStateBuf);
            gl.bufferData(gl.ARRAY_BUFFER, new Uint8Array(n), gl.DYNAMIC_DRAW);
            hasState = false;
          },

          // entries: array of {core:[r,g,b], mid:[...], edge:[...]} (0..1)
          setPalette: function(entries) {
            for (var i = 0; i < entries.length && i < PALETTE_SIZE; i++) {
              for (var k = 0; k < 3; k++) {
                palCore[i * 3 + k] = entries[i].core[k];
                palMid[i * 3 + k] = entries[i].mid[k];
                palEdge[i * 3 + k] = entries[i].edge[k];
              }
            }
          },

          // stateArr: Uint8Array (0 normal, 1 dim, 2 highlight) or null = all normal
          setPointState: function(stateArr) {
            gl.bindBuffer(gl.ARRAY_BUFFER, pointStateBuf);
            if (stateArr) {
              gl.bufferData(gl.ARRAY_BUFFER, stateArr, gl.DYNAMIC_DRAW);
              hasState = true;
            } else if (hasState) {
              gl.bufferData(gl.ARRAY_BUFFER, new Uint8Array(pointCount), gl.DYNAMIC_DRAW);
              hasState = false;
            }
          },

          // verts: Float32Array interleaved [x, y, colorIdx, bucket] per vertex
          uploadLines: function(verts, count) {
            gl.bindBuffer(gl.ARRAY_BUFFER, lineBuf);
            gl.bufferData(gl.ARRAY_BUFFER, verts, gl.STATIC_DRAW);
            lineVerts = count;
          },

          // one call per frame: clears, draws lines then points.
          // opts: {W, H, dpr, dark, scale, cx, cy, radius, starness, alpha,
          //        dim, lineFade, lineAlphas: [a,b,c], hoverIdx}
          frame: function(o) {
            this.resize(o.W, o.H, o.dpr);
            gl.viewport(0, 0, canvas.width, canvas.height);
            gl.clearColor(0, 0, 0, 0);
            gl.clear(gl.COLOR_BUFFER_BIT);
            setBlend();

            if (lineVerts > 0 && o.lineFade > 0.001) {
              disableAllAttribs();
              gl.useProgram(lineProg);
              gl.uniform2f(LU.uRes, o.W, o.H);
              gl.uniform1f(LU.uScale, o.scale);
              gl.uniform2f(LU.uCenter, o.cx, o.cy);
              gl.uniform3f(LU.uBucketAlpha, o.lineAlphas[0], o.lineAlphas[1], o.lineAlphas[2]);
              gl.uniform3fv(LU['uMid[0]'], palMid);
              gl.uniform1f(LU.uFade, o.lineFade);
              gl.bindBuffer(gl.ARRAY_BUFFER, lineBuf);
              gl.enableVertexAttribArray(lA.aPos);
              gl.vertexAttribPointer(lA.aPos, 2, gl.FLOAT, false, 16, 0);
              gl.enableVertexAttribArray(lA.aColor);
              gl.vertexAttribPointer(lA.aColor, 1, gl.FLOAT, false, 16, 8);
              gl.enableVertexAttribArray(lA.aBucket);
              gl.vertexAttribPointer(lA.aBucket, 1, gl.FLOAT, false, 16, 12);
              gl.lineWidth(1);
              gl.drawArrays(gl.LINES, 0, lineVerts);
            }

            if (pointCount > 0) {
              disableAllAttribs();
              gl.useProgram(pointProg);
              gl.uniform2f(TU.uRes, o.W, o.H);
              gl.uniform1f(TU.uScale, o.scale);
              gl.uniform2f(TU.uCenter, o.cx, o.cy);
              gl.uniform1f(TU.uDpr, o.dpr);
              gl.uniform1f(TU.uRadius, o.radius);
              gl.uniform1f(TU.uEmph, 0);
              gl.uniform1f(TU.uStarness, o.starness);
              gl.uniform1f(TU.uAlpha, o.alpha);
              gl.uniform1f(TU.uDark, o.dark ? 1 : 0);
              gl.uniform1f(TU.uDim, o.dim);
              gl.uniform3fv(TU['uCore[0]'], palCore);
              gl.uniform3fv(TU['uMid[0]'], palMid);
              gl.uniform3fv(TU['uEdge[0]'], palEdge);
              gl.bindBuffer(gl.ARRAY_BUFFER, pointPosBuf);
              gl.enableVertexAttribArray(pA.aPos);
              gl.vertexAttribPointer(pA.aPos, 2, gl.FLOAT, false, 0, 0);
              gl.bindBuffer(gl.ARRAY_BUFFER, pointColorBuf);
              gl.enableVertexAttribArray(pA.aColor);
              gl.vertexAttribPointer(pA.aColor, 1, gl.UNSIGNED_BYTE, false, 0, 0);
              gl.bindBuffer(gl.ARRAY_BUFFER, pointStateBuf);
              gl.enableVertexAttribArray(pA.aState);
              gl.vertexAttribPointer(pA.aState, 1, gl.UNSIGNED_BYTE, false, 0, 0);
              gl.drawArrays(gl.POINTS, 0, pointCount);
              // hovered point re-drawn emphasized on top
              if (o.hoverIdx >= 0 && o.hoverIdx < pointCount) {
                gl.uniform1f(TU.uEmph, 1);
                gl.drawArrays(gl.POINTS, o.hoverIdx, 1);
              }
            }
          },

          // planet pass: bind the planet program (no clear — planets draw
          // over the lines/points laid down by frame())
          beginPlanets: function(W, H, dpr, dark) {
            this.resize(W, H, dpr);
            gl.viewport(0, 0, canvas.width, canvas.height);
            disableAllAttribs();
            gl.useProgram(planetProg);
            setBlend();
            gl.bindBuffer(gl.ARRAY_BUFFER, quadBuf);
            gl.enableVertexAttribArray(planetAPos);
            gl.vertexAttribPointer(planetAPos, 2, gl.FLOAT, false, 0, 0);
            gl.activeTexture(gl.TEXTURE0);
            gl.uniform1i(PU.uTex, 0);
            gl.uniform2f(PU.uRes, W, H);
            gl.uniform1f(PU.uDark, dark ? 1 : 0);
            gl.uniform1f(PU.uMargin, MARGIN);
            gl.uniform1f(PU.uTilt, 0.35);
            gl.uniform1f(PU.uLift, 1.08);
          },
          // opts: {base:[r,g,b 0-1], accent:[r,g,b 0-1], seed, texSpan, hover, dpr}
          drawPlanet: function(texCanvas, sx, sy, R, alpha, rot, opts) {
            gl.bindTexture(gl.TEXTURE_2D, getTex(texCanvas));
            gl.uniform2f(PU.uCenter, sx, sy);
            gl.uniform1f(PU.uRadius, R);
            gl.uniform1f(PU.uRot, rot);
            gl.uniform1f(PU.uAlpha, alpha);
            gl.uniform1f(PU.uSeed, opts.seed || 0);
            gl.uniform1f(PU.uTexSpan, opts.texSpan || 0.8);
            gl.uniform1f(PU.uHover, opts.hover ? 1 : 0);
            gl.uniform1f(PU.uPx, 1 / Math.max(8, R * (opts.dpr || 1)));
            gl.uniform3f(PU.uBase, opts.base[0], opts.base[1], opts.base[2]);
            gl.uniform3f(PU.uAccent, opts.accent[0], opts.accent[1], opts.accent[2]);
            gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
          },
        };
      } catch (e) {
        return null;
      }
    },
  };
})();
