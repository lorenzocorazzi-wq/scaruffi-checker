// particles.js — Particle Physics System + Rating Scale Visualization
// Standalone module, no imports required.

// ─────────────────────────────────────────────────────────────────────────────
// SECTION 1 — PARTICLE PHYSICS SYSTEM
// ─────────────────────────────────────────────────────────────────────────────

(function initParticleSystem() {
  const PARTICLE_COUNT = 70;
  const CONNECTION_DISTANCE = 130;
  const REPULSION_STRENGTH = 4000;   // Coulomb-like constant
  const GRAVITY_DAMPING = 0.985;     // energy conservation factor (< 1)
  const SPEED_LIMIT = 3.5;
  const COLORS = ['#f59e0b', '#3b82f6'];

  let canvas, ctx, particles, mouse, animFrameId;

  function createParticle(width, height) {
    const color = COLORS[Math.random() < 0.5 ? 0 : 1];
    return {
      x: Math.random() * width,
      y: Math.random() * height,
      vx: (Math.random() - 0.5) * 1.2,
      vy: (Math.random() - 0.5) * 1.2,
      mass: 0.8 + Math.random() * 1.4,   // affects repulsion response
      radius: 1.8 + Math.random() * 2.2,
      color: color,
      opacity: 0.15 + Math.random() * 0.25,
    };
  }

  function initParticles() {
    particles = [];
    for (let i = 0; i < PARTICLE_COUNT; i++) {
      particles.push(createParticle(canvas.width, canvas.height));
    }
  }

  function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
  }

  function updateParticle(p) {
    // Mouse repulsion — inverse-square law (Coulomb-like)
    if (mouse.x !== null) {
      const dx = p.x - mouse.x;
      const dy = p.y - mouse.y;
      const distSq = dx * dx + dy * dy;
      const minDistSq = 1;  // avoid singularity
      const safeDist = Math.max(distSq, minDistSq);
      const forceMag = REPULSION_STRENGTH / (safeDist * p.mass);
      const dist = Math.sqrt(safeDist);
      p.vx += (dx / dist) * forceMag;
      p.vy += (dy / dist) * forceMag;
    }

    // Energy conservation via velocity dampening
    p.vx *= GRAVITY_DAMPING;
    p.vy *= GRAVITY_DAMPING;

    // Speed cap
    const speed = Math.sqrt(p.vx * p.vx + p.vy * p.vy);
    if (speed > SPEED_LIMIT) {
      p.vx = (p.vx / speed) * SPEED_LIMIT;
      p.vy = (p.vy / speed) * SPEED_LIMIT;
    }

    // Integrate position
    p.x += p.vx;
    p.y += p.vy;

    // Soft boundary reflection
    if (p.x < 0) { p.x = 0; p.vx = Math.abs(p.vx); }
    if (p.x > canvas.width)  { p.x = canvas.width;  p.vx = -Math.abs(p.vx); }
    if (p.y < 0) { p.y = 0; p.vy = Math.abs(p.vy); }
    if (p.y > canvas.height) { p.y = canvas.height; p.vy = -Math.abs(p.vy); }
  }

  function hexToRgb(hex) {
    const r = parseInt(hex.slice(1, 3), 16);
    const g = parseInt(hex.slice(3, 5), 16);
    const b = parseInt(hex.slice(5, 7), 16);
    return `${r},${g},${b}`;
  }

  // Pre-compute RGB strings for the two palette colors
  const COLOR_RGB = {};
  COLORS.forEach(c => { COLOR_RGB[c] = hexToRgb(c); });

  function drawConnections() {
    for (let i = 0; i < particles.length; i++) {
      for (let j = i + 1; j < particles.length; j++) {
        const a = particles[i];
        const b = particles[j];
        const dx = a.x - b.x;
        const dy = a.y - b.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < CONNECTION_DISTANCE) {
          // Opacity proportional to proximity: max at dist=0, 0 at CONNECTION_DISTANCE
          const lineOpacity = (1 - dist / CONNECTION_DISTANCE) * 0.18;
          // Blend color: use the "closer" particle's color, or average via mid-color
          const rgb = COLOR_RGB[a.color];
          ctx.beginPath();
          ctx.strokeStyle = `rgba(${rgb},${lineOpacity})`;
          ctx.lineWidth = 0.7;
          ctx.moveTo(a.x, a.y);
          ctx.lineTo(b.x, b.y);
          ctx.stroke();
        }
      }
    }
  }

  function drawParticles() {
    particles.forEach(p => {
      ctx.beginPath();
      ctx.arc(p.x, p.y, p.radius, 0, Math.PI * 2);
      ctx.fillStyle = `rgba(${COLOR_RGB[p.color]},${p.opacity})`;
      ctx.fill();
    });
  }

  function loop() {
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    particles.forEach(updateParticle);
    drawConnections();
    drawParticles();
    animFrameId = requestAnimationFrame(loop);
  }

  function resizeCanvas() {
    canvas.width  = window.innerWidth;
    canvas.height = window.innerHeight;
  }

  function setup() {
    canvas = document.createElement('canvas');
    canvas.style.cssText = [
      'position:fixed',
      'top:0',
      'left:0',
      'width:100%',
      'height:100%',
      'z-index:-1',
      'pointer-events:none',
    ].join(';');
    document.body.appendChild(canvas);
    ctx = canvas.getContext('2d');

    mouse = { x: null, y: null };

    resizeCanvas();
    initParticles();

    window.addEventListener('resize', () => {
      resizeCanvas();
    });

    window.addEventListener('mousemove', e => {
      mouse.x = e.clientX;
      mouse.y = e.clientY;
    });

    window.addEventListener('mouseleave', () => {
      mouse.x = null;
      mouse.y = null;
    });

    loop();
  }

  document.addEventListener('DOMContentLoaded', setup);
})();


// ─────────────────────────────────────────────────────────────────────────────
// SECTION 2 — RATING SCALE VISUALIZATION
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Renders an SVG rating scale (0–10) with coloured zones and a
 * physics-based spring-animated needle pointer.
 *
 * @param {string} containerId  - ID of the host element
 * @param {number} rating       - Album rating value in [0, 10]
 */
function renderRatingScale(containerId, rating) {
  const container = document.getElementById(containerId);
  if (!container) { return; }

  // ── Layout constants ─────────────────────────────────────────────────────
  const W = container.clientWidth || 400;
  const H = 80;
  const BAR_Y = 28;       // top of the coloured bar
  const BAR_H = 14;       // height of the coloured bar
  const PAD_L = 18;       // left padding (room for the "0" label)
  const PAD_R = 18;       // right padding
  const BAR_W = W - PAD_L - PAD_R;

  // Scale breakpoints (rating → x position)
  const TICKS = [0, 6, 7, 10];

  function ratingToX(r) {
    return PAD_L + (r / 10) * BAR_W;
  }

  // Zone definitions [start, end, fill, label]
  const ZONES = [
    { from: 0, to: 6,  fill: '#ef4444', label: 'escluso',         opacity: 0.82 },
    { from: 6, to: 7,  fill: '#9ca3af', label: 'top 2 singoli',   opacity: 0.82 },
    { from: 7, to: 10, fill: '#f59e0b', label: 'tutte le tracce', opacity: 0.82 },
  ];

  // ── Build SVG markup ─────────────────────────────────────────────────────
  const svgNS = 'http://www.w3.org/2000/svg';

  // Remove previous SVG if re-rendering
  const existing = container.querySelector('svg.rating-scale-svg');
  if (existing) { existing.remove(); }

  const svg = document.createElementNS(svgNS, 'svg');
  svg.setAttribute('class', 'rating-scale-svg');
  svg.setAttribute('width', W);
  svg.setAttribute('height', H);
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
  svg.style.display = 'block';
  svg.style.fontFamily = "'Space Mono', monospace, monospace";

  // ── Draw zone rectangles ─────────────────────────────────────────────────
  ZONES.forEach(z => {
    const x1 = ratingToX(z.from);
    const x2 = ratingToX(z.to);
    const rect = document.createElementNS(svgNS, 'rect');
    rect.setAttribute('x', x1);
    rect.setAttribute('y', BAR_Y);
    rect.setAttribute('width', x2 - x1);
    rect.setAttribute('height', BAR_H);
    rect.setAttribute('fill', z.fill);
    rect.setAttribute('opacity', z.opacity);
    svg.appendChild(rect);

    // Zone label — centred, small, inside bar
    const labelX = (x1 + x2) / 2;
    const text = document.createElementNS(svgNS, 'text');
    text.setAttribute('x', labelX);
    text.setAttribute('y', BAR_Y + BAR_H / 2 + 1);
    text.setAttribute('text-anchor', 'middle');
    text.setAttribute('dominant-baseline', 'middle');
    text.setAttribute('font-size', '7');
    text.setAttribute('fill', '#fff');
    text.setAttribute('font-family', "'Space Mono', monospace");
    text.setAttribute('font-weight', '700');
    text.setAttribute('letter-spacing', '0.5');
    text.textContent = z.label;
    svg.appendChild(text);
  });

  // ── Tick marks and numeric labels ────────────────────────────────────────
  TICKS.forEach(t => {
    const x = ratingToX(t);

    // Short tick line
    const tick = document.createElementNS(svgNS, 'line');
    tick.setAttribute('x1', x);
    tick.setAttribute('x2', x);
    tick.setAttribute('y1', BAR_Y + BAR_H);
    tick.setAttribute('y2', BAR_Y + BAR_H + 4);
    tick.setAttribute('stroke', '#6b7280');
    tick.setAttribute('stroke-width', '1');
    svg.appendChild(tick);

    // Numeric label below tick
    const label = document.createElementNS(svgNS, 'text');
    label.setAttribute('x', x);
    label.setAttribute('y', BAR_Y + BAR_H + 13);
    label.setAttribute('text-anchor', 'middle');
    label.setAttribute('dominant-baseline', 'auto');
    label.setAttribute('font-size', '9');
    label.setAttribute('fill', '#9ca3af');
    label.setAttribute('font-family', "'Space Mono', monospace");
    label.textContent = t;
    svg.appendChild(label);
  });

  // ── Needle group (animated) ──────────────────────────────────────────────
  // Needle anatomy: a thin vertical line + diamond head at the top
  const needleGroup = document.createElementNS(svgNS, 'g');
  needleGroup.setAttribute('id', containerId + '-needle');

  const NEEDLE_TOP = BAR_Y - 12;
  const NEEDLE_BOT = BAR_Y;
  const DIAMOND_SIZE = 5;

  // Vertical stem
  const stem = document.createElementNS(svgNS, 'line');
  stem.setAttribute('x1', 0);
  stem.setAttribute('x2', 0);
  stem.setAttribute('y1', NEEDLE_TOP + DIAMOND_SIZE);
  stem.setAttribute('y2', NEEDLE_BOT);
  stem.setAttribute('stroke', '#ffffff');
  stem.setAttribute('stroke-width', '1.5');
  needleGroup.appendChild(stem);

  // Diamond head
  const diamond = document.createElementNS(svgNS, 'polygon');
  diamond.setAttribute('points',
    `0,${NEEDLE_TOP}  ${DIAMOND_SIZE},${NEEDLE_TOP + DIAMOND_SIZE}  0,${NEEDLE_TOP + DIAMOND_SIZE * 2}  ${-DIAMOND_SIZE},${NEEDLE_TOP + DIAMOND_SIZE}`
  );
  diamond.setAttribute('fill', '#ffffff');
  diamond.setAttribute('opacity', '0.95');
  needleGroup.appendChild(diamond);

  // Rating value text below bar
  const valueText = document.createElementNS(svgNS, 'text');
  valueText.setAttribute('x', 0);
  valueText.setAttribute('y', BAR_Y + BAR_H + 28);
  valueText.setAttribute('text-anchor', 'middle');
  valueText.setAttribute('dominant-baseline', 'auto');
  valueText.setAttribute('font-size', '11');
  valueText.setAttribute('fill', '#f3f4f6');
  valueText.setAttribute('font-family', "'Space Mono', monospace");
  valueText.setAttribute('font-weight', '700');
  valueText.textContent = '';
  needleGroup.appendChild(valueText);

  svg.appendChild(needleGroup);
  container.appendChild(svg);

  // ── Spring animation ─────────────────────────────────────────────────────
  // Physics: damped harmonic oscillator (spring)
  //   acceleration = -stiffness * displacement - damping * velocity
  const TARGET_X = ratingToX(Math.max(0, Math.min(10, rating)));
  const START_X  = ratingToX(0);   // always animate from 0

  const SPRING_K = 220;   // stiffness (rad²/s²)
  const DAMPING  = 18;    // damping coefficient (1/s)

  let pos = START_X;
  let vel = 0;
  let lastTime = null;
  let animId = null;

  function springStep(now) {
    if (lastTime === null) { lastTime = now; }
    const dt = Math.min((now - lastTime) / 1000, 0.05);  // cap at 50ms
    lastTime = now;

    const displacement = pos - TARGET_X;
    const acc = -SPRING_K * displacement - DAMPING * vel;
    vel += acc * dt;
    pos += vel * dt;

    // Settle check
    const settled = Math.abs(displacement) < 0.05 && Math.abs(vel) < 0.05;
    if (settled) {
      pos = TARGET_X;
      vel = 0;
    }

    needleGroup.setAttribute('transform', `translate(${pos}, 0)`);
    valueText.textContent = rating.toFixed(1);

    if (!settled) {
      animId = requestAnimationFrame(springStep);
    }
  }

  // Start animation on next frame (gives browser time to paint first)
  requestAnimationFrame(springStep);
}

// ── Expose globals ────────────────────────────────────────────────────────────
window.renderRatingScale = renderRatingScale;
