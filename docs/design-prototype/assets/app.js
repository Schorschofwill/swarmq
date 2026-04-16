// SwarmQ Docs — Prototype interactivity

// Theme toggle with localStorage
(function () {
  const root = document.documentElement;
  const stored = localStorage.getItem('swarmq-theme');
  if (stored) root.setAttribute('data-theme', stored);

  document.addEventListener('click', (e) => {
    const btn = e.target.closest('.theme-toggle');
    if (!btn) return;
    const current = root.getAttribute('data-theme') || 'light';
    const next = current === 'light' ? 'dark' : 'light';
    root.setAttribute('data-theme', next);
    localStorage.setItem('swarmq-theme', next);
  });
})();

// Copy buttons
document.addEventListener('click', async (e) => {
  const btn = e.target.closest('.copy-btn');
  if (!btn) return;
  const block = btn.closest('.codeblock');
  const code = block?.querySelector('pre')?.innerText || '';
  try {
    await navigator.clipboard.writeText(code);
    btn.classList.add('copied');
    const label = btn.querySelector('.copy-label');
    const orig = label?.textContent;
    if (label) label.textContent = 'Copied';
    setTimeout(() => {
      btn.classList.remove('copied');
      if (label && orig) label.textContent = orig;
    }, 1400);
  } catch (_) {}
});

// Language tabs (visual only for prototype)
document.addEventListener('click', (e) => {
  const tab = e.target.closest('.codeblock-tabs button');
  if (!tab) return;
  tab.parentElement.querySelectorAll('button').forEach(b => b.classList.remove('active'));
  tab.classList.add('active');
});

// TOC active-section highlighting
(function () {
  const links = document.querySelectorAll('.toc a');
  if (!links.length) return;
  const targets = [...links].map(a => document.querySelector(a.getAttribute('href'))).filter(Boolean);
  if (!targets.length) return;

  const map = new Map();
  targets.forEach((t, i) => map.set(t, links[i]));

  const obs = new IntersectionObserver((entries) => {
    entries.forEach(ent => {
      const link = map.get(ent.target);
      if (!link) return;
      if (ent.isIntersecting) {
        links.forEach(l => l.classList.remove('active'));
        link.classList.add('active');
      }
    });
  }, { rootMargin: '-80px 0px -70% 0px', threshold: 0 });

  targets.forEach(t => obs.observe(t));
})();
