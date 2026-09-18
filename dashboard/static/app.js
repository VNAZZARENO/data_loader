// Tous les appels passent par BASE (prefixe du proxy pergam-tools, vide en direct).
const api = async (method, path, body) => {
  const r = await fetch(`${BASE}/api${path}`, {method, headers: body ? {"Content-Type": "application/json"} : {},
                                               body: body ? JSON.stringify(body) : undefined});
  const text = await r.text();
  let data; try { data = text ? JSON.parse(text) : null; } catch { data = {detail: text.slice(0, 200)}; }
  if (!r.ok) { const e = new Error((data && data.detail) || `HTTP ${r.status}`); e.status = r.status; throw e; }
  return data;
};
const $ = (s, el = document) => el.querySelector(s);
const $$ = (s, el = document) => [...el.querySelectorAll(s)];
const esc = s => String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const toast = (msg, err) => { const t = $("#toast"); t.textContent = msg; t.className = err ? "err" : ""; t.style.display = "block";
  clearTimeout(toast.t); toast.t = setTimeout(() => t.style.display = "none", err ? 7000 : 3000); };
const fail = e => toast(e.status === 409 ? `Conflit : ${e.message}. La page a ete rechargee.` : e.message, true);
const badge = (cls, label) => `<span class="badge ${cls}"><i class="dot"></i>${esc(label)}</span>`;
const fmtDate = s => s ? String(s).slice(0, 10) : "—";
const ageBadge = (bd, warn, crit) => bd == null ? badge("off", "aucune donnee")
  : badge(bd > crit ? "crit" : bd > warn ? "warn" : "good", bd === 0 ? "a jour" : `${bd} j ouvres`);
const runBadge = r => !r ? badge("off", "jamais") : badge({ok: "good", partial: "warn", failed: "crit"}[r.status] || "off",
  {ok: "OK", partial: "partiel", failed: "echec", running: "en cours"}[r.status] || r.status);

function sortable(table) {
  $$("th[data-sort]", table).forEach((th, i) => th.onclick = () => {
    const rows = $$("tbody tr", table), dir = th.dataset.dir = th.dataset.dir === "asc" ? "desc" : "asc";
    const idx = [...th.parentNode.children].indexOf(th);
    rows.sort((a, b) => { const x = a.children[idx].dataset.v ?? a.children[idx].textContent, y = b.children[idx].dataset.v ?? b.children[idx].textContent;
      const nx = parseFloat(x), ny = parseFloat(y), num = !isNaN(nx) && !isNaN(ny);
      return (num ? nx - ny : String(x).localeCompare(String(y))) * (dir === "asc" ? 1 : -1); });
    rows.forEach(r => $("tbody", table).appendChild(r));
  });
}

const PLOT = {surface: "#fcfcfb", grid: "#e1e0d9", axis: "#c3c2b7", muted: "#898781", ink2: "#52514e", s1: "#2a78d6", s2: "#eb6834"};
function lineChart(el, traces, yTitle) {
  const layout = {paper_bgcolor: PLOT.surface, plot_bgcolor: PLOT.surface, margin: {l: 52, r: 16, t: 8, b: 32}, showlegend: false,
    hovermode: "x unified", font: {family: "system-ui, sans-serif", size: 12, color: PLOT.ink2},
    xaxis: {gridcolor: PLOT.grid, linecolor: PLOT.axis, tickcolor: PLOT.axis, showspikes: true, spikemode: "across", spikethickness: 1, spikecolor: PLOT.axis, spikedash: "solid"},
    yaxis: {gridcolor: PLOT.grid, zeroline: false, title: {text: yTitle, font: {size: 11, color: PLOT.muted}}}};
  Plotly.newPlot(el, traces.map(t => ({type: "scatter", mode: "lines", line: {width: 2, color: t.color, shape: t.step ? "hv" : "linear"},
    x: t.x, y: t.y, name: t.name, hovertemplate: `%{y:,.${t.digits ?? 1}f}<extra>${t.name}</extra>`})), layout, {displayModeBar: false, responsive: true});
}
