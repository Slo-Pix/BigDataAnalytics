// Project dashboard — vanilla JS, no external libraries (works offline).
const $ = (s, el = document) => el.querySelector(s);
const page = $("#page");
const nf = new Intl.NumberFormat("en-US");
const fmt = (n) => (n === null || n === undefined || n === "" ? "—" : typeof n === "number" ? nf.format(Math.round(n * 100) / 100) : n);
const compact = (n) => {
  if (n === null || n === undefined) return "—";
  const a = Math.abs(n);
  if (a >= 1e9) return (n / 1e9).toFixed(2) + " B";
  if (a >= 1e6) return (n / 1e6).toFixed(1) + " M";
  if (a >= 1e3) return (n / 1e3).toFixed(1) + " k";
  return nf.format(n);
};
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const ago = (iso) => {
  if (!iso) return "—";
  const s = Math.round((Date.now() - new Date(iso.replace(" ", "T")).getTime()) / 1000);
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.round(s / 60)} min ago`;
  if (s < 86400) return `${Math.round(s / 3600)} h ago`;
  return `${Math.round(s / 86400)} d ago`;
};
const hhmm = (iso) => new Date(iso.replace(" ", "T")).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
async function api(path, opts) {
  const r = await fetch(path, opts);
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(body.detail || r.statusText);
  return body;
}

// ---------- building blocks
const kpi = (title, value, sub, dark = false) =>
  `<div class="card kpi ${dark ? "dark" : ""}"><h3>${esc(title)}</h3><div class="v">${value}</div><div class="s">${sub}</div></div>`;
const chip = (cat) => (cat ? `<span class="chip ${esc(cat)}">${esc(cat)}</span>` : "");

function table(cols, rows, numeric = []) {
  if (!rows.length) return `<div class="empty">No rows yet</div>`;
  return `<table><thead><tr>${cols.map((c, i) => `<th class="${numeric.includes(i) ? "n" : ""}">${esc(c)}</th>`).join("")}</tr></thead><tbody>${rows
    .map((r) => `<tr>${r.map((v, i) => `<td class="${numeric.includes(i) ? "n" : ""}">${v}</td>`).join("")}</tr>`)
    .join("")}</tbody></table>`;
}

// Vertical bar chart (rounded bars, recessive grid, <title> tooltips). The last bar is highlighted.
function bars(data, { label, value, fmtLabel = (x) => x, height = 230 } = {}) {
  if (!data.length) return `<div class="empty">No data yet</div>`;
  const W = 760, H = height, pl = 46, pb = 26, pt = 10;
  const max = Math.max(...data.map(value)) || 1;
  const nice = Math.pow(10, Math.floor(Math.log10(max)));
  const top = Math.ceil(max / nice) * nice;
  const bw = Math.min((W - pl) / data.length, 70);
  const gap = Math.min(10, bw * 0.28);
  const ticks = [0, top / 2, top];
  const every = Math.max(1, Math.ceil(data.length / 8));
  let s = `<svg class="chart" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" role="img">`;
  ticks.forEach((t) => {
    const y = pt + (H - pb - pt) * (1 - t / top);
    s += `<line class="grid-line" x1="${pl}" x2="${W}" y1="${y}" y2="${y}"/><text x="${pl - 8}" y="${y + 4}" text-anchor="end">${compact(t)}</text>`;
  });
  data.forEach((d, i) => {
    const h = Math.max(2, (H - pb - pt) * (value(d) / top));
    const x = pl + i * bw + gap / 2;
    s += `<rect class="bar ${i === data.length - 1 ? "hi" : ""}" x="${x}" y="${H - pb - h}" width="${Math.max(2, bw - gap)}" height="${h}" rx="${Math.min(6, (bw - gap) / 2)}"><title>${esc(fmtLabel(label(d)))}: ${nf.format(value(d))}</title></rect>`;
    if (i % every === 0) s += `<text x="${x + (bw - gap) / 2}" y="${H - 8}" text-anchor="middle">${esc(fmtLabel(label(d)))}</text>`;
  });
  return s + `</svg>`;
}

function hbars(items, label, value) {
  const max = Math.max(...items.map(value), 1);
  return `<div class="bars-h">${items
    .map((d) => `<div class="row"><span class="mono">${esc(label(d))}</span><div class="track"><div class="fill" style="width:${(100 * value(d)) / max}%"></div></div><span class="n" style="text-align:right">${compact(value(d))}</span></div>`)
    .join("")}</div>`;
}

// ---------- pages
const pages = {};
let timer = null;
const every = (ms, fn) => { clearInterval(timer); fn(); timer = setInterval(fn, ms); };

pages.overview = {
  title: "Overview",
  async render() {
    page.innerHTML = `<div class="grid kpis" id="k"></div>
      <div class="grid two"><div class="card"><div class="card-head"><h2>Live collection, 10 Oct 2026 — rows streamed per 30 min</h2><a class="btn ghost" href="#stream">Open stream</a></div><div id="c"></div></div>
      <div class="card"><div class="card-head"><h2>Services</h2><a class="btn ghost" href="#tools">All tools</a></div><div id="svc"></div></div></div>
      <div class="grid half"><div class="card"><div class="card-head"><h2>Live demo stream</h2><span id="demoState" class="chip">—</span></div><div id="demo"></div></div>
      <div class="card"><div class="card-head"><h2>Containers</h2><span class="muted">docker compose</span></div><div id="ctr"></div></div></div>`;
    every(5000, async () => {
      const [o, s] = await Promise.all([api("/api/overview"), api("/api/services")]);
      $("#k").innerHTML =
        kpi("Data lake", compact(o.lake_rows), `rows in ${19} Parquet tables`, true) +
        kpi("Wikipedia pageviews", compact(o.wiki_rows), "rows, 1–7 Sep 2026 · 168 hourly files") +
        kpi("Streamed live via Kafka", compact(o.stream_rows), `${fmt(o.stream_batches)} micro-batches · <span class="up">0 lost</span>`) +
        kpi("Torrents alive", fmt(o.alive), "of 1,385,068 scanned on 10 Oct");
      $("#c").innerHTML = bars(o.collection_series, { label: (d) => d.t, value: (d) => d.rows, fmtLabel: hhmm });
      $("#svc").innerHTML = s.services.map((x) => `<div class="svc"><div class="meta"><span class="dot ${x.up ? "ok" : ""}"></span><div><b>${esc(x.name)}</b><small>${esc(x.role)} · :${x.port}</small></div></div>${x.url && x.up ? `<a class="btn ghost" href="${x.url}" target="_blank">Open</a>` : `<span class="chip ${x.up ? "ok" : "off"}">${x.up ? "up" : "down"}</span>`}</div>`).join("");
      const d = o.demo;
      $("#demoState").className = "chip " + (s.demo_stream_job ? "ok" : "");
      $("#demoState").textContent = s.demo_stream_job ? "streaming" : "stopped";
      $("#demo").innerHTML = `<div class="grid three" style="margin:0"><div><h3>Rows</h3><div class="kpi"><div class="v">${fmt(d.rows)}</div></div></div><div><h3>Micro-batches</h3><div class="kpi"><div class="v">${fmt(d.batches)}</div></div></div><div><h3>Last batch</h3><div class="kpi"><div class="v" style="font-size:22px">${ago(d.last)}</div></div></div></div><p class="muted">Scraper → Kafka topic <span class="mono">tracker.scrapes.demo</span> → Spark Structured Streaming (10 s) → PostgreSQL → here and Grafana.</p>`;
      $("#ctr").innerHTML = table(["Container", "Image", "Status"], s.containers.map((c) => [`<b>${esc(c.name)}</b>`, `<span class="mono">${esc(c.image)}</span>`, `<span class="chip ${c.up ? "ok" : ""}">${esc(c.status)}</span>`]));
    });
  },
};

let streamSchema = "demo";
pages.stream = {
  title: "Live stream",
  async render() {
    page.innerHTML = `<div class="card-head" style="margin-bottom:16px"><div class="seg" id="seg"><button data-s="demo">Demo (live)</button><button data-s="public">10 Oct collection</button></div>
      <div style="display:flex;gap:8px"><button class="btn" id="go">Start demo</button><a class="btn ghost" href="http://localhost:3000/d/live-demo" target="_blank">Grafana</a><a class="btn ghost" href="http://localhost:4040" target="_blank">Spark UI</a></div></div>
      <div class="grid kpis" id="k"></div>
      <div class="grid two"><div class="card"><div class="card-head"><h2 id="ct">Rows per minute</h2></div><div id="c"></div></div><div class="card"><div class="card-head"><h2>Rows by tracker</h2></div><div id="tr"></div></div></div>
      <div class="grid half"><div class="card"><div class="card-head"><h2>Latest micro-batches</h2><span class="muted">exactly-once ledger</span></div><div id="b"></div></div>
      <div class="card"><div class="card-head"><h2>Busiest swarms</h2></div><div class="scroll" id="t"></div></div></div>`;
    const seg = () => document.querySelectorAll("#seg button").forEach((b) => b.classList.toggle("on", b.dataset.s === streamSchema));
    document.querySelectorAll("#seg button").forEach((b) => (b.onclick = () => { streamSchema = b.dataset.s; seg(); pages.stream.refresh(); }));
    seg();
    $("#go").onclick = toggleDemo;
    pages.stream.refresh = async () => {
      const [x, s] = await Promise.all([api(`/api/stream?schema=${streamSchema}`), api("/api/services")]);
      $("#go").textContent = s.demo_stream_job ? "Stop demo" : "Start demo";
      $("#k").innerHTML =
        kpi(streamSchema === "demo" ? "Rows streamed (demo)" : "Rows streamed (10 Oct)", fmt(x.totals.rows), "Kafka → Spark → PostgreSQL", true) +
        kpi("Micro-batches", fmt(x.totals.batches), streamSchema === "demo" ? "one every 10 s" : "1-minute triggers") +
        kpi("Torrents tracked", fmt(x.torrents), "distinct infohashes") +
        kpi("Last batch", ago(x.totals.last), x.totals.last ? esc(x.totals.last) : "not started");
      $("#ct").textContent = streamSchema === "demo" ? "Rows per minute — last 20 min" : "Rows per minute — last 3 h of 10 Oct";
      $("#c").innerHTML = bars(x.per_minute, { label: (d) => d.t, value: (d) => d.rows, fmtLabel: hhmm });
      $("#tr").innerHTML = x.trackers.length ? hbars(x.trackers, (d) => d.tracker.split(":")[0], (d) => d.rows) : `<div class="empty">No rows yet</div>`;
      $("#b").innerHTML = table(["Batch id", "Rows", "Applied"], x.batches.map((r) => [fmt(r.batch_id), fmt(r.rows), `${esc(r.applied_at.slice(11, 19))} <span class="muted">${ago(r.applied_at)}</span>`]), [0, 1]);
      $("#t").innerHTML = table(["Title", "Type", "Seeders", "Leechers", "Downloads"], x.top.map((r) => [esc(r.title), chip(r.category), fmt(r.seeders), fmt(r.leechers), fmt(r.completed)]), [2, 3, 4]);
    };
    every(streamSchema === "demo" ? 5000 : 30000, pages.stream.refresh);
  },
};

let kafkaTopic = null;
pages.kafka = {
  title: "Kafka",
  async render() {
    page.innerHTML = `<div class="grid kpis" id="k"></div><div class="grid two"><div class="card"><div class="card-head"><h2>Latest messages</h2><select id="sel" class="btn ghost"></select></div><div class="scroll" id="m"></div></div>
      <div class="card"><div class="card-head"><h2>Topics &amp; partitions</h2><a class="btn" href="http://localhost:8080" target="_blank">Open Kafka UI</a></div><div id="tp"></div></div></div>`;
    every(5000, async () => {
      const k = await api(`/api/kafka${kafkaTopic ? `?topic=${encodeURIComponent(kafkaTopic)}` : ""}`);
      kafkaTopic = k.topic;
      const total = k.topics.reduce((a, t) => a + t.messages, 0);
      $("#k").innerHTML =
        kpi("Broker", "KRaft", `<span class="mono">${esc(k.bootstrap)}</span>`, true) +
        kpi("Topics", fmt(k.topics.length), k.topics.map((t) => esc(t.topic)).join(", ") || "none") +
        kpi("Messages retained", fmt(total), "7-day retention") +
        kpi("Partitions", fmt(k.topics.reduce((a, t) => a + t.partitions.length, 0)), "across all topics");
      const sel = $("#sel");
      sel.innerHTML = k.topics.map((t) => `<option ${t.topic === k.topic ? "selected" : ""}>${esc(t.topic)}</option>`).join("");
      sel.onchange = () => { kafkaTopic = sel.value; pages.kafka.render(); };
      $("#m").innerHTML = table(["Time", "P", "Offset", "Infohash", "Tracker", "Seeders", "Leechers", "Done"],
        k.messages.map((m) => { const v = m.value || {}; return [new Date(m.timestamp).toLocaleTimeString(), m.partition, fmt(m.offset), `<span class="mono">${esc(String(v.infohash || "").slice(0, 12))}…</span>`, esc(String(v.tracker || "").split(":")[0]), fmt(v.seeders), fmt(v.leechers), fmt(v.completed)]; }), [1, 2, 5, 6, 7]);
      $("#tp").innerHTML = k.topics.map((t) => `<div style="margin-bottom:16px"><div class="card-head" style="margin-bottom:6px"><b class="mono">${esc(t.topic)}</b><span class="chip">${fmt(t.messages)} msgs</span></div>${table(["Partition", "First offset", "End offset"], t.partitions.map((p) => [p.partition, fmt(p.begin), fmt(p.end)]), [0, 1, 2])}</div>`).join("") || `<div class="empty">No topics — start the demo</div>`;
    });
  },
};

pages.dataset = {
  title: "Dataset",
  async render() {
    page.innerHTML = `<div class="grid kpis" id="k"></div><div class="grid two"><div class="card"><div class="card-head"><h2>Tables</h2><span class="muted">click a row to preview</span></div><div id="tl"></div></div>
      <div class="card"><div class="card-head"><h2 id="sn">Schema</h2></div><div class="scroll" id="sc"><div class="empty">Pick a table</div></div></div></div>
      <div class="card"><div class="card-head"><h2 id="pn">Preview</h2><span class="muted">first 25 rows, read with pyarrow</span></div><div class="scroll" id="pv"><div class="empty">Pick a table</div></div></div>`;
    const t = await api("/api/tables");
    const rows = t.reduce((a, x) => a + x.rows, 0), size = t.reduce((a, x) => a + x.size_mb, 0);
    $("#k").innerHTML = kpi("Rows in the lake", compact(rows), fmt(rows) + " rows", true) + kpi("Tables", fmt(t.length), "staging · curated · stream") +
      kpi("On disk (Parquet)", (size / 1000).toFixed(1) + " GB", "zstd-compressed, columnar") + kpi("Largest", "wiki_pageviews", compact(t[0].rows) + " rows");
    $("#tl").innerHTML = `<table><thead><tr><th>Layer</th><th>Table</th><th class="n">Rows</th><th class="n">Files</th><th class="n">Size</th><th class="n">Cols</th></tr></thead><tbody>${t
      .map((x, i) => `<tr data-i="${i}" style="cursor:pointer"><td><span class="chip">${x.layer}</span></td><td><b>${esc(x.name)}</b></td><td class="n">${fmt(x.rows)}</td><td class="n">${fmt(x.files)}</td><td class="n">${x.size_mb >= 1000 ? (x.size_mb / 1000).toFixed(1) + " GB" : fmt(x.size_mb) + " MB"}</td><td class="n">${x.columns.length}</td></tr>`).join("")}</tbody></table>`;
    document.querySelectorAll("#tl tr[data-i]").forEach((tr) => (tr.onclick = async () => {
      const x = t[+tr.dataset.i];
      $("#sn").textContent = `Schema · ${x.name}`;
      $("#sc").innerHTML = table(["Column", "Type"], x.columns.map((c) => [`<b>${esc(c.name)}</b>`, `<span class="mono">${esc(c.type)}</span>`]));
      $("#pn").textContent = `Preview · ${x.layer}/${x.name}`;
      $("#pv").innerHTML = `<div class="empty">Loading…</div>`;
      try {
        const p = await api(`/api/tables/${x.layer}/${x.name}?n=25`);
        $("#pv").innerHTML = table(p.columns, p.rows.map((r) => r.map((v) => `<span class="mono">${esc(Array.isArray(v) ? JSON.stringify(v) : v)}</span>`)));
      } catch (e) { $("#pv").innerHTML = `<div class="err">${esc(e.message)}</div>`; }
    }));
    clearInterval(timer);
  },
};

pages.sql = {
  title: "Spark SQL",
  async render() {
    page.innerHTML = `<div class="grid two" style="grid-template-columns:minmax(0,1fr) minmax(0,2fr)"><div class="card"><div class="card-head"><h2>Prepared queries</h2></div><div class="qlist" id="q"></div></div>
      <div class="card"><div class="card-head"><h2>Query</h2><div style="display:flex;gap:8px;align-items:center"><span id="st" class="muted"></span><a class="btn ghost" href="http://localhost:4041" target="_blank">Spark UI</a><button class="btn" id="run">Run ▶</button></div></div>
      <textarea id="sqlbox" spellcheck="false">SELECT category, count(*) AS torrents FROM torrents GROUP BY category ORDER BY torrents DESC</textarea>
      <p class="muted">The first query starts Spark (≈ 15 s). Tables: wiki_pageviews, torrents, torrents_enriched, imdb_titles, steam_games, swarm, swarm_fullscan, demo_swarm (live) …</p>
      <div class="scroll" id="res" style="max-height:520px"></div></div></div>`;
    clearInterval(timer);
    const qs = await api("/api/queries");
    $("#q").innerHTML = qs.map((q, i) => `<button data-i="${i}">${esc(q.title)}</button>`).join("");
    document.querySelectorAll("#q button").forEach((b) => (b.onclick = () => { $("#sqlbox").value = qs[+b.dataset.i].sql; run(); }));
    const run = async () => {
      $("#run").disabled = true; $("#st").textContent = "running…"; $("#res").innerHTML = "";
      try {
        const r = await api("/api/sql", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ sql: $("#sqlbox").value }) });
        $("#st").innerHTML = `<b>${r.rows.length}</b> rows in <b>${r.seconds} s</b>${r.spark_startup_seconds > 1 ? ` (+${r.spark_startup_seconds} s Spark start)` : ""}`;
        $("#res").innerHTML = table(r.columns, r.rows.map((row) => row.map((v) => (typeof v === "number" ? fmt(v) : esc(Array.isArray(v) ? JSON.stringify(v) : v)))));
      } catch (e) { $("#st").innerHTML = `<span class="err">error</span>`; $("#res").innerHTML = `<div class="err">${esc(e.message)}</div>`; }
      $("#run").disabled = false;
    };
    $("#run").onclick = run;
    $("#sqlbox").onkeydown = (e) => { if ((e.ctrlKey || e.metaKey) && e.key === "Enter") run(); };
  },
};

const FINDINGS = [
  ["A1 · Activity index", "TV uploads 1k → 100k / year", "1080p went from 2% to 62% of movie uploads (2008 → 2022); x265 from 1% to 30% (2019 → 2022).", "a1_uploads_by_year.png"],
  ["A2 · Release-to-leak lag", "66–70% on RARBG the day after legal release", "Films 2020–23; 18–25% appeared before it. Median cinema → torrent 9 days (2020–21); new TV series 1 day.", "a2_leak_vs_home_ecdf.png"],
  ["A3 · Popularity decay", "Films fade as seeders ∝ age^−0.73", "MLlib Poisson GLM, power law beats exponential by AIC. TV series and games do not decay.", "a3_decay.png"],
  ["A4 · Interest vs piracy", "Spearman ρ 0.47 · 0.41 · 0.24", "English Wikipedia views vs current seeders for movies, TV and games (53,596 titles).", "a4_wiki_vs_seeders.png"],
  ["A5 · Games", "Free 5% pirated · $5–40 ~53% · >$40 38%", "MLlib logistic regression, test AUC 0.83: popularity and single-player raise piracy; free-to-play and Early Access lower it.", "a5_pirated_by_band.png"],
  ["A6 · Genres", "Sci-Fi +59% · Horror +56% · Romance −18%", "Torrent versions vs an equally popular film of the same year (MLlib OLS residuals, 8,024 films).", "a6_genre_excess_piracy.png"],
  ["A7 · Swarms through the day", "Film / TV within ±1.5% · games +11%", "Fixed panel of 110,705 torrents on opentrackr, 12:40–20:50 IST, 10 Oct 2026. Leechers per seeder: TV 0.42, movie 0.35, game 0.24.", "a7_swarm_day.png"],
];
pages.findings = {
  title: "Findings",
  async render() {
    clearInterval(timer);
    page.innerHTML = `<div class="grid half">${FINDINGS.map(([t, big, txt, img]) => `<div class="card finding"><h3>${esc(t)}</h3><div class="big">${esc(big)}</div><p>${esc(txt)}</p><img src="/figures/${img}" alt="${esc(t)} chart" loading="lazy"></div>`).join("")}
      <div class="card"><div class="card-head"><h2>Validation</h2><a class="btn" href="/report.pdf" target="_blank">Report PDF</a></div><div class="scroll" id="v"></div></div></div>`;
    document.querySelectorAll(".finding img").forEach((im) => (im.onclick = () => { $("#lightbox img").src = im.src; $("#lightbox").hidden = false; }));
    const v = await api("/api/validation");
    const pass = v.filter((r) => r.status === "PASS").length;
    $("#v").innerHTML = `<p><b class="up">${pass} / ${v.length} checks pass</b> <span class="muted">· uv run python -m analysis.validate</span></p>` +
      table(["Check", "Expected", "Observed", ""], v.map((r) => [esc(r.check), `<span class="mono">${esc(r.expected)}</span>`, `<span class="mono">${esc(r.observed)}</span>`, `<span class="chip ${r.status === "PASS" ? "ok" : "off"}">${esc(r.status)}</span>`]));
  },
};

const TOOLS = [
  ["K", "Apache Kafka 3.9", "Ingestion buffer between the scrapers and Spark. KRaft mode, topic tracker.scrapes (6 partitions, 7-day retention).", [["Kafka page", "#kafka"], ["Kafka UI", "http://localhost:8080"]], 9092],
  ["UI", "Kafka UI", "Web console for the broker: topics, partitions, consumer offsets, browse messages.", [["Open", "http://localhost:8080"]], 8080],
  ["S", "Spark Structured Streaming", "Reads Kafka in micro-batches, appends Parquet, upserts PostgreSQL exactly-once.", [["Spark UI", "http://localhost:4040"], ["Live stream", "#stream"]], 4040],
  ["SQL", "Spark SQL", "All ETL joins and the seven analyses; ad-hoc queries from this portal.", [["Query now", "#sql"], ["Spark UI", "http://localhost:4041"]], 4041],
  ["ML", "Spark MLlib", "Poisson GLM (decay), logistic regression + random forest (games), OLS (genres), Spearman (interest).", [["Findings", "#findings"]], null],
  ["P", "Apache Parquet data lake", "1.38 B rows, zstd, partitioned: raw → staging → curated, plus the stream output.", [["Dataset", "#dataset"]], null],
  ["PG", "PostgreSQL 16", "Serving layer: per-minute stream aggregates, latest swarm per torrent, batch results.", [["Explore in Grafana", "http://localhost:3000/explore"]], 5432],
  ["G", "Grafana 13", "Three provisioned dashboards over PostgreSQL.", [["Live demo", "http://localhost:3000/d/live-demo"], ["Live swarms", "http://localhost:3000/d/live-swarm"], ["Batch analytics", "http://localhost:3000/d/batch-analytics"]], 3000],
  ["D", "Docker Compose", "One command starts Kafka, Kafka UI, PostgreSQL, Grafana and the scraper workers.", [["Containers", "#overview"]], null],
  ["BT", "BEP 15 tracker scraper", "Our own UDP scrape client: 5 public trackers, 74 infohashes per packet, per-tracker loops.", [["Live stream", "#stream"]], null],
  ["PDF", "Report & slides", "19-page report with validation; 16-slide deck with speaker notes.", [["Report", "/report.pdf"], ["Slides", "https://claude.ai/artifact/7xzUmdV3cTZNCTbcB7qnU9"]], null],
  ["✓", "Validation suite", "26 automated checks: reconciliation, stream integrity, tracker agreement, spot checks.", [["Results", "#findings"]], null],
];
pages.tools = {
  title: "Tools",
  async render() {
    clearInterval(timer);
    const s = await api("/api/services");
    const up = Object.fromEntries(s.services.map((x) => [x.port, x.up]));
    page.innerHTML = `<div class="grid three">${TOOLS.map(([ic, name, role, links, port]) => `<div class="card tool"><div class="card-head" style="margin:0"><span class="icon">${esc(ic)}</span>${port ? `<span class="chip ${up[port] ? "ok" : "off"}">${up[port] ? "running" : "not running"} · :${port}</span>` : ""}</div><h2>${esc(name)}</h2><p>${esc(role)}</p><div style="display:flex;gap:8px;flex-wrap:wrap">${links.map(([l, u]) => `<a class="btn ghost" href="${u}" ${u.startsWith("http") || u.startsWith("/report") ? 'target="_blank"' : ""}>${esc(l)}</a>`).join("")}</div></div>`).join("")}</div>`;
  },
};

// ---------- demo control, header, router
async function toggleDemo() {
  const s = await api("/api/services");
  const action = s.demo_stream_job ? "stop" : "start";
  for (const b of [$("#demoBtn"), $("#go")]) if (b) { b.disabled = true; b.textContent = action === "start" ? "Starting…" : "Stopping…"; }
  try { await api(`/api/demo/${action}`, { method: "POST" }); } catch (e) { alert(e.message); }
  setTimeout(header, action === "start" ? 20000 : 4000);
}
$("#demoBtn").onclick = toggleDemo;

async function header() {
  try {
    const [s, o] = await Promise.all([api("/api/services"), api("/api/overview")]);
    const fresh = s.demo_stream_job && o.demo.last && Date.now() - new Date(o.demo.last.replace(" ", "T")).getTime() < 60000;
    const n = s.services.filter((x) => x.up).length;
    $("#livePill").className = "pill" + (fresh ? " live" : "");
    $("#livePill span").textContent = fresh ? `Demo streaming · ${fmt(o.demo.rows)} rows` : s.demo_stream_job ? "Demo starting…" : `${n} / ${s.services.length} services up`;
    const b = $("#demoBtn");
    b.disabled = false;
    b.textContent = s.demo_stream_job ? "Stop demo" : "Start demo";
    $("#demoText").textContent = s.demo_stream_job ? `Streaming now: ${fmt(o.demo.batches)} micro-batches so far.` : "Stream the 2,000 busiest torrents through Kafka and Spark, right now.";
  } catch { $("#livePill span").textContent = "backend unreachable"; }
}
setInterval(header, 5000);
setInterval(() => ($("#clock").textContent = new Date().toLocaleTimeString()), 1000);

function route() {
  const name = (location.hash || "#overview").slice(1);
  const p = pages[name] || pages.overview;
  document.querySelectorAll("#nav a").forEach((a) => a.classList.toggle("on", a.dataset.page === (pages[name] ? name : "overview")));
  $("#title").textContent = p.title;
  p.render().catch((e) => (page.innerHTML = `<div class="card err">${esc(e.message)}</div>`));
}
$("#lightbox").onclick = () => ($("#lightbox").hidden = true);
window.addEventListener("hashchange", route);
route();
header();
