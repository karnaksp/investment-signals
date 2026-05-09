(function () {
  "use strict";

  const TOKEN_KEY = "tinvest_admin_token";
  const state = {
    minutes: 0,
    minq: "",
    signalType: "",
    offset: 0,
    limit: 40,
  };
  const charts = {};

  function getToken() {
    return (localStorage.getItem(TOKEN_KEY) || "").trim();
  }
  function setToken(v) {
    localStorage.setItem(TOKEN_KEY, (v || "").trim());
  }

  function parseHash() {
    const raw = (location.hash || "#/overview").replace(/^#/, "");
    const [pathPart, queryPart] = raw.split("?");
    const name = (pathPart || "/overview").replace(/^\//, "") || "overview";
    const q = new URLSearchParams(queryPart || "");
    return { name, q };
  }

  function readUrlState() {
    const u = new URL(location.href);
    if (u.searchParams.has("minutes"))
      state.minutes = parseInt(u.searchParams.get("minutes"), 10) || 0;
    if (u.searchParams.has("minq")) state.minq = u.searchParams.get("minq") || "";
    if (u.searchParams.has("type")) state.signalType = u.searchParams.get("type") || "";
    if (u.searchParams.has("offset"))
      state.offset = parseInt(u.searchParams.get("offset"), 10) || 0;
  }

  function writeUrlState() {
    const u = new URL(location.href);
    u.searchParams.set("minutes", String(state.minutes));
    if (state.minq) u.searchParams.set("minq", state.minq);
    else u.searchParams.delete("minq");
    if (state.signalType) u.searchParams.set("type", state.signalType);
    else u.searchParams.delete("type");
    if (state.offset) u.searchParams.set("offset", String(state.offset));
    else u.searchParams.delete("offset");
    history.replaceState(null, "", u.pathname + u.search + location.hash);
  }

  async function api(path, opts) {
    const tok = getToken();
    if (!tok) throw new Error("Нет токена: введите ADMIN_API_TOKEN в шапке и «Сохранить».");
    const url = path.startsWith("http") ? path : new URL(path, location.origin).toString();
    const headers = Object.assign(
      { "X-Admin-Token": tok, Accept: "application/json" },
      (opts && opts.headers) || {}
    );
    const r = await fetch(url, Object.assign({}, opts, { headers }));
    if (!r.ok) {
      let msg = r.statusText;
      try {
        const j = await r.json();
        msg = j.detail || msg;
      } catch (e) {}
      throw new Error(msg);
    }
    if (opts && opts.parse === "blob") return r.blob();
    const ct = r.headers.get("content-type") || "";
    if (ct.includes("application/json")) return r.json();
    return r.text();
  }

  function destroyCharts() {
    Object.keys(charts).forEach((k) => {
      if (charts[k]) {
        charts[k].destroy();
        charts[k] = null;
      }
    });
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/"/g, "&quot;");
  }

  /** Ссылки как в terminal_links.py (веб-терминал + каталог). */
  function terminalSearchUrl(ticker) {
    const t = String(ticker || "").trim().toUpperCase();
    if (!t) return "https://www.tbank.ru/terminal/";
    return "https://www.tbank.ru/terminal/?search=" + encodeURIComponent(t);
  }

  function investInstrumentUrl(ticker, classCode) {
    const t = String(ticker || "").trim().toUpperCase();
    const cc = String(classCode || "").trim().toUpperCase();
    const base = "https://www.tbank.ru/invest";
    if (!t) return terminalSearchUrl("");
    if (cc === "SPBFUT") return base + "/futures/" + encodeURIComponent(t) + "/";
    if (
      ["TQBR", "TQTF", "TQTD", "TQTE", "TQCB", "TQOB"].indexOf(cc) !== -1
    ) {
      if (cc === "TQTF") return base + "/etfs/" + encodeURIComponent(t) + "/";
      return base + "/stocks/" + encodeURIComponent(t) + "/";
    }
    return terminalSearchUrl(ticker);
  }

  function tickerCellHtml(ticker, classCode) {
    const href = esc(investInstrumentUrl(ticker, classCode));
    return (
      '<a class="ticker-link" href="' +
      href +
      '" target="_blank" rel="noopener noreferrer" title="Открыть в Т‑Инвестиции / терминале"><code>' +
      esc(ticker) +
      "</code></a>"
    );
  }

  function createChart(canvas, config) {
    if (!canvas || typeof window.Chart !== "function") return null;
    try {
      return new window.Chart(canvas, config);
    } catch (e) {
      console.error("Chart init failed", e);
      return null;
    }
  }

  function qClass(q) {
    if (q == null || isNaN(q)) return "";
    if (q >= 70) return "q-high";
    if (q >= 45) return "q-mid";
    return "q-low";
  }

  function chartScales() {
    const tick = "#a1a1aa";
    const grid = "rgba(63, 63, 70, 0.55)";
    return {
      x: { ticks: { color: tick, maxRotation: 45 }, grid: { color: grid } },
      y: { ticks: { color: tick }, grid: { color: grid } },
    };
  }

  function chartLegendOpts() {
    return { labels: { color: "#d4d4d8", boxWidth: 10, padding: 12 } };
  }

  function navLink(route, label) {
    const h = location.hash || "#/overview";
    const active = h.indexOf("#/" + route) === 0 ? "active" : "";
    return `<a class="${active}" href="#/${route}">${esc(label)}</a>`;
  }

  function shell(inner) {
    const tok = getToken();
    return `
      <div class="layout">
        <aside class="sidebar">
          <div class="sidebar-brand">
            <span class="brand-mark" aria-hidden="true">◇</span>
            <div class="brand-text">
              <strong>Сигналы</strong>
              <div class="brand-sub">панель управления</div>
            </div>
          </div>
          <h2>Разделы</h2>
          <nav class="sidebar-nav">
            ${navLink("overview", "Обзор")}
            ${navLink("table", "Таблица")}
            ${navLink("catalog", "Типы сигналов")}
            ${navLink("tickers", "Тикеры")}
            ${navLink("quality", "Качество")}
            ${navLink("slices", "Разрезы")}
            ${navLink("accuracy", "Точность (JSON)")}
            ${navLink("unary", "Unary API")}
          </nav>
          <div class="sidebar-token">
            <h2>Доступ</h2>
            <input type="password" id="admTok" placeholder="ADMIN_API_TOKEN" value="${esc(tok)}" autocomplete="off" />
            <button type="button" id="admSaveTok">Сохранить токен</button>
          </div>
        </aside>
        <main class="main-panel">
          <div class="toolbar toolbar-card">
            <div class="field">
              <label for="fMin">Период</label>
              <select id="fMin">
                <option value="0">Всё время</option>
                <option value="60">60 мин</option>
                <option value="360">6 ч</option>
                <option value="1440">24 ч</option>
                <option value="4320">3 дня</option>
                <option value="10080">7 дней</option>
              </select>
            </div>
            <div class="field">
              <label for="fMq">Мин. оценка</label>
              <input id="fMq" type="number" min="0" max="100" step="1" placeholder="—" style="width:5.5rem" />
            </div>
            <div class="field">
              <label for="fTy">Тип сигнала</label>
              <input id="fTy" type="text" placeholder="напр. volume_spike" style="width:11rem" />
            </div>
            <div class="field field-actions">
              <button type="button" id="fApply">Применить</button>
              <button type="button" class="ghost" id="fCsv">Экспорт CSV</button>
            </div>
          </div>
          <div id="mainInner" class="main-inner">${inner}</div>
        </main>
      </div>`;
  }

  function syncFiltersFromState() {
    const sm = document.getElementById("fMin");
    const mq = document.getElementById("fMq");
    const ty = document.getElementById("fTy");
    if (sm) sm.value = String(state.minutes);
    if (mq) mq.value = state.minq;
    if (ty) ty.value = state.signalType;
  }

  function bindShellHandlers() {
    document.getElementById("admSaveTok").onclick = () => {
      setToken(document.getElementById("admTok").value);
      alert("Токен сохранён в localStorage.");
    };
    document.getElementById("fApply").onclick = () => {
      state.minutes = parseInt(document.getElementById("fMin").value, 10) || 0;
      state.minq = document.getElementById("fMq").value.trim();
      state.signalType = document.getElementById("fTy").value.trim();
      state.offset = 0;
      writeUrlState();
      route();
    };
    document.getElementById("fCsv").onclick = downloadCsv;
    syncFiltersFromState();
  }

  async function downloadCsv() {
    try {
      const qs = new URLSearchParams();
      qs.set("minutes", String(state.minutes));
      if (state.minq) qs.set("min_quality", state.minq);
      if (state.signalType) qs.set("signal_type", state.signalType);
      const url =
        location.origin + "/admin/api/signals/export.csv?" + qs.toString();
      const r = await fetch(url, { headers: { "X-Admin-Token": getToken() } });
      if (!r.ok) throw new Error(await r.text());
      const blob = await r.blob();
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = "signals_export.csv";
      a.click();
      URL.revokeObjectURL(a.href);
    } catch (e) {
      alert(String(e.message || e));
    }
  }

  function renderHeatmap(cells) {
    const grid = {};
    cells.forEach((c) => {
      grid[c.dow + "_" + c.hod] = Number(c.c) || 0;
    });
    const max = Math.max(1, ...cells.map((c) => Number(c.c) || 0));
    const dows = [1, 2, 3, 4, 5, 6, 7];
    const dnames = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"];
    let h = '<table class="heatmap"><thead><tr><th></th>';
    for (let hour = 0; hour < 24; hour++) h += "<th>" + hour + "</th>";
    h += "</tr></thead><tbody>";
    dows.forEach((d, idx) => {
      h += "<tr><th>" + dnames[idx] + "</th>";
      for (let hour = 0; hour < 24; hour++) {
        const v = grid[d + "_" + hour] || 0;
        const r = Math.min(3, Math.ceil((v / max) * 3));
        h += '<td class="hm' + r + '" title="' + v + '">' + (v || "") + "</td>";
      }
      h += "</tr>";
    });
    h +=
      '</tbody></table><p class="lead" style="font-size:0.8rem;color:var(--muted);margin-top:0.75rem">День недели × час UTC (ISO).</p>';
    return '<div class="heatmap-wrap">' + h + "</div>";
  }

  async function pageOverview() {
    destroyCharts();
    const ov = await api(
      "/admin/api/overview?minutes=" + encodeURIComponent(state.minutes)
    );
    let sl = {
      rapid_followups_within_5m: 0,
      total_signals: 0,
      rapid_followup_rate: 0,
      heatmap_utc: [],
    };
    try {
      sl = await api(
        "/admin/api/slices?minutes=" + encodeURIComponent(state.minutes)
      );
    } catch (e) {
      console.warn("slices", e);
    }
    const t = ov.totals || {};
    const fmt = (x) =>
      x == null || isNaN(x) ? "—" : Number(x).toFixed(1);
    let cmp = "";
    if (ov.compare_windows) {
      const c = ov.compare_windows.current;
      const p = ov.compare_windows.previous;
      cmp = `<div class="msg info">Сравнение окон (${ov.compare_windows.window_minutes} мин): сейчас <b>${c.total}</b> сигн., ср.оценка ${fmt(c.avg_quality)}; ранее <b>${p.total}</b>, ср.оценка ${fmt(p.avg_quality)}</div>`;
    }
    const inner =
      cmp +
      `<div class="page-head"><h1>Обзор</h1>
      <p class="lead">Сводка сигналов за выбранный период: объёмы, типы, тикеры и распределение оценки.</p></div><div class="kpis">
      <div class="kpi"><div class="l">Всего</div><div class="v">${t.total ?? 0}</div></div>
      <div class="kpi"><div class="l">Средняя оценка</div><div class="v">${fmt(t.avg_quality)}</div></div>
      <div class="kpi"><div class="l">Медиана</div><div class="v">${fmt(t.median_quality)}</div></div>
      <div class="kpi"><div class="l">Первый</div><div class="v" style="font-size:0.85rem">${esc((t.first_detected_at || "—").slice(0, 19))}</div></div>
      <div class="kpi"><div class="l">Последний</div><div class="v" style="font-size:0.85rem">${esc((t.last_detected_at || "—").slice(0, 19))}</div></div>
    </div>
    <p class="lead" style="font-size:0.875rem;color:var(--muted);margin:0 0 1rem">Повторы за 5 мин по тому же инструменту: <b>${sl.rapid_followups_within_5m}</b> из ${sl.total_signals} (<b>${(sl.rapid_followup_rate * 100).toFixed(2)}%</b>)</p>
    <div class="charts">
      <div class="chart-wrap"><h3>Динамика</h3><canvas id="cTime"></canvas></div>
      <div class="chart-wrap"><h3>По типу</h3><canvas id="cType"></canvas></div>
    </div>
    <div class="charts">
      <div class="chart-wrap"><h3>Серьёзность</h3><canvas id="cSev"></canvas></div>
      <div class="chart-wrap"><h3>Топ тикеров</h3><canvas id="cTick"></canvas></div>
    </div>
    <div class="chart-wrap" style="height:220px"><h3>Оценка (корзины)</h3><canvas id="cQual"></canvas></div>`;
    document.getElementById("app").innerHTML = shell(inner);
    bindShellHandlers();
    if (!t.total) {
      document.getElementById("mainInner").innerHTML +=
        '<div class="msg warn">Нет данных за выбранный период.</div>';
      return;
    }
    if (typeof window.Chart !== "function") {
      document.querySelectorAll("#mainInner .chart-wrap").forEach(function (w) {
        const c = w.querySelector("canvas");
        if (c) c.style.display = "none";
        const p = document.createElement("p");
        p.className = "msg warn";
        p.style.margin = "0.75rem";
        p.textContent =
          "Графики недоступны: не выполнился скрипт Chart.js (/admin/vendor/chart.umd.min.js). Обновите API и проверьте, что файл есть в образе.";
        w.appendChild(p);
      });
      return;
    }
    const tl = ov.hourly || [];
    const gran = ov.timeline_granularity || "hour";
    const labels = tl.map((x) =>
      gran === "day"
        ? (x.bucket || "").slice(0, 10)
        : (x.bucket || "").slice(5, 16).replace("T", " ")
    );
    const bs = chartScales();
    charts.cTime = createChart(document.getElementById("cTime"), {
      type: "bar",
      data: {
        labels,
        datasets: [
          {
            label: "Сигналов",
            data: tl.map((x) => x.signal_count),
            backgroundColor: "rgba(56, 189, 248, 0.55)",
            borderRadius: 4,
          },
          {
            label: "Ср.оценка",
            type: "line",
            data: tl.map((x) =>
              x.avg_quality == null ? null : Number(x.avg_quality)
            ),
            yAxisID: "y1",
            borderColor: "#4ade80",
            tension: 0.25,
            borderWidth: 2,
            fill: false,
            pointRadius: 0,
            pointHoverRadius: 4,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: chartLegendOpts() },
        scales: {
          x: bs.x,
          y: Object.assign({}, bs.y, { position: "left" }),
          y1: Object.assign({}, bs.y, {
            position: "right",
            grid: { drawOnChartArea: false },
            min: 0,
            max: 100,
          }),
        },
      },
    });
    const bt = ov.by_type || [];
    charts.cType = createChart(document.getElementById("cType"), {
      type: "bar",
      data: {
        labels: bt.map((r) => r.signal_type),
        datasets: [
          {
            label: "Кол-во",
            data: bt.map((r) => r.signal_count),
            backgroundColor: "rgba(167, 139, 250, 0.55)",
            borderRadius: 4,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: chartLegendOpts() },
        scales: { x: bs.x, y: bs.y },
      },
    });
    const sev = ov.by_severity || [];
    charts.cSev = createChart(document.getElementById("cSev"), {
      type: "bar",
      data: {
        labels: sev.map((r) => "ур. " + r.severity),
        datasets: [
          {
            label: "Кол-во",
            data: sev.map((r) => r.signal_count),
            backgroundColor: "rgba(74, 222, 128, 0.55)",
            borderRadius: 4,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: chartLegendOpts() },
        scales: { x: bs.x, y: bs.y },
      },
    });
    const tk = ov.by_ticker || [];
    charts.cTick = createChart(document.getElementById("cTick"), {
      type: "bar",
      data: {
        labels: tk.map((r) => r.ticker),
        datasets: [
          {
            label: "N",
            data: tk.map((r) => r.signal_count),
            backgroundColor: "rgba(251, 146, 60, 0.55)",
            borderRadius: 4,
          },
        ],
      },
      options: {
        indexAxis: "y",
        responsive: true,
        maintainAspectRatio: false,
        onClick: function (_evt, elements, chart) {
          if (!elements || !elements.length) return;
          const ix = elements[0].index;
          const tick = chart.data.labels[ix];
          if (tick)
            window.open(
              terminalSearchUrl(String(tick)),
              "_blank",
              "noopener,noreferrer"
            );
        },
        plugins: { legend: chartLegendOpts() },
        scales: { x: bs.x, y: bs.y },
      },
    });
    const qb = ov.quality_buckets || [];
    const doughnutColors = [
      "#3f3f46",
      "#52525b",
      "#71717a",
      "#38bdf8",
      "#4ade80",
      "#fbbf24",
      "#fb7185",
    ];
    charts.cQual = createChart(document.getElementById("cQual"), {
      type: "doughnut",
      data: {
        labels: qb.map((r) => r.bucket_label),
        datasets: [
          {
            data: qb.map((r) => r.signal_count),
            backgroundColor: qb.map(
              (_, i) => doughnutColors[i % doughnutColors.length]
            ),
            borderWidth: 2,
            borderColor: "#18181b",
            hoverOffset: 6,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: "58%",
        plugins: {
          legend: Object.assign({ position: "right" }, chartLegendOpts()),
        },
      },
    });
  }

  async function pageTable() {
    destroyCharts();
    let qs =
      "?minutes=" +
      encodeURIComponent(state.minutes) +
      "&limit=" +
      state.limit +
      "&offset=" +
      state.offset;
    if (state.minq) qs += "&min_quality=" + encodeURIComponent(state.minq);
    if (state.signalType) qs += "&signal_type=" + encodeURIComponent(state.signalType);
    const data = await api("/admin/api/signals" + qs);
    const rows = data.items || [];
    let tbl =
      '<div class="page-head"><h1>Таблица</h1><p class="lead">Постраничный список сигналов с фильтрами из панели выше.</p></div><div class="table-card"><div class="table-scroll"><table class="data-table"><thead><tr><th>Время</th><th>Тикер</th><th class="td-wrap">Тип</th><th class="th-num">Sev</th><th class="th-num">Оценка</th><th class="th-num">|z|</th><th>Разметка</th><th></th></tr></thead><tbody>';
    rows.forEach((row) => {
      const p = row.payload || {};
      const q = p.quality_score != null ? Math.round(Number(p.quality_score)) : null;
      const fb = row.admin_feedback_label || "—";
      const qc = qClass(q);
      tbl +=
        "<tr><td>" +
        esc((row.detected_at || "").replace("T", " ").slice(0, 19)) +
        "</td><td>" +
        tickerCellHtml(row.ticker, row.class_code) +
        '</td><td class="td-wrap">' +
        esc(row.signal_type) +
        '</td><td class="td-num">' +
        esc(row.severity) +
        '</td><td class="td-num' +
        (qc ? " " + qc : "") +
        '">' +
        (q == null ? "—" : q) +
        '</td><td class="td-num">' +
        (row.z_score == null
          ? "—"
          : Math.abs(Number(row.z_score)).toFixed(2)) +
        "</td><td>" +
        esc(fb) +
        '</td><td><a href="#/signal?id=' +
        esc(row.signal_id) +
        '">Открыть</a></td></tr>';
    });
    tbl +=
      "</tbody></table></div>" +
      '<div class="pager"><button class="ghost" id="pgP">Назад</button><span>' +
      esc(
        "Показано " +
          rows.length +
          " из " +
          (data.total || 0) +
          ", offset " +
          state.offset
      ) +
      '</span><button class="ghost" id="pgN">Вперёд</button></div></div>';
    document.getElementById("app").innerHTML = shell(tbl);
    bindShellHandlers();
    document.getElementById("pgP").onclick = () => {
      state.offset = Math.max(0, state.offset - state.limit);
      writeUrlState();
      route();
    };
    document.getElementById("pgN").onclick = () => {
      state.offset += state.limit;
      writeUrlState();
      route();
    };
  }

  async function pageCatalog() {
    destroyCharts();
    const ov = await api(
      "/admin/api/overview?minutes=" + encodeURIComponent(state.minutes)
    );
    const bt = ov.by_type || [];
    let h =
      '<div class="page-head"><h1>Типы сигналов</h1><p class="lead">Агрегаты за выбранный период. Подробности — в docs/detectors.md</p></div><div class="table-card"><div class="table-scroll"><table class="data-table"><thead><tr><th class="td-wrap">Тип</th><th class="th-num">Кол-во</th><th class="th-num">Ср. оценка</th></tr></thead><tbody>';
    bt.forEach((r) => {
      h +=
        '<tr><td class="td-wrap"><code>' +
        esc(r.signal_type) +
        '</code></td><td class="td-num">' +
        r.signal_count +
        '</td><td class="td-num">' +
        (r.avg_quality == null ? "—" : Number(r.avg_quality).toFixed(1)) +
        "</td></tr>";
    });
    h += "</tbody></table></div></div>";
    document.getElementById("app").innerHTML = shell(h);
    bindShellHandlers();
  }

  async function pageTickers() {
    destroyCharts();
    const ov = await api(
      "/admin/api/overview?minutes=" + encodeURIComponent(state.minutes)
    );
    const tk = ov.by_ticker || [];
    let h =
      '<div class="page-head"><h1>Тикеры</h1><p class="lead">Сколько сигналов пришло по каждому тикеру за период.</p></div><div class="table-card"><div class="table-scroll"><table class="data-table"><thead><tr><th>Тикер</th><th class="th-num">Кол-во</th><th class="th-num">Ср. оценка</th></tr></thead><tbody>';
    tk.forEach((r) => {
      h +=
        "<tr><td>" +
        tickerCellHtml(r.ticker, "") +
        '</td><td class="td-num">' +
        r.signal_count +
        '</td><td class="td-num">' +
        (r.avg_quality == null ? "—" : Number(r.avg_quality).toFixed(1)) +
        "</td></tr>";
    });
    h += "</tbody></table></div></div>";
    document.getElementById("app").innerHTML = shell(h);
    bindShellHandlers();
  }

  async function pageQuality() {
    await pageOverview();
    const inner = document.getElementById("mainInner");
    if (!inner) return;
    const h1 = inner.querySelector("h1");
    if (h1) h1.textContent = "Качество и обзор";
    inner.insertAdjacentHTML(
      "afterbegin",
      '<p class="msg info" style="margin-bottom:0.75rem">Оценка 0–100 в payload — <b>эвристика</b> (|z|, величина, severity, вес типа). Истинная полезность: раздел <a href="#/accuracy">Точность</a> (офлайн DuckDB) и разметка на карточке сигнала.</p>'
    );
  }

  async function pageSlices() {
    destroyCharts();
    const sl = await api(
      "/admin/api/slices?minutes=" + encodeURIComponent(state.minutes)
    );
    const inner =
      '<div class="page-head"><h1>Разрезы</h1>' +
      '<p class="lead">Тепловая карта: день недели × час UTC. Метрика быстрых повторов — следующий сигнал по тому же инструменту в течение 5 минут.</p></div>' +
      "<p><b>Всего сигналов:</b> " +
      sl.total_signals +
      ", <b>быстрых пар:</b> " +
      sl.rapid_followups_within_5m +
      " (<b>" +
      (sl.rapid_followup_rate * 100).toFixed(2) +
      "%</b>)</p>" +
      renderHeatmap(sl.heatmap_utc || []);
    document.getElementById("app").innerHTML = shell(inner);
    bindShellHandlers();
  }

  async function pageAccuracy() {
    destroyCharts();
    let body = "";
    try {
      const j = await api("/admin/api/accuracy");
      body = "<pre class=json>" + esc(JSON.stringify(j, null, 2)) + "</pre>";
    } catch (e) {
      body =
        '<div class="msg err">' +
        esc(String(e.message || e)) +
        "</div><p style=font-size:0.85rem;color:var(--muted)>Сгенерируйте JSON: <code>python scripts/duckdb_label_signals.py ...</code> и смонтируйте в контейнер api (<code>./var/accuracy</code>).</p>";
    }
    document.getElementById("app").innerHTML = shell(
      '<div class="page-head"><h1>Офлайн точность</h1><p class="lead">JSON из duckdb_label_signals (смонтированный файл).</p></div>' +
        body
    );
    bindShellHandlers();
  }

  const UNARY_IID_KEY = "tinvest_unary_last_instrument_id";

  async function pageUnary() {
    destroyCharts();
    const savedIid = (localStorage.getItem(UNARY_IID_KEY) || "").trim();
    let options = '<option value="">— выберите —</option>';
    let loadErr = "";
    try {
      const list = await api("/admin/api/instruments");
      (list.items || []).forEach((it) => {
        const lab =
          esc(String(it.ticker || "")) +
          " / " +
          esc(String(it.class_code || "")) +
          " (" +
          esc(String(it.instrument_id || "")) +
          ")";
        options +=
          '<option value="' +
          esc(String(it.instrument_id || "")) +
          '">' +
          lab +
          "</option>";
      });
    } catch (e) {
      loadErr =
        '<div class="msg err">' + esc(String(e.message || e)) + "</div>";
      options += '<option value="">(список недоступен)</option>';
    }
    document.getElementById("app").innerHTML = shell(
      '<div class="page-head"><h1>Unary T‑Invest</h1><p class="lead">Снимки <code>GetMarketValues</code> и <code>GetTechAnalysis</code> по <code>conf/instruments.yaml</code> (не стрим). Нужны <code>TINVEST_TOKEN</code> в окружении API и сохранённый здесь <code>ADMIN_API_TOKEN</code>. Периодическая публикация в Kafka: сервис <code>market-unary-emitter</code> (<code>docker compose --profile unary</code>).</p></div>' +
        loadErr +
        '<details class="card" open style="margin-bottom:0.75rem"><summary style="cursor:pointer;font-weight:600">Инструмент</summary><div style="margin-top:0.75rem">' +
        "<label>Из конфига</label><br/>" +
        '<select id="unInstr" style="min-width:18rem;margin:0.35rem 0">' +
        options +
        "</select>" +
        '<p style="font-size:0.85rem;color:var(--muted);margin:0.5rem 0 0.75rem">Или вручную <code>instrument_id</code> (сохраняется в localStorage):</p>' +
        '<input id="unInstrManual" type="text" placeholder="например SBER_TQBR" style="width:100%;max-width:24rem" />' +
        "</div></details>" +
        '<details class="card" open style="margin-bottom:0.75rem"><summary style="cursor:pointer;font-weight:600">GetMarketValues</summary><div style="margin-top:0.75rem">' +
        "<label>value_types (CSV, опционально)</label>" +
        '<input id="unMvTypes" type="text" placeholder="last_price,open_interest,close_price" style="width:100%;max-width:28rem;margin:0.35rem 0;display:block" />' +
        '<p><button type="button" id="unBtnMv">Загрузить рыночные значения</button></p></div></details>' +
        '<details class="card" open style="margin-bottom:0.75rem"><summary style="cursor:pointer;font-weight:600">GetTechAnalysis</summary><div style="margin-top:0.75rem">' +
        '<p style="display:flex;flex-wrap:wrap;gap:0.75rem;align-items:flex-end">' +
        '<span class="field"><label>indicator</label><br/><select id="unTaInd"><option>rsi</option><option>ema</option><option>sma</option><option>bb</option><option>macd</option></select></span>' +
        '<span class="field"><label>interval</label><br/><input id="unTaIv" type="text" value="1h" style="width:4rem" /></span>' +
        '<span class="field"><label>type_of_price</label><br/><select id="unTaTop"><option>close</option><option>open</option><option>high</option><option>low</option><option>avg</option></select></span>' +
        '<span class="field"><label>length</label><br/><input id="unTaLen" type="number" value="14" min="1" max="500" style="width:4.5rem" /></span>' +
        '<span class="field"><label>window_min</label><br/><input id="unTaWin" type="number" value="1440" min="5" style="width:5rem" /></span></p>' +
        '<p><button type="button" id="unBtnTa">Загрузить теханализ</button></p></div></details>' +
        '<div class="card"><h3 style="margin-top:0">Ответ</h3>' +
        '<p><button type="button" class="ghost" id="unCopyOut">Копировать JSON</button></p>' +
        '<pre id="unOut" class="json" style="max-height:70vh;overflow:auto;margin:0">{}</pre></div>'
    );
    bindShellHandlers();
    const sel = document.getElementById("unInstr");
    const manual = document.getElementById("unInstrManual");
    if (savedIid) {
      manual.value = savedIid;
      for (let i = 0; i < sel.options.length; i++) {
        if (sel.options[i].value === savedIid) {
          sel.selectedIndex = i;
          break;
        }
      }
    }
    sel.addEventListener("change", function () {
      if (sel.value) localStorage.setItem(UNARY_IID_KEY, sel.value);
    });
    manual.addEventListener("blur", function () {
      const v = curIid();
      if (v) localStorage.setItem(UNARY_IID_KEY, v);
    });
    function curIid() {
      const m = (document.getElementById("unInstrManual").value || "").trim();
      if (m) return m;
      return (document.getElementById("unInstr").value || "").trim();
    }
    document.getElementById("unBtnMv").onclick = async () => {
      const iid = curIid();
      if (!iid) {
        alert("Выберите инструмент или введите instrument_id");
        return;
      }
      const vt = (document.getElementById("unMvTypes").value || "").trim();
      let path =
        "/admin/api/instruments/" +
        encodeURIComponent(iid) +
        "/market-values";
      if (vt) path += "?" + new URLSearchParams({ value_types: vt }).toString();
      const out = document.getElementById("unOut");
      out.textContent = "Загрузка…";
      try {
        const j = await api(path);
        out.textContent = JSON.stringify(j, null, 2);
        localStorage.setItem(UNARY_IID_KEY, iid);
      } catch (e) {
        out.textContent = String(e.message || e);
      }
    };
    document.getElementById("unCopyOut").onclick = async function () {
      const t = document.getElementById("unOut").textContent || "";
      try {
        await navigator.clipboard.writeText(t);
        alert("Скопировано в буфер обмена");
      } catch (e) {
        alert(String(e.message || e) || "Не удалось скопировать");
      }
    };
    document.getElementById("unBtnTa").onclick = async () => {
      const iid = curIid();
      if (!iid) {
        alert("Выберите инструмент или введите instrument_id");
        return;
      }
      const qs = new URLSearchParams();
      qs.set("indicator", document.getElementById("unTaInd").value);
      qs.set("interval", document.getElementById("unTaIv").value.trim() || "1h");
      qs.set("type_of_price", document.getElementById("unTaTop").value);
      qs.set("length", String(document.getElementById("unTaLen").value || "14"));
      qs.set(
        "window_minutes",
        String(document.getElementById("unTaWin").value || "1440")
      );
      const path =
        "/admin/api/instruments/" +
        encodeURIComponent(iid) +
        "/tech-analysis?" +
        qs.toString();
      const out = document.getElementById("unOut");
      out.textContent = "Загрузка…";
      try {
        const j = await api(path);
        out.textContent = JSON.stringify(j, null, 2);
        localStorage.setItem(UNARY_IID_KEY, iid);
      } catch (e) {
        out.textContent = String(e.message || e);
      }
    };
  }

  async function pageSignal() {
    destroyCharts();
    const { q } = parseHash();
    const id = (q.get("id") || "").trim();
    if (!id) {
      document.getElementById("app").innerHTML =
        shell('<div class="msg err">Не указан id в hash: #/signal?id=UUID</div>');
      bindShellHandlers();
      return;
    }
    const row = await api("/admin/api/signal/" + encodeURIComponent(id));
    const p = row.payload || {};
    const qv = p.quality_score;
    const termU = p.terminal_url
      ? String(p.terminal_url)
      : "";
    const invU = p.instrument_page_url
      ? String(p.instrument_page_url)
      : "";
    let links = "";
    if (termU || invU) {
      links = '<div class="signal-meta" style="margin-top:0.5rem">';
      if (termU)
        links +=
          '<span class="link-pill"><a href="' +
          esc(termU) +
          '" target="_blank" rel="noopener">Терминал</a></span> ';
      if (invU)
        links +=
          '<span class="link-pill"><a href="' +
          esc(invU) +
          '" target="_blank" rel="noopener">Карточка</a></span>';
      links += "</div>";
    }
    let fbForm =
      '<div class="panel"><h3>Разметка</h3><p class="lead" style="margin-bottom:0.75rem">Текущая метка: <b>' +
      esc(row.admin_feedback_label || "нет") +
      "</b></p>" +
      '<div style="display:flex;gap:0.5rem;flex-wrap:wrap;align-items:center">' +
      '<select id="fbLab"><option value="useful">Полезно</option><option value="noise">Шум</option><option value="unsure">Не уверен</option></select>' +
      '<input id="fbNote" type="text" placeholder="Заметка" style="min-width:12rem;flex:1" />' +
      '<button type="button" id="fbSend">Сохранить</button></div></div>';
    let chBtn =
      '<div class="panel"><h3>ClickHouse</h3><p class="lead" style="margin-bottom:0.65rem">Сырые события вокруг времени сигнала (±2 мин).</p>' +
      '<button type="button" class="ghost" id="chLoad">Загрузить контекст</button>' +
      '<pre id="chOut" class="json" style="display:none;margin-top:0.75rem"></pre></div>';
    const card =
      '<article class="signal-page">' +
      '<div class="page-head"><h1>Сигнал</h1><p class="lead"><code>' +
      esc(row.signal_id) +
      "</code></p></div>" +
      '<p style="margin:0 0 0.5rem;font-size:0.9rem">' +
      tickerCellHtml(row.ticker, row.class_code) +
      " · <code>" +
      esc(row.signal_type) +
      "</code></p>" +
      '<div class="signal-summary">' +
      esc(row.summary) +
      "</div>" +
      '<div class="signal-meta">' +
      "<span>detected_at: <code>" +
      esc(row.detected_at) +
      "</code></span>" +
      "<span>|z|: <code>" +
      esc(row.z_score) +
      "</code></span>" +
      "<span>severity: <code>" +
      esc(row.severity) +
      "</code></span>" +
      "<span>оценка: <code>" +
      esc(qv != null ? String(qv) : "—") +
      "</code></span></div>" +
      links +
      '<div class="panel"><h3>Payload</h3><pre class="json">' +
      esc(JSON.stringify(p, null, 2)) +
      "</pre></div>" +
      fbForm +
      chBtn +
      "</article>";
    document.getElementById("app").innerHTML = shell(card);
    bindShellHandlers();
    if (row.admin_feedback_label) {
      const sel = document.getElementById("fbLab");
      if (sel) sel.value = row.admin_feedback_label;
    }
    document.getElementById("fbNote").value = row.admin_feedback_note || "";
    document.getElementById("fbSend").onclick = async () => {
      try {
        await api("/admin/api/feedback", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            signal_id: id,
            label: document.getElementById("fbLab").value,
            note: document.getElementById("fbNote").value,
          }),
        });
        alert("Сохранено");
        route();
      } catch (e) {
        alert(String(e.message || e));
      }
    };
    document.getElementById("chLoad").onclick = async () => {
      const out = document.getElementById("chOut");
      out.style.display = "block";
      out.textContent = "Загрузка…";
      try {
        const ctx = await api(
          "/admin/api/signal/" +
            encodeURIComponent(id) +
            "/context?seconds_before=120&seconds_after=120"
        );
        out.textContent = JSON.stringify(ctx, null, 2);
      } catch (e) {
        out.textContent = String(e.message || e);
      }
    };
  }

  async function route() {
    readUrlState();
    const { name } = parseHash();
    try {
      if (name === "overview" || name === "") await pageOverview();
      else if (name === "table") await pageTable();
      else if (name === "catalog") await pageCatalog();
      else if (name === "tickers") await pageTickers();
      else if (name === "quality") await pageQuality();
      else if (name === "slices") await pageSlices();
      else if (name === "accuracy") await pageAccuracy();
      else if (name === "unary") await pageUnary();
      else if (name === "signal") await pageSignal();
      else {
        document.getElementById("app").innerHTML = shell(
          '<div class="msg err">Неизвестный раздел: ' + esc(name) + "</div>"
        );
        bindShellHandlers();
      }
    } catch (e) {
      document.getElementById("app").innerHTML = shell(
        '<div class="msg err">' + esc(String(e.message || e)) + "</div>"
      );
      bindShellHandlers();
    }
    syncFiltersFromState();
  }

  window.addEventListener("hashchange", () => {
    route();
  });
  readUrlState();
  route();
})();
