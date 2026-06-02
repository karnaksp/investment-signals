(function () {
  "use strict";

  const TOKEN_KEY = "tinvest_admin_token";
  const app = document.getElementById("app");
  const state = {
    token: localStorage.getItem(TOKEN_KEY) || "",
    minutes: localStorage.getItem("tinvest_admin_minutes") || "1440",
    auto: localStorage.getItem("tinvest_admin_auto") !== "0",
    offset: 0,
    runtime: null,
    filters: {
      instrument: "",
      type: "",
      delivery: "",
      qmin: "",
      qmax: "",
      feedback: "",
      severity: "",
    },
  };

  const nav = [
    ["triage", "Triage", "TR"],
    ["signals", "Signals", "SG"],
    ["delivery", "Delivery", "DL"],
    ["calibration", "Calibration", "CL"],
    ["feedback", "Feedback", "FB"],
    ["instruments", "Instruments", "IN"],
    ["accuracy", "Accuracy", "AC"],
    ["settings", "Settings", "ST"],
  ];

  function route() {
    const raw = (location.hash || "#/triage").replace(/^#/, "") || "/triage";
    const u = new URL(raw, location.origin);
    return {
      name: (u.pathname.replace(/^\/+/, "") || "triage").toLowerCase(),
      params: u.searchParams,
    };
  }

  function esc(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function n(value, digits) {
    const num = finiteNumberOrNull(value);
    if (num == null) return "-";
    return num.toLocaleString("ru-RU", {
      maximumFractionDigits: digits == null ? 1 : digits,
    });
  }

  function pct(value) {
    const num = finiteNumberOrNull(value);
    if (num == null) return "-";
    return (num * 100).toFixed(1) + "%";
  }

  function shortTime(value) {
    if (!value) return "-";
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return "-";
    return d.toLocaleString("ru-RU", {
      day: "2-digit",
      month: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    });
  }

  function finiteNumberOrNull(value) {
    if (value == null || value === "") return null;
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  }

  function finiteNumber(value, fallback) {
    const num = finiteNumberOrNull(value);
    return num == null ? fallback : num;
  }

  function params(obj) {
    const qs = new URLSearchParams();
    Object.entries(obj).forEach(([k, v]) => {
      if (v !== undefined && v !== null && String(v).trim() !== "") {
        qs.set(k, String(v).trim());
      }
    });
    const s = qs.toString();
    return s ? "?" + s : "";
  }

  async function api(path, options) {
    const res = await fetch(path, {
      ...(options || {}),
      headers: {
        "X-Admin-Token": state.token,
        ...((options && options.headers) || {}),
      },
    });
    const text = await res.text();
    let body = null;
    try {
      body = text ? JSON.parse(text) : null;
    } catch (_) {
      body = text;
    }
    if (!res.ok) {
      const detail = body && body.detail ? body.detail : text || res.statusText;
      throw new Error(detail);
    }
    return body;
  }

  function badge(text, cls) {
    return `<span class="badge ${cls || ""}">${esc(text)}</span>`;
  }

  function deliveryBadge(status) {
    const s = status || "unknown";
    return badge(s, s === "delivered" ? "b-delivered" : s === "suppressed" ? "b-suppressed" : "b-unknown");
  }

  function qualityBadge(score) {
    if (score == null || score === "") return badge("q -", "b-unknown");
    const q = Number(score);
    const cls = q >= 72 ? "b-high" : q >= 48 ? "b-medium" : "b-low";
    return badge("q " + Math.round(q), cls);
  }

  function severityBadge(sev) {
    const s = Number(sev || 0);
    return badge("S" + (s || "-"), s >= 3 ? "b-sev3" : s === 2 ? "b-sev2" : "");
  }

  function signalScore(row) {
    const p = row.payload || {};
    const q = Number(p.quality_score || 0);
    const delivered = (row.delivery_status || p.delivery_status) === "delivered" ? 100 : 0;
    return delivered + q + Number(row.severity || 0) * 8 + Math.abs(Number(row.z_score || 0));
  }

  function humanLine(row) {
    const p = row.payload || {};
    const i = p.interpretation || {};
    return i.headline_ru || p.interpretation_ru || (row.summary || "").split("\n")[0] || "";
  }

  function interpretationFacts(payload) {
    const i = (payload || {}).interpretation || {};
    const facts = Array.isArray(i.facts) ? i.facts : [];
    if (!facts.length) return "";
    return `
      <div class="fact-grid">
        ${facts.slice(0, 12).map((f) => `
          <div class="fact">
            <div class="label">${esc(f.label || f.key || "")}</div>
            <div class="value">${esc(f.value || "")}</div>
          </div>
        `).join("")}
      </div>
    `;
  }

  function renderShell(active) {
    app.className = "";
    app.innerHTML = `
      <div class="shell">
        <aside class="side">
          <div class="brand">
            <div class="brand-mark">SC</div>
            <div>
              <div class="brand-title">Signal Cockpit</div>
              <div class="brand-sub">T-Invest anomaly desk</div>
            </div>
          </div>
          <nav class="nav">
            ${nav.map(([id, label, glyph]) => `
              <a href="#/${id}" class="${id === active ? "active" : ""}">
                <span class="glyph">${glyph}</span><span>${label}</span>
              </a>
            `).join("")}
          </nav>
          <div class="side-footer">
            <label for="tokenInput">Admin token</label>
            <input id="tokenInput" type="password" value="${esc(state.token)}" autocomplete="off" />
            <button id="saveToken" class="primary">Save token</button>
          </div>
        </aside>
        <main class="main">
          <div class="topbar">
            <div class="topbar-left">
              <span class="status-dot"></span>
              <span id="runtimeBadge" class="runtime-badge"></span>
              <span class="status-text"><strong>${esc(activeLabel(active))}</strong> / ${esc(periodLabel())}</span>
            </div>
            <div class="topbar-right">
              <select id="periodSelect" title="Период">
                ${periodOption("60", "1h")}
                ${periodOption("360", "6h")}
                ${periodOption("1440", "24h")}
                ${periodOption("10080", "7d")}
                ${periodOption("0", "All")}
              </select>
              <label class="toggle"><input id="autoRefresh" type="checkbox" ${state.auto ? "checked" : ""} /> auto</label>
              <button id="refreshBtn" class="icon" title="Refresh">R</button>
            </div>
          </div>
          <section id="view" class="view"></section>
        </main>
      </div>
    `;
    document.getElementById("saveToken").onclick = () => {
      state.token = document.getElementById("tokenInput").value.trim();
      localStorage.setItem(TOKEN_KEY, state.token);
      loadCurrent();
    };
    document.getElementById("periodSelect").onchange = (e) => {
      state.minutes = e.target.value;
      localStorage.setItem("tinvest_admin_minutes", state.minutes);
      state.offset = 0;
      loadCurrent();
    };
    document.getElementById("autoRefresh").onchange = (e) => {
      state.auto = e.target.checked;
      localStorage.setItem("tinvest_admin_auto", state.auto ? "1" : "0");
    };
    document.getElementById("refreshBtn").onclick = () => loadCurrent();
    updateRuntimeBadge();
  }

  function rememberRuntime(data) {
    if (data && data.runtime) {
      state.runtime = data.runtime;
      updateRuntimeBadge();
    }
  }

  function runtimeLabel() {
    const rt = state.runtime || {};
    const sha = String(rt.commit_sha || "unknown");
    const shortSha = sha === "unknown" ? sha : sha.slice(0, 12);
    const version = rt.app_version || "0.1.0";
    const built = rt.build_time && rt.build_time !== "unknown" ? " / " + rt.build_time : "";
    return "v" + version + " / " + shortSha + built;
  }

  function updateRuntimeBadge() {
    const el = document.getElementById("runtimeBadge");
    if (el && state.runtime) el.textContent = runtimeLabel();
  }

  function periodOption(value, label) {
    return `<option value="${value}" ${state.minutes === value ? "selected" : ""}>${label}</option>`;
  }

  function activeLabel(active) {
    const item = nav.find((x) => x[0] === active);
    return item ? item[1] : "Signal";
  }

  function periodLabel() {
    return state.minutes === "0" ? "all time" : "last " + activePeriodShort();
  }

  function activePeriodShort() {
    return state.minutes === "60" ? "1h" :
      state.minutes === "360" ? "6h" :
      state.minutes === "1440" ? "24h" :
      state.minutes === "10080" ? "7d" : "all";
  }

  function sourceHealthMinutes() {
    const m = Number(state.minutes || 1440);
    return m > 0 ? m : 1440;
  }

  function view() {
    return document.getElementById("view");
  }

  function authEmpty() {
    view().innerHTML = `
      <div class="page-head"><div><h1>Signal Cockpit</h1><p>Введите ADMIN_API_TOKEN в левой панели.</p></div></div>
      <div class="empty">API защищён токеном. Токен хранится только в localStorage браузера.</div>
    `;
  }

  function pageHead(title, sub, actionHtml) {
    return `
      <div class="page-head">
        <div><h1>${esc(title)}</h1>${sub ? `<p>${esc(sub)}</p>` : ""}</div>
        ${actionHtml || ""}
      </div>
    `;
  }

  function metrics(items) {
    return `<div class="metrics">${items.map((m) => `
      <div class="metric">
        <div class="label">${esc(m.label)}</div>
        <div class="value">${m.value}</div>
        <div class="hint">${esc(m.hint || "")}</div>
      </div>
    `).join("")}</div>`;
  }

  function table(headers, rows, emptyText) {
    if (!rows.length) return `<div class="empty">${esc(emptyText || "Нет данных")}</div>`;
    return `
      <div class="table-wrap">
        <table>
          <thead><tr>${headers.map((h) => `<th class="${h.cls || ""}">${esc(h.label)}</th>`).join("")}</tr></thead>
          <tbody>${rows.join("")}</tbody>
        </table>
      </div>
    `;
  }

  function signalRow(row) {
    const p = row.payload || {};
    const status = row.delivery_status || p.delivery_status || "unknown";
    const reason = row.delivery_reason || p.delivery_reason || "unknown";
    return `
      <tr>
        <td class="mono">${shortTime(row.detected_at)}</td>
        <td><a href="#/signal?id=${encodeURIComponent(row.signal_id)}"><strong>${esc(row.ticker)}</strong></a><div class="muted">${esc(row.instrument_id)}</div></td>
        <td><span class="clip">${esc(row.signal_type)}</span></td>
        <td>${deliveryBadge(status)}<div class="muted clip">${esc(reason)}</div></td>
        <td>${qualityBadge(p.quality_score)}</td>
        <td>${severityBadge(row.severity)}</td>
        <td class="num">${n(row.z_score, 2)}</td>
        <td class="clip">${esc(humanLine(row))}</td>
      </tr>
    `;
  }

  async function pageTriage() {
    const [ov, delivery, signals, settings] = await Promise.all([
      api("/admin/api/overview" + params({ minutes: state.minutes })),
      api("/admin/api/delivery/overview" + params({ minutes: state.minutes })),
      api("/admin/api/signals" + params({ minutes: state.minutes, limit: 40 })),
      api("/admin/api/settings"),
    ]);
    rememberRuntime(settings);
    const totals = delivery.totals || {};
    const all = totals.total || 0;
    const ranked = (signals.items || []).slice().sort((a, b) => signalScore(b) - signalScore(a)).slice(0, 14);
    const last = (ov.totals || {}).last_detected_at;
    view().innerHTML = `
      ${pageHead("Triage", "Очередь внимания, доставка и шум за выбранный период.")}
      ${metrics([
        { label: "Generated", value: n(all, 0), hint: "saved signals" },
        { label: "Delivered", value: n(totals.delivered, 0), hint: pct(totals.delivery_rate) },
        { label: "Suppressed", value: n(totals.suppressed, 0), hint: "visible in cockpit" },
        { label: "Last signal", value: shortTime(last), hint: "exchange stream" },
      ])}
      <div class="grid-2">
        <section class="panel">
          <div class="panel-head"><h2>Priority Queue</h2><a href="#/signals">Open table</a></div>
          ${table(priorityHeaders(), ranked.map(priorityRow), "Нет сигналов за период")}
        </section>
        <div class="stack">
          <section class="panel">
            <div class="panel-head"><h2>Delivery Funnel</h2></div>
            <div class="panel-body">${deliveryFunnel(totals)}</div>
          </section>
          <section class="panel">
            <div class="panel-head"><h2>Suppressed Reasons</h2><a href="#/delivery">Details</a></div>
            <div class="panel-body">${reasonBars(delivery.reasons || [])}</div>
          </section>
          <section class="panel">
            <div class="panel-head"><h2>Hot Tickers</h2><a href="#/instruments">All</a></div>
            ${tickerTable(delivery.by_ticker || [], 8)}
          </section>
        </div>
      </div>
    `;
  }

  function signalHeaders() {
    return [
      { label: "Time" },
      { label: "Ticker" },
      { label: "Type" },
      { label: "Delivery" },
      { label: "Quality" },
      { label: "Sev" },
      { label: "z", cls: "num" },
      { label: "Summary" },
    ];
  }

  function priorityHeaders() {
    return [
      { label: "Time" },
      { label: "Ticker" },
      { label: "Type" },
      { label: "Delivery" },
      { label: "Quality" },
      { label: "z", cls: "num" },
    ];
  }

  function priorityRow(row) {
    const p = row.payload || {};
    const status = row.delivery_status || p.delivery_status || "unknown";
    const reason = row.delivery_reason || p.delivery_reason || "unknown";
    return `
      <tr>
        <td class="mono">${shortTime(row.detected_at)}</td>
        <td><a href="#/signal?id=${encodeURIComponent(row.signal_id)}"><strong>${esc(row.ticker)}</strong></a><div class="muted">${esc(row.instrument_id)}</div></td>
        <td><span class="clip">${esc(row.signal_type)}</span></td>
        <td>${deliveryBadge(status)}<div class="muted clip">${esc(reason)}</div></td>
        <td>${qualityBadge(p.quality_score)}</td>
        <td class="num">${n(row.z_score, 2)}</td>
      </tr>
    `;
  }

  function deliveryFunnel(t) {
    const total = Number(t.total || 0) || 1;
    return barList([
      ["delivered", Number(t.delivered || 0), total],
      ["suppressed", Number(t.suppressed || 0), total],
      ["unknown", Number(t.unknown || 0), total],
    ]);
  }

  function reasonBars(rows) {
    const filtered = rows.filter((r) => r.delivery_status !== "delivered").slice(0, 8);
    const max = Math.max(1, ...filtered.map((r) => Number(r.signal_count || 0)));
    return barList(filtered.map((r) => [r.delivery_reason, Number(r.signal_count || 0), max]));
  }

  function barList(rows) {
    if (!rows.length) return `<div class="empty">Нет данных</div>`;
    return `<div class="bar">${rows.map(([label, value, max]) => `
      <div class="bar-row">
        <div class="clip">${esc(label)}</div>
        <div class="bar-track"><div class="bar-fill" style="width:${Math.max(2, Math.min(100, (Number(value) / Number(max || 1)) * 100))}%"></div></div>
        <div class="num">${n(value, 0)}</div>
      </div>
    `).join("")}</div>`;
  }

  function tickerTable(rows, limit) {
    const items = rows.slice(0, limit || 40);
    return table(
      [{ label: "Ticker" }, { label: "Total", cls: "num" }, { label: "Delivered", cls: "num" }, { label: "Rate", cls: "num" }],
      items.map((r) => {
        const total = Number(r.total || r.signal_count || 0);
        const delivered = Number(r.delivered || 0);
        return `<tr>
          <td><strong>${esc(r.ticker)}</strong><div class="muted">${shortTime(r.last_detected_at)}</div></td>
          <td class="num">${n(total, 0)}</td>
          <td class="num">${n(delivered, 0)}</td>
          <td class="num">${pct(total ? delivered / total : 0)}</td>
        </tr>`;
      }),
      "Нет тикеров"
    );
  }

  function sourceBadges(sources) {
    const list = Array.isArray(sources) ? sources : [];
    if (!list.length) return `<span class="muted">none</span>`;
    return list.map((name) => badge(name)).join(" ");
  }

  function sourceHealthBadges(sourceHealth) {
    const data = sourceHealth || {};
    const rows = Object.entries(data).filter(([, info]) => info && info.subscribed);
    if (!rows.length) return `<span class="muted">not subscribed</span>`;
    return rows.map(([name, info]) => {
      const status = info.status || "unknown";
      const cls = status === "ok" ? "b-high" :
        status === "stale" || status === "missing" ? "b-medium" :
        status === "not_subscribed" ? "b-unknown" : "b-low";
      const last = info.last_source_time ? shortTime(info.last_source_time) : status;
      return `${badge(name + ":" + status, cls)}<div class="muted clip">${esc(last)}</div>`;
    }).join("");
  }

  function unavailableSignalHint(rows) {
    const blocked = (Array.isArray(rows) ? rows : [])
      .filter((r) => !r.enabled)
      .slice(0, 3)
      .map((r) => (r.signal_type || "") + ":" + (r.reason || "blocked"));
    return blocked.length ? blocked.join(" / ") : "all configured signals possible";
  }

  function instrumentUniverseTable(rows) {
    return table(
      [
        { label: "Instrument" },
        { label: "Sources" },
        { label: "Freshness" },
        { label: "Blocked" },
        { label: "Signals", cls: "num" },
        { label: "Delivered", cls: "num" },
        { label: "Rate", cls: "num" },
        { label: "Avg q", cls: "num" },
        { label: "Last" },
      ],
      rows.map((r) => {
        const total = Number(r.total || 0);
        const delivered = Number(r.delivered || 0);
        return `<tr>
          <td><strong>${esc(r.ticker)}</strong><div class="muted">${esc(r.instrument_id)} / ${esc(r.alias || "")}</div></td>
          <td>${sourceBadges(r.sources)}<div class="muted">book ${esc((r.subscriptions || {}).order_book_depth || "off")}</div></td>
          <td>${sourceHealthBadges(r.source_health)}</td>
          <td class="clip">${esc(unavailableSignalHint(r.signal_availability))}</td>
          <td class="num">${n(total, 0)}</td>
          <td class="num">${n(delivered, 0)}</td>
          <td class="num">${pct(total ? delivered / total : 0)}</td>
          <td class="num">${r.avg_quality == null ? "-" : n(r.avg_quality, 1)}</td>
          <td>${shortTime(r.last_detected_at)}</td>
        </tr>`;
      }),
      "Нет инструментов в конфиге"
    );
  }

  async function pageSignals() {
    const q = {
      minutes: state.minutes,
      limit: 50,
      offset: state.offset,
      instrument_id: state.filters.instrument,
      signal_type: state.filters.type,
      delivery_status: state.filters.delivery,
      quality_min: state.filters.qmin,
      quality_max: state.filters.qmax,
      feedback: state.filters.feedback,
      severity: state.filters.severity,
    };
    const data = await api("/admin/api/signals" + params(q));
    const exportUrl = "/admin/api/signals/export.csv" + params({
      ...q,
      offset: "",
      limit: "",
    });
    view().innerHTML = `
      ${pageHead("Signals", "Плотная таблица всех сохранённых сигналов.", `<a href="${exportUrl}" target="_blank">Export CSV</a>`)}
      <section class="panel">
        ${filtersHtml()}
        ${table(signalHeaders(), (data.items || []).map(signalRow), "Нет сигналов под фильтрами")}
        <div class="pager">
          <button id="prevPage">Prev</button>
          <span class="mono">${n(state.offset + 1, 0)}-${n(Math.min(state.offset + 50, data.total || 0), 0)} / ${n(data.total, 0)}</span>
          <button id="nextPage">Next</button>
        </div>
      </section>
    `;
    bindFilters();
    document.getElementById("prevPage").onclick = () => {
      state.offset = Math.max(0, state.offset - 50);
      pageSignals().catch(showError);
    };
    document.getElementById("nextPage").onclick = () => {
      if (state.offset + 50 < Number(data.total || 0)) {
        state.offset += 50;
        pageSignals().catch(showError);
      }
    };
  }

  function filtersHtml() {
    return `
      <div class="filters">
        ${filterInput("instrument", "Instrument", "SBER_TQBR")}
        ${filterInput("type", "Signal type", "volume_spike")}
        <label>Delivery
          <select id="f_delivery">
            ${selectOption("", "any", state.filters.delivery)}
            ${selectOption("delivered", "delivered", state.filters.delivery)}
            ${selectOption("suppressed", "suppressed", state.filters.delivery)}
            ${selectOption("unknown", "unknown", state.filters.delivery)}
          </select>
        </label>
        ${filterInput("qmin", "Q min", "65")}
        ${filterInput("qmax", "Q max", "100")}
        <label>Feedback
          <select id="f_feedback">
            ${selectOption("", "any", state.filters.feedback)}
            ${selectOption("useful", "useful", state.filters.feedback)}
            ${selectOption("noise", "noise", state.filters.feedback)}
            ${selectOption("unsure", "unsure", state.filters.feedback)}
            ${selectOption("none", "none", state.filters.feedback)}
          </select>
        </label>
        <label>Severity
          <select id="f_severity">
            ${selectOption("", "any", state.filters.severity)}
            ${selectOption("1", "1", state.filters.severity)}
            ${selectOption("2", "2", state.filters.severity)}
            ${selectOption("3", "3", state.filters.severity)}
          </select>
        </label>
        <div class="row"><button id="applyFilters" class="primary">Apply</button><button id="clearFilters">Clear</button></div>
      </div>
    `;
  }

  function filterInput(id, label, ph) {
    return `<label>${esc(label)}<input id="f_${id}" value="${esc(state.filters[id])}" placeholder="${esc(ph)}" /></label>`;
  }

  function selectOption(value, label, current) {
    return `<option value="${esc(value)}" ${String(current) === String(value) ? "selected" : ""}>${esc(label)}</option>`;
  }

  function bindFilters() {
    document.getElementById("applyFilters").onclick = () => {
      Object.keys(state.filters).forEach((k) => {
        state.filters[k] = document.getElementById("f_" + k).value.trim();
      });
      state.offset = 0;
      pageSignals().catch(showError);
    };
    document.getElementById("clearFilters").onclick = () => {
      Object.keys(state.filters).forEach((k) => { state.filters[k] = ""; });
      state.offset = 0;
      pageSignals().catch(showError);
    };
  }

  async function pageDelivery() {
    const [overview, reasons, settings, simulation] = await Promise.all([
      api("/admin/api/delivery/overview" + params({ minutes: state.minutes })),
      api("/admin/api/delivery/reasons" + params({ minutes: state.minutes })),
      api("/admin/api/settings"),
      api("/admin/api/delivery/simulation", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ preset: "conservative", minutes: Number(state.minutes || 0), limit: 200 }),
      }),
    ]);
    rememberRuntime(settings);
    const t = overview.totals || {};
    view().innerHTML = `
      ${pageHead("Delivery", "Что ушло наружу, что подавлено и почему.")}
      ${metrics([
        { label: "Delivery rate", value: pct(t.delivery_rate), hint: n(t.delivered, 0) + " delivered" },
        { label: "Suppressed", value: n(t.suppressed, 0), hint: "stored, not sent" },
        { label: "Unknown", value: n(t.unknown, 0), hint: "old rows" },
        { label: "Total", value: n(t.total, 0), hint: activePeriodShort() },
      ])}
      ${signalCatalogPanel(settings.signals)}
      <div class="grid-2">
        <section class="panel">
          <div class="panel-head"><h2>Per Type</h2></div>
          ${deliveryTypeTable(overview.by_type || [])}
        </section>
        <section class="panel">
          <div class="panel-head"><h2>Reasons</h2></div>
          ${deliveryReasonTable(reasons.items || [])}
        </section>
      </div>
      <section class="panel">
        <div class="panel-head"><h2>Dry-run Simulation</h2><span class="muted">conservative preset, no Telegram impact</span></div>
        ${deliverySimulationPanel(simulation)}
      </section>
      <section class="panel">
        <div class="panel-head"><h2>Recent Delivered</h2></div>
        ${table(signalHeaders(), (overview.recent_delivered || []).map(signalRow), "Нет delivered сигналов")}
      </section>
    `;
  }

  function deliveryTypeTable(rows) {
    return table(
      [
        { label: "Type" },
        { label: "Total", cls: "num" },
        { label: "Delivered", cls: "num" },
        { label: "Suppressed", cls: "num" },
        { label: "Rate", cls: "num" },
        { label: "Avg q", cls: "num" },
      ],
      rows.map((r) => {
        const total = Number(r.total || 0);
        const delivered = Number(r.delivered || 0);
        return `<tr>
          <td class="clip">${esc(r.signal_type)}</td>
          <td class="num">${n(total, 0)}</td>
          <td class="num">${n(delivered, 0)}</td>
          <td class="num">${n(r.suppressed, 0)}</td>
          <td class="num">${pct(total ? delivered / total : 0)}</td>
          <td class="num">${n(r.avg_quality, 1)}</td>
        </tr>`;
      }),
      "Нет delivery статистики"
    );
  }

  function deliverySimulationPanel(data) {
    const rows = data || {};
    const changed = (rows.changed_sample || []).slice(0, 10).map((r) => `<tr>
      <td><strong>${esc(r.ticker)}</strong><div class="muted">${esc(r.instrument_id)}</div></td>
      <td class="clip">${esc(r.signal_type)}</td>
      <td>${deliveryBadge(r.current_delivery_status)}</td>
      <td>${deliveryBadge(r.simulated_delivery_status)}<div class="muted clip">${esc(r.simulated_delivery_reason)}</div></td>
      <td>${badge(r.simulated_delivery_channel || "admin_only")}</td>
    </tr>`);
    return `
      ${metrics([
        { label: "Sampled", value: n(rows.sampled, 0), hint: "stored rows" },
        { label: "Changed", value: n(rows.changed_count, 0), hint: "status delta" },
        { label: "Preset", value: esc(rows.preset || "current"), hint: "dry-run" },
        { label: "Window", value: esc(rows.minutes === 0 ? "all" : String(rows.minutes || state.minutes)), hint: "minutes" },
      ])}
      ${table(
        [
          { label: "Ticker" },
          { label: "Type" },
          { label: "Current" },
          { label: "Simulated" },
          { label: "Channel" },
        ],
        changed,
        "No status changes in the sampled set"
      )}
      <div class="grid-2">
        <section class="mini-panel">
          <h3>By Status</h3>
          ${barList((rows.by_status || []).map((r) => [r.key, r.count, Math.max(1, rows.sampled || 1)]))}
        </section>
        <section class="mini-panel">
          <h3>By Channel</h3>
          ${barList((rows.by_channel || []).map((r) => [r.key, r.count, Math.max(1, rows.sampled || 1)]))}
        </section>
      </div>
    `;
  }

  function deliveryReasonTable(rows) {
    return table(
      [
        { label: "Reason" },
        { label: "Status" },
        { label: "Type" },
        { label: "Count", cls: "num" },
        { label: "Avg q", cls: "num" },
      ],
      rows.map((r) => `<tr>
        <td class="clip">${esc(r.delivery_reason)}</td>
        <td>${deliveryBadge(r.delivery_status)}</td>
        <td class="clip">${esc(r.signal_type)}</td>
        <td class="num">${n(r.signal_count, 0)}</td>
        <td class="num">${n(r.avg_quality, 1)}</td>
      </tr>`),
      "Нет причин"
    );
  }

  function signalCatalogPanel(signals) {
    const all = (signals && signals.types) || [];
    const enabled = all.filter((r) => r.enabled);
    const source = signals && signals.source_coverage ? signals.source_coverage : {};
    const sourceHint = [
      `trade ${n(source.trade, 0)}`,
      `last ${n(source.last_price, 0)}`,
      `book ${n(source.orderbook, 0)}`,
      `info ${n(source.trading_status, 0)}`,
    ].join(" / ");
    return `<section class="panel">
      <div class="panel-head">
        <h2>Configured Detector Types</h2>
        <span class="muted">${n(enabled.length, 0)} active / ${n(all.length, 0)} known / ${esc(sourceHint)}</span>
      </div>
      ${signalCatalogTable(all)}
    </section>`;
  }

  function signalCatalogTable(rows) {
    const sorted = (rows || []).slice().sort((a, b) => {
      if (a.enabled !== b.enabled) return a.enabled ? -1 : 1;
      return String(a.signal_type || "").localeCompare(String(b.signal_type || ""));
    });
    return table(
      [
        { label: "Type" },
        { label: "State" },
        { label: "Source", cls: "num" },
        { label: "Config" },
        { label: "Delivery rule" },
      ],
      sorted.map((r) => {
        const cls = r.enabled ? "b-high" : r.reason === "source_not_subscribed" ? "b-medium" : "b-unknown";
        const state = r.enabled ? "active" : (r.reason || "disabled");
        return `<tr>
          <td class="clip">${esc(r.signal_type)}</td>
          <td>${badge(state, cls)}<div class="muted clip">${esc(r.scope || "")}</div></td>
          <td class="num">${n(r.source_coverage, 0)}<div class="muted clip">${esc(r.source || "")}</div></td>
          <td class="clip">${esc(r.config || "")}</td>
          <td class="clip">${esc(r.delivery_rule || "")}</td>
        </tr>`;
      }),
      "Нет каталога типов сигналов"
    );
  }

  async function pageCalibration() {
    const [data, settings] = await Promise.all([
      api("/admin/api/calibration" + params({ minutes: state.minutes })),
      api("/admin/api/settings"),
    ]);
    rememberRuntime(settings);
    const rows = data.items || [];
    view().innerHTML = `
      ${pageHead("Calibration", "Матрица качества, доставки и ручной разметки.")}
      ${signalCatalogPanel(settings.signals)}
      <section class="panel">
        <div class="panel-head"><h2>Signal Type Matrix</h2></div>
        ${calibrationTable(rows)}
      </section>
    `;
  }

  function calibrationTable(rows) {
    return table(
      [
        { label: "Type" },
        { label: "Tier" },
        { label: "Delivery" },
        { label: "Feedback" },
        { label: "Count", cls: "num" },
        { label: "Avg q", cls: "num" },
      ],
      rows.map((r) => `<tr>
        <td class="clip">${esc(r.signal_type)}</td>
        <td>${badge(r.quality_tier, r.quality_tier === "high" ? "b-high" : r.quality_tier === "medium" ? "b-medium" : "b-low")}</td>
        <td>${deliveryBadge(r.delivery_status)}</td>
        <td>${badge(r.feedback || "none")}</td>
        <td class="num">${n(r.signal_count, 0)}</td>
        <td class="num">${n(r.avg_quality, 1)}</td>
      </tr>`),
      "Нет данных для калибровки"
    );
  }

  async function pageFeedback() {
    const data = await api("/admin/api/feedback/overview" + params({ minutes: state.minutes }));
    const s = feedbackSummary(data);
    view().innerHTML = `
      ${pageHead("Feedback Quality", "Разметка useful/noise/unsure: где delivered-сигналы шумят, а suppressed могли быть полезными.")}
      ${metrics([
        { label: "Signals", value: n(s.total, 0), hint: activePeriodShort() },
        { label: "Labeled", value: n(s.labeled, 0), hint: pct(s.coverage_rate) },
        { label: "Useful", value: n(feedbackTotal(data.totals, "useful"), 0), hint: "manual" },
        { label: "Noise", value: n(feedbackTotal(data.totals, "noise"), 0), hint: "manual" },
      ])}
      <div class="grid-2">
        <section class="panel">
          <div class="panel-head"><h2>By Type</h2></div>
          ${feedbackByTypeTable(data.by_type || [])}
        </section>
        <section class="panel">
          <div class="panel-head"><h2>By Ticker</h2></div>
          ${feedbackByTickerTable(data.by_ticker || [])}
        </section>
      </div>
      <div class="grid-2">
        <section class="panel">
          <div class="panel-head"><h2>Delivered Marked Noise</h2></div>
          ${feedbackReasonTable(data.noise_delivered || [])}
        </section>
        <section class="panel">
          <div class="panel-head"><h2>Suppressed Marked Useful</h2></div>
          ${feedbackReasonTable(data.useful_suppressed || [])}
        </section>
      </div>
    `;
  }

  function feedbackSummary(data) {
    const raw = data && data.summary ? data.summary : {};
    const totals = feedbackRows(data && data.totals);
    const totalFromRows = totals.reduce((acc, r) => acc + finiteNumber(r.signal_count, 0), 0);
    const labeledFromRows = totals
      .filter((r) => (r.feedback || "none") !== "none")
      .reduce((acc, r) => acc + finiteNumber(r.signal_count, 0), 0);
    const total = finiteNumber(raw.total ?? raw.total_signals, totalFromRows);
    const labeled = finiteNumber(raw.labeled ?? raw.labeled_count, labeledFromRows);
    const coverage = total > 0 ? labeled / total : 0;
    return {
      total,
      labeled,
      coverage_rate: finiteNumber(raw.coverage_rate, coverage),
    };
  }

  function feedbackRows(rows) {
    if (Array.isArray(rows)) return rows;
    if (rows && typeof rows === "object") {
      return Object.entries(rows).map(([feedback, signal_count]) => ({
        feedback,
        signal_count,
      }));
    }
    return [];
  }

  function feedbackTotal(rows, label) {
    return feedbackRows(rows)
      .filter((r) => r.feedback === label)
      .reduce((acc, r) => acc + finiteNumber(r.signal_count, 0), 0);
  }

  function feedbackByTypeTable(rows) {
    return table(
      [
        { label: "Type" },
        { label: "Delivery" },
        { label: "Feedback" },
        { label: "Count", cls: "num" },
        { label: "Avg q", cls: "num" },
      ],
      rows.map((r) => `<tr>
        <td class="clip">${esc(r.signal_type)}</td>
        <td>${deliveryBadge(r.delivery_status)}</td>
        <td>${badge(r.feedback || "none")}</td>
        <td class="num">${n(r.signal_count, 0)}</td>
        <td class="num">${n(r.avg_quality, 1)}</td>
      </tr>`),
      "Пока нет feedback-разметки"
    );
  }

  function feedbackByTickerTable(rows) {
    return table(
      [
        { label: "Ticker" },
        { label: "Delivery" },
        { label: "Feedback" },
        { label: "Count", cls: "num" },
        { label: "Avg q", cls: "num" },
      ],
      rows.slice(0, 80).map((r) => `<tr>
        <td><strong>${esc(r.ticker)}</strong><div class="muted">${esc(r.instrument_id)}</div></td>
        <td>${deliveryBadge(r.delivery_status)}</td>
        <td>${badge(r.feedback || "none")}</td>
        <td class="num">${n(r.signal_count, 0)}</td>
        <td class="num">${n(r.avg_quality, 1)}</td>
      </tr>`),
      "Нет feedback по тикерам"
    );
  }

  function feedbackReasonTable(rows) {
    return table(
      [
        { label: "Type" },
        { label: "Reason" },
        { label: "Count", cls: "num" },
        { label: "Avg q", cls: "num" },
      ],
      rows.map((r) => `<tr>
        <td class="clip">${esc(r.signal_type)}</td>
        <td class="clip">${esc(r.delivery_reason)}</td>
        <td class="num">${n(r.signal_count, 0)}</td>
        <td class="num">${n(r.avg_quality, 1)}</td>
      </tr>`),
      "Нет таких меток"
    );
  }

  async function pageInstruments() {
    const [data, sourceHealth] = await Promise.all([
      api("/admin/api/instruments" + params({ minutes: state.minutes })),
      api("/admin/api/source-health" + params({ minutes: sourceHealthMinutes() })),
    ]);
    const healthById = {};
    (sourceHealth.items || []).forEach((row) => { healthById[row.instrument_id] = row; });
    const items = (data.items || []).map((row) => ({
      ...row,
      source_health: (healthById[row.instrument_id] || {}).source_health || {},
      signal_availability: (healthById[row.instrument_id] || {}).signal_availability || [],
    }));
    const totals = items.reduce((acc, row) => {
      acc.signals += Number(row.total || 0);
      acc.delivered += Number(row.delivered || 0);
      return acc;
    }, { signals: 0, delivered: 0 });
    const coverage = data.source_coverage || {};
    view().innerHTML = `
      ${pageHead("Instruments", "Тикеры с частотой сигналов, качеством и delivery rate.")}
      ${metrics([
        { label: "Configured", value: n(data.count, 0), hint: "instruments.yaml" },
        { label: "With signals", value: n(data.active_count, 0), hint: activePeriodShort() },
        { label: "Signals", value: n(totals.signals, 0), hint: "stored" },
        { label: "Delivered", value: n(totals.delivered, 0), hint: pct(totals.signals ? totals.delivered / totals.signals : 0) },
        { label: "Orderbook", value: n(coverage.orderbook, 0), hint: "L2 subscriptions" },
        { label: "Raw source", value: esc(sourceHealth.status || "unknown"), hint: n(sourceHealth.ok_source_count, 0) + " fresh" },
      ])}
      <section class="panel">
        <div class="panel-head"><h2>Configured Instruments</h2><span class="muted">Rows with 0 signals are still monitored</span></div>
        ${instrumentUniverseTable(items)}
      </section>
    `;
  }

  async function pageAccuracy() {
    try {
      const data = await api("/admin/api/accuracy");
      const summary = data.summary || {};
      view().innerHTML = accuracyHtml(data, summary);
      return;
      view().innerHTML = `
        ${pageHead("Accuracy", "Офлайн JSON из duckdb_label_signals.")}
        <section class="panel"><div class="panel-body"><pre class="json">${esc(JSON.stringify(data, null, 2))}</pre></div></section>
      `;
    } catch (err) {
      view().innerHTML = `
        ${pageHead("Accuracy", "Офлайн JSON из duckdb_label_signals.")}
        <div class="empty">${esc(err.message)}</div>
      `;
    }
  }

  function accuracyHtml(data, summary) {
    return `
      ${pageHead("Accuracy", "Offline DuckDB accuracy report from signal_accuracy.json.")}
      ${data.status === "missing" ? `<div class="empty">Accuracy JSON is not built yet: ${esc(data.path || "")}</div>` : ""}
      <section class="panel">
        <div class="panel-head"><h2>Forward Horizons</h2></div>
        ${accuracyHorizonTable(summary.horizons || [])}
      </section>
      <div class="grid-2">
        <section class="panel">
          <div class="panel-head"><h2>By Type</h2></div>
          ${accuracyMetricTable(summary.by_type || [], "signal_type")}
        </section>
        <section class="panel">
          <div class="panel-head"><h2>By Quality Tier</h2></div>
          ${accuracyMetricTable(summary.by_quality_tier || [], "quality_tier")}
        </section>
      </div>
      <div class="grid-2">
        <section class="panel">
          <div class="panel-head"><h2>Delivered vs Suppressed</h2></div>
          ${accuracyMetricTable(summary.by_delivery_status || [], "delivery_status")}
        </section>
        <section class="panel">
          <div class="panel-head"><h2>Noisiest Tickers</h2></div>
          ${accuracyMetricTable((summary.by_ticker || []).slice(0, 80), "ticker")}
        </section>
      </div>
      <section class="panel">
        <div class="panel-head"><h2>Raw JSON</h2></div>
        <div class="panel-body"><pre class="json">${esc(JSON.stringify(data.raw || {}, null, 2))}</pre></div>
      </section>
    `;
  }

  function accuracyHorizonTable(rows) {
    return table(
      [
        { label: "Horizon" },
        { label: "Hit-rate", cls: "num" },
        { label: "Hits", cls: "num" },
        { label: "Misses", cls: "num" },
        { label: "Decided", cls: "num" },
      ],
      rows.map((r) => `<tr>
        <td>${esc(r.horizon)}m</td>
        <td class="num">${r.directional_hit_rate == null ? "-" : pct(r.directional_hit_rate)}</td>
        <td class="num">${n(r.directional_hits, 0)}</td>
        <td class="num">${n(r.directional_misses, 0)}</td>
        <td class="num">${n(r.directional_decided, 0)}</td>
      </tr>`),
      "No accuracy horizons yet"
    );
  }

  function accuracyMetricTable(rows, key) {
    return table(
      [
        { label: key },
        { label: "H" },
        { label: "Count", cls: "num" },
        { label: "Hit-rate", cls: "num" },
        { label: "Median move", cls: "num" },
      ],
      rows.map((r) => `<tr>
        <td class="clip">${esc(r[key] || "unknown")}</td>
        <td>${esc(r.horizon || "")}</td>
        <td class="num">${n(r.signal_count, 0)}</td>
        <td class="num">${r.directional_hit_rate == null ? "-" : pct(r.directional_hit_rate)}</td>
        <td class="num">${r.median_forward_return_pct == null ? "-" : n(r.median_forward_return_pct, 3) + "%"}</td>
      </tr>`),
      "No grouped accuracy data"
    );
  }

  async function pageSettings() {
    const data = await api("/admin/api/settings");
    rememberRuntime(data);
    view().innerHTML = `
      ${pageHead("Settings", "Read-only runtime configuration.")}
      <div class="grid-3">
        ${settingsPanel("Delivery", data.delivery)}
        ${settingsPanel("Signals", data.signals)}
        ${settingsPanel("Paths", data.paths)}
        ${settingsPanel("Kafka", data.kafka)}
      </div>
    `;
  }

  function settingsPanel(title, obj) {
    return `<section class="panel">
      <div class="panel-head"><h2>${esc(title)}</h2></div>
      <div class="panel-body"><pre class="json">${esc(JSON.stringify(obj || {}, null, 2))}</pre></div>
    </section>`;
  }

  async function pageSignal(id) {
    if (!id) {
      view().innerHTML = `${pageHead("Signal", "")}<div class="empty">signal_id missing</div>`;
      return;
    }
    const row = await api("/admin/api/signal/" + encodeURIComponent(id));
    const p = row.payload || {};
    view().innerHTML = `
      ${pageHead(row.ticker + " / " + row.signal_type, row.instrument_id, `<a href="#/signals">Back to table</a>`)}
      <div class="detail-grid">
        <section class="panel">
          <div class="panel-head"><h2>Signal</h2><div class="row">${deliveryBadge(row.delivery_status || p.delivery_status)} ${qualityBadge(p.quality_score)} ${severityBadge(row.severity)}</div></div>
          <div class="panel-body stack">
            <div class="summary-text">${esc(row.summary)}</div>
            ${interpretationFacts(p)}
            <div class="grid-3">
              ${miniMetric("Detected", shortTime(row.detected_at))}
              ${miniMetric("z-score", n(row.z_score, 2))}
              ${miniMetric("Metric", n(row.metric_value, 4))}
            </div>
            <div class="row">
              ${p.terminal_url ? `<a class="badge" href="${esc(p.terminal_url)}" target="_blank">Terminal</a>` : ""}
              ${p.instrument_page_url ? `<a class="badge" href="${esc(p.instrument_page_url)}" target="_blank">Instrument</a>` : ""}
            </div>
          </div>
        </section>
        <section class="panel">
          <div class="panel-head"><h2>Delivery Decision</h2></div>
          <div class="panel-body stack">
            ${decisionLine("Status", row.delivery_status || p.delivery_status || "unknown")}
            ${decisionLine("Reason", row.delivery_reason || p.delivery_reason || "unknown")}
            ${decisionLine("Rule", p.delivery_rule || "unknown")}
            ${decisionLine("Policy", p.delivery_policy_version || "unknown")}
            ${decisionLine("Priority", p.delivery_priority || "unknown")}
            ${decisionLine("Channel", p.delivery_channel || "unknown")}
            ${decisionLine("Explanation", p.delivery_explanation_ru || "unknown")}
            ${decisionLine("Delivered at", p.delivered_at || "-")}
          </div>
        </section>
      </div>
      <div class="detail-grid">
        <section class="panel">
          <div class="panel-head"><h2>Payload</h2></div>
          <div class="panel-body"><pre class="json">${esc(JSON.stringify(p, null, 2))}</pre></div>
        </section>
        <section class="panel">
          <div class="panel-head"><h2>Feedback</h2></div>
          <div class="panel-body stack">
            <select id="fbLabel">
              ${selectOption("useful", "useful", row.admin_feedback_label || "")}
              ${selectOption("noise", "noise", row.admin_feedback_label || "")}
              ${selectOption("unsure", "unsure", row.admin_feedback_label || "")}
            </select>
            <textarea id="fbNote" placeholder="note">${esc(row.admin_feedback_note || "")}</textarea>
            <button id="saveFeedback" class="primary">Save feedback</button>
            <div class="muted">${row.admin_feedback_at ? "updated " + shortTime(row.admin_feedback_at) : "no feedback yet"}</div>
          </div>
        </section>
      </div>
    `;
    document.getElementById("saveFeedback").onclick = async () => {
      await api("/admin/api/feedback", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          signal_id: row.signal_id,
          label: document.getElementById("fbLabel").value,
          note: document.getElementById("fbNote").value,
        }),
      });
      await pageSignal(id);
    };
  }

  function miniMetric(label, value) {
    return `<div class="metric"><div class="label">${esc(label)}</div><div class="value">${esc(value)}</div></div>`;
  }

  function decisionLine(label, value) {
    return `<div class="split"><span class="muted">${esc(label)}</span><code>${esc(value)}</code></div>`;
  }

  function showError(err) {
    view().innerHTML = `<div class="error">${esc(err.message || err)}</div>`;
  }

  async function loadCurrent() {
    const r = route();
    const active = r.name === "signal" ? "signals" : (
      nav.some((x) => x[0] === r.name) ? r.name : "triage"
    );
    renderShell(active);
    if (!state.token) {
      authEmpty();
      return;
    }
    try {
      if (r.name === "signals") await pageSignals();
      else if (r.name === "delivery") await pageDelivery();
      else if (r.name === "calibration") await pageCalibration();
      else if (r.name === "feedback") await pageFeedback();
      else if (r.name === "instruments") await pageInstruments();
      else if (r.name === "accuracy") await pageAccuracy();
      else if (r.name === "settings") await pageSettings();
      else if (r.name === "signal") await pageSignal(r.params.get("id"));
      else await pageTriage();
    } catch (err) {
      showError(err);
    }
  }

  window.addEventListener("hashchange", () => {
    state.offset = 0;
    loadCurrent();
  });
  setInterval(() => {
    if (state.auto && state.token && route().name !== "signal") {
      loadCurrent();
    }
  }, 30000);
  loadCurrent();
})();
