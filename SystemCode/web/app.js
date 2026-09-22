// Thin frontend: fetches from the FastAPI backend and renders the data
// dictionary + raw records. Contains no data logic of its own.

const $ = (id) => document.getElementById(id);

async function loadCompanies() {
  const res = await fetch("/api/companies");
  const map = await res.json();
  const sel = $("ticker");
  sel.innerHTML = "";
  for (const [ticker, info] of Object.entries(map)) {
    const opt = document.createElement("option");
    opt.value = ticker;
    opt.textContent = `${ticker} — ${info.name}`;
    sel.appendChild(opt);
  }
}

function renderDataDict(rows) {
  const head = `<tr><th>field</th><th>type</th><th>example</th><th>description</th></tr>`;
  const body = rows
    .map(
      (r) =>
        `<tr><td class="field">${esc(r.field)}</td>` +
        `<td class="type">${esc(r.type)}</td>` +
        `<td class="example">${esc(fmt(r.example))}</td>` +
        `<td>${esc(r.description || "")}</td></tr>`
    )
    .join("");
  return `<div class="panel-title">Data dictionary (${rows.length} fields)</div>
          <table>${head}${body}</table>`;
}

function renderRecords(records) {
  if (!records.length) return `<div class="hint">No records returned.</div>`;
  const items = records
    .map((rec, i) => {
      const title = rec.headline || rec.form || rec.accessionNumber || `record ${i + 1}`;
      const link = rec.url || rec.primary_doc_url || rec.filing_index_url;
      const linkHtml = link ? ` — <a href="${esc(link)}" target="_blank" rel="noopener">open</a>` : "";
      return `<details class="record"><summary>${esc(String(title))}${linkHtml}</summary>
              <pre>${esc(JSON.stringify(rec, null, 2))}</pre></details>`;
    })
    .join("");
  return `<div class="records"><div class="panel-title">Raw records (${records.length})</div>${items}</div>`;
}

async function load(source) {
  const ticker = $("ticker").value;
  const limit = $("limit").value;
  const bodyEl = $(`${source}-body`);
  const badgeEl = $(`${source}-badge`);
  const sectionEl = $(`sec-${source}`);
  sectionEl.classList.add("open");
  bodyEl.innerHTML = `<div class="hint">Loading ${source} data for ${ticker}…</div>`;
  badgeEl.className = "badge";
  badgeEl.textContent = "";

  try {
    const res = await fetch(`/api/${source}?ticker=${encodeURIComponent(ticker)}&limit=${limit}`);
    const data = await res.json();
    if (data.error) {
      bodyEl.innerHTML = `<div class="error">${esc(data.error)}</div>`;
      return;
    }
    if (source === "alpaca") {
      badgeEl.classList.add(data.live ? "live" : "sample");
      badgeEl.textContent = data.live ? "live" : "demo mode · synthetic sample data";
    } else {
      const cacheTag = data.from_cache ? "cached" : "fresh request";
      badgeEl.textContent = `${data.count} filings · CIK ${data.cik} · ${cacheTag}`;
    }
    bodyEl.innerHTML = renderDataDict(data.data_dict) + renderRecords(data.records);
  } catch (err) {
    bodyEl.innerHTML = `<div class="error">Request failed: ${esc(String(err))}</div>`;
  }
}

function fmt(v) {
  if (v === null || v === undefined) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}
function esc(s) {
  return String(s).replace(/[&<>"]/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c])
  );
}

$("btn-alpaca").addEventListener("click", () => load("alpaca"));
$("btn-edgar").addEventListener("click", () => load("edgar"));
loadCompanies();
