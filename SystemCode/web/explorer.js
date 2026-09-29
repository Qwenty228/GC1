// Source explorer: the original data-dictionary view. On request only (never polled), it fetches
// recent records straight from /api/alpaca and /api/edgar and shows each record's raw JSON with the
// generated data dictionary. Nothing here is stored.

(function () {
  "use strict";

  const R = window.StockNewsRender;
  const $ = (id) => document.getElementById(id);
  let companiesLoaded = false;

  async function loadCompanies() {
    if (companiesLoaded) return;
    companiesLoaded = true;
    try {
      const response = await fetch("/api/companies");
      const map = await response.json();
      $("explorer-ticker").innerHTML = Object.entries(map).map(([ticker, info]) =>
        `<option value="${R.esc(ticker)}"${ticker === "NVDA" ? " selected" : ""}>${R.esc(ticker)} (${R.esc(info.name)})</option>`
      ).join("");
    } catch (error) {
      companiesLoaded = false;
      $("explorer-output").innerHTML = R.loadError("the company list", String(error));
    }
  }

  function fmt(value) {
    if (value === null || value === undefined) return "";
    return typeof value === "object" ? JSON.stringify(value) : String(value);
  }

  function dataDictionary(rows) {
    const body = rows.map((r) => `<tr><td class="mono">${R.esc(r.field)}</td><td class="mono">${R.esc(r.type)}</td>
      <td class="mono">${R.esc(fmt(r.example).slice(0, 300))}</td><td>${R.esc(r.description || "")}</td></tr>`).join("");
    return `<h4>Data dictionary (${rows.length} fields)</h4><div class="table-scroll"><table>
      <thead><tr><th scope="col">Field</th><th scope="col">Type</th><th scope="col">Example</th>
      <th scope="col">Description</th></tr></thead><tbody>${body}</tbody></table></div>`;
  }

  function records(list) {
    if (!list.length) return `<p class="muted">No records returned.</p>`;
    const items = list.map((rec, i) => {
      const title = rec.headline || rec.form || rec.accessionNumber || `record ${i + 1}`;
      const url = rec.url || rec.primary_doc_url || rec.filing_index_url;
      const open = R.safeUrl(url) ? ` (${R.extLink(url, "open original")})` : "";
      return `<details class="record"><summary>${R.esc(String(title))}${open}</summary>
        <pre>${R.esc(JSON.stringify(rec, null, 2))}</pre></details>`;
    }).join("");
    return `<h4>Raw records (${list.length})</h4>${items}`;
  }

  async function load(source) {
    const ticker = $("explorer-ticker").value || "NVDA";
    const limit = $("explorer-limit").value;
    const out = $("explorer-output");
    const label = source === "alpaca" ? "Alpaca news" : "SEC EDGAR 8-K filings";
    out.innerHTML = `<div class="state" role="status">Loading ${R.esc(label)} for ${R.esc(ticker)}…</div>`;
    try {
      const response = await fetch(`/api/${source}?ticker=${encodeURIComponent(ticker)}&limit=${encodeURIComponent(limit)}`);
      const data = await response.json();
      if (data.error) {
        out.innerHTML = R.loadError(label, data.error);
        return;
      }
      const tag = source === "alpaca"
        ? (data.live ? "live request to Alpaca" : "demo mode: synthetic sample data")
        : `${data.count} filings · CIK ${data.cik} · ${data.from_cache ? "from the local cache" : "fresh request to SEC"}`;
      out.innerHTML = `<section class="card explorer-section"><h3>${R.esc(label)} <span class="badge">${R.esc(tag)}</span></h3>
        ${dataDictionary(data.data_dict || [])}${records(data.records || [])}</section>`;
    } catch (error) {
      out.innerHTML = R.loadError(label, String(error));
    }
  }

  $("explorer-alpaca").addEventListener("click", () => load("alpaca"));
  $("explorer-edgar").addEventListener("click", () => load("edgar"));
  $("tab-explorer").addEventListener("click", loadCompanies);
  $("tab-explorer").addEventListener("focus", loadCompanies);
  if (!$("panel-explorer").hidden) loadCompanies();
})();
