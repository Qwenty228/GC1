// Event summaries: a list of grouped events and one event's detail, read from
// /api/events. Renders stored data only (no model calls). Every piece of
// article or model text is escaped, and only absolute http(s) links are
// rendered. The conflict notice is built from the API's stored review records.

(function () {
  "use strict";

  const $ = (id) => document.getElementById(id);
  const ICON_WARN = '<span class="icon" aria-hidden="true">⚠</span>';
  const ICON_INFO = '<span class="icon" aria-hidden="true">ℹ</span>';
  const ICON_OK = '<span class="icon" aria-hidden="true">✓</span>';

  function esc(s) {
    return String(s ?? "").replace(/[&<>"']/g, (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])
    );
  }

  // Only absolute http(s) URLs become links; anything else is shown as plain text.
  function safeUrl(u) {
    if (typeof u !== "string" || !u || /[\s<>"'`\\]/.test(u)) return null;
    try {
      const url = new URL(u);
      return url.protocol === "http:" || url.protocol === "https:" ? url.href : null;
    } catch (_) {
      return null;
    }
  }

  function link(url, text) {
    const href = safeUrl(url);
    if (!href) return esc(text);
    return `<a href="${esc(href)}" target="_blank" rel="noopener noreferrer nofollow">${esc(text)}</a>`;
  }

  const DATE_FMT = { year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
                     timeZoneName: "short" };
  function when(iso) {
    if (!iso) return "unknown time";
    const d = new Date(iso);
    if (isNaN(d)) return esc(iso);
    return `<time datetime="${esc(d.toISOString())}">${esc(d.toLocaleString(undefined, DATE_FMT))}</time>`;
  }

  function listNames(names) {
    if (names.length <= 1) return names.join("");
    return names.slice(0, -1).join(", ") + " and " + names[names.length - 1];
  }

  // --------------------------------------------------------------------- state
  const state = { sample: false };

  function readHash() {
    const params = new URLSearchParams(location.hash.replace(/^#/, ""));
    if (params.has("sample")) state.sample = params.get("sample") === "1";
    return params;
  }

  function hashFor(eventId) {
    const params = new URLSearchParams();
    if (eventId) params.set("event", eventId);
    else params.set("events", "");
    if (state.sample) params.set("sample", "1");
    return "#" + params.toString().replace("events=", "events");
  }

  async function getJson(url) {
    const res = await fetch(url, { headers: { Accept: "application/json" } });
    const data = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    if (!res.ok || data.error) throw new Error(data.error || `HTTP ${res.status}`);
    return data;
  }

  // --------------------------------------------------------------------- badges
  function conflictBadge(conflict) {
    if (!conflict) return "";
    switch (conflict.status) {
      case "under_review":
        return `<span class="flag flag-review">${ICON_WARN} Conflicting reports—under review</span>`;
      case "correction_pending":
        return `<span class="flag flag-review">${ICON_WARN} Grouping correction in progress</span>`;
      case "disputed":
        return `<span class="flag flag-disputed">${ICON_WARN} Sources disagree</span>`;
      default:
        return "";
    }
  }

  const STATE_TEXT = {
    published: "Summary published",
    awaiting_review: "Summary awaiting review because sources disagree.",
    pending: "Summary pending",
    failed: "Summary could not be generated yet",
    not_available: "No summary available",
    not_requested: "No summary yet",
  };

  // --------------------------------------------------------------------- list
  function renderCard(e) {
    const headline = e.summary?.headline || e.title;
    const extra = e.conflict && e.conflict.count > 1 ? ` <span class="muted">(${e.conflict.count} disputes)</span>` : "";
    const sample = e.is_sample ? ` <span class="badge sample">sample data</span>` : "";
    // While a review is open the update is blocked, not on its way: the conflict badge says so instead.
    const blocked = e.conflict && ["under_review", "correction_pending"].includes(e.conflict.status);
    const pending = e.update_pending && e.summary_state === "published" && !blocked
      ? ` <span class="flag flag-info">${ICON_INFO} Update pending</span>` : "";
    return `<li class="event-card">
      <a class="event-link" href="${esc(hashFor(e.event_id))}"><span class="event-title">${esc(headline)}</span></a>
      <div class="flags">${conflictBadge(e.conflict)}${extra}${pending}${sample}</div>
      <div class="meta">${esc(STATE_TEXT[e.summary_state] || e.summary_state)} · ${esc(e.documents)} source(s) ·
        last report ${when(e.last_source_time)}</div>
    </li>`;
  }

  async function showList() {
    const body = $("events-body");
    body.innerHTML = `<div class="hint">Loading events…</div>`;
    try {
      const data = await getJson(`/api/events?limit=50&include_sample=${state.sample}`);
      $("events-badge").textContent = `${data.events.length} event(s)${state.sample ? " · including sample data" : ""}`;
      body.innerHTML = data.events.length
        ? `<ul class="event-list">${data.events.map(renderCard).join("")}</ul>`
        : `<div class="hint">No events${state.sample ? "" : " (sample events are hidden; tick “Include sample events”)"}.</div>`;
    } catch (err) {
      body.innerHTML = `<div class="error">Could not load events: ${esc(err.message)}</div>`;
    }
    document.body.dataset.eventsReady = "list";
  }

  // --------------------------------------------------------------------- conflict notice
  function sourceLine(s) {
    const name = s.name || "Unnamed source";
    const title = s.title || "Original source";
    const linkHtml = safeUrl(s.url) ? link(s.url, title) : `${esc(title)} <span class="muted">(no valid link)</span>`;
    return `${linkHtml} — ${esc(name)}, published ${when(s.published_at)}`;
  }

  function claimText(claims) {
    if (claims.length === 1) return esc(claims[0].value);
    return claims.map((c) => `${esc(c.aspect)}: ${esc(c.value)}`).join("; ");
  }

  function quotesHtml(quotes) {
    return quotes.map((q) => `<blockquote class="quote">“${esc(q)}”</blockquote>`).join("");
  }

  const DISPUTE_STATUS = {
    under_review: "Under review.",
    correction_pending: "A reviewer decided this report describes a different event. The correction has not been completed yet.",
    disputed: "Review complete: the reviewer confirmed the sources disagree. The review does not establish which source is correct.",
    separated: "A reviewer decided this report describes a different event; it was moved to its own event.",
    cleared: "A reviewer decided these reports do not conflict.",
  };

  function renderDispute(d) {
    const parts = [];
    if (d.precision === "claim_pair") {
      parts.push(`<p class="dispute-topic"><strong>Sources disagree about the ${esc(listNames(d.topic))}.</strong></p>`);
    } else {
      parts.push(`<p class="dispute-topic"><strong>${esc(d.notice)}</strong></p>`);
    }
    const items = d.sources.map((s) => {
      const name = esc(s.name || "An unnamed source");
      const claim = s.claims.length ? `<p class="claim">${name} reports ${claimText(s.claims)}.</p>` : "";
      const quotes = s.quotes.length ? quotesHtml(s.quotes)
        : `<p class="muted">No quotation from this source was stored.</p>`;
      return `<li class="dispute-source">${claim}${quotes}<p class="source-line">Source: ${sourceLine(s)}</p></li>`;
    });
    if (d.unattributed_quotes.length) {
      items.push(`<li class="dispute-source"><p class="muted">Quoted from the event's earlier sources (the exact
        source could not be identified):</p>${quotesHtml(d.unattributed_quotes)}</li>`);
    }
    parts.push(`<ul class="dispute-sources">${items.join("")}</ul>`);
    const decided = d.outcome && d.outcome.final ? ` Decided ${when(d.outcome.decided_at)}.` : "";
    const statusText = d.review_status === "in_review"
      ? "Under review: more evidence is needed."
      : DISPUTE_STATUS[d.display_status] || "Under review.";
    parts.push(`<p class="dispute-meta">Conflict detected ${when(d.detected_at)}. ${esc(statusText)}${decided}
      <span class="muted">Review ${esc(d.review_id.slice(0, 8))}.</span></p>`);
    return `<div class="dispute">${parts.join("")}</div>`;
  }

  const NOTICE_TITLE = {
    under_review: "Conflicting reports—under review",
    correction_pending: "Grouping correction in progress",
    disputed: "Sources disagree",
  };

  function renderConflictNotice(detail) {
    const c = detail.conflict;
    if (!c) return "";
    const active = c.disputes.filter((d) => d.active);
    const history = c.disputes.filter((d) => !d.active);
    let html = "";
    if (active.length) {
      const summaryLine = detail.summary
        ? `<p class="notice-summary-time">The summary below was published ${when(detail.summary.published_at)}.` +
          (c.summary_not_updated_for_conflict
            ? " This summary has not yet been updated to reflect the disagreement." : "") + `</p>`
        : "";
      html += `<aside class="conflict-notice" role="note" aria-labelledby="conflict-title">
        <h4 id="conflict-title">${ICON_WARN} ${esc(NOTICE_TITLE[c.status] || "Sources disagree")}</h4>
        ${active.map(renderDispute).join("")}
        ${summaryLine}
      </aside>`;
    }
    if (c.pending_update) {
      const p = c.pending_update;
      const next = {
        queued: "an updated summary was queued",
        generating: "an updated summary is being prepared",
        failed: "an updated summary was requested but could not be generated yet",
        blocked_by_review: "an updated summary will be prepared once the remaining review(s) are complete",
      }[p.status] || "an updated summary is pending";
      const tail = detail.summary
        ? " It has not been published yet: the summary below does not include the reviewer's findings."
        : " It has not been published yet.";
      html += `<aside class="update-notice" role="note">${ICON_INFO} After the review decision on
        ${when(p.decided_at)}, ${esc(next)}.${esc(tail)}</aside>`;
    }
    if (history.length) {
      html += `<details class="review-history"><summary>${ICON_OK} Resolved reviews (${history.length})</summary>
        ${history.map(renderDispute).join("")}</details>`;
    }
    return html;
  }

  // --------------------------------------------------------------------- summary
  function citationsHtml(citations) {
    if (!citations.length) return "";
    const items = citations.map((c) =>
      `<li><q>${esc(c.quote)}</q> — ${link(c.source.url, c.source.name || c.source.title || "source")}</li>`).join("");
    return `<details class="cites"><summary>${citations.length} citation(s)</summary><ul>${items}</ul></details>`;
  }

  function renderSummary(detail) {
    const s = detail.summary;
    if (!s) {
      const text = STATE_TEXT[detail.summary_state] || detail.summary_state;
      return `<section class="summary empty" aria-label="Summary"><p><strong>${esc(text)}</strong></p></section>`;
    }
    const byKind = (k) => s.statements.filter((st) => st.kind === k);
    const headline = byKind("headline")[0];
    const overview = byKind("overview").map((st) => `<p>${esc(st.text)}</p>${citationsHtml(st.citations)}`).join("");
    const points = byKind("key_point")
      .map((st) => `<li>${esc(st.text)}${citationsHtml(st.citations)}</li>`).join("");
    const limits = (s.limitations || []).length
      ? `<p class="muted">Limitations: ${s.limitations.map(esc).join("; ")}</p>` : "";
    return `<section class="summary" aria-label="Summary">
      <h4>${esc(s.headline)}</h4>
      ${headline ? citationsHtml(headline.citations) : ""}
      <p class="meta">Summary version ${esc(s.version)} · published ${when(s.published_at)}</p>
      ${overview}
      ${points ? `<ul class="key-points">${points}</ul>` : ""}
      ${limits}
      <p class="muted small">${esc(s.note || "")}</p>
    </section>`;
  }

  function renderSources(detail) {
    // Flag only reports in a dispute that is still active (a reviewer may have cleared the others).
    const disputed = new Set((detail.conflict?.disputes || []).filter((d) => d.active)
      .flatMap((d) => d.sources.filter((s) => s.side === "incoming").map((s) => s.revision_id)));
    const items = detail.sources.map((s) => `<li>${sourceLine(s)}${disputed.has(s.revision_id)
      ? ` <span class="flag flag-review">${ICON_WARN} disputed report</span>` : ""}</li>`).join("");
    return `<section class="sources"><h4>Sources (${detail.sources.length})</h4><ul>${items}</ul></section>`;
  }

  async function showDetail(eventId) {
    const body = $("events-body");
    body.innerHTML = `<div class="hint">Loading event…</div>`;
    try {
      const d = await getJson(`/api/events/${encodeURIComponent(eventId)}?include_sample=${state.sample}`);
      $("events-badge").textContent = d.is_sample ? "sample data" : "";
      body.innerHTML = `<article class="event-detail">
        <p><a href="${esc(hashFor(null))}">← All events</a></p>
        <h3>${esc(d.title)}</h3>
        <p class="meta">${esc(d.documents)} source(s) · first report ${when(d.first_source_time)} ·
          last report ${when(d.last_source_time)}${d.is_sample ? ' · <span class="badge sample">sample data</span>' : ""}</p>
        ${renderConflictNotice(d)}
        ${renderSummary(d)}
        ${renderSources(d)}
      </article>`;
    } catch (err) {
      body.innerHTML = `<div class="error">Could not load event: ${esc(err.message)}</div>
        <p><a href="${esc(hashFor(null))}">← All events</a></p>`;
    }
    document.body.dataset.eventsReady = "detail";
  }

  function route() {
    const params = readHash();
    $("events-sample").checked = state.sample;
    const eventId = params.get("event");
    if (eventId) {
      $("sec-events").classList.add("open");
      showDetail(eventId);
      $("sec-events").scrollIntoView();
    } else if (params.has("events")) {
      $("sec-events").classList.add("open");
      showList();
    }
  }

  $("btn-events").addEventListener("click", () => {
    state.sample = $("events-sample").checked;
    const target = hashFor(null);
    if (location.hash === target) showList();
    else location.hash = target;
  });
  window.addEventListener("hashchange", route);
  route();
})();
