// Rendering for the Stock News page: pure functions from API data to HTML strings.
// Every piece of source, model or database text goes through esc(); only absolute
// http(s) URLs become links. Nothing here fetches data or calls a model. The
// conflict notice follows the stored review records exactly (see summary/conflicts.py).

(function (root) {
  "use strict";

  const ICONS = { warn: "⚠", info: "ℹ", ok: "✓", off: "■", error: "✕" };
  const icon = (name) => `<span class="icon" aria-hidden="true">${ICONS[name]}</span>`;

  function esc(s) {
    return String(s ?? "").replace(/[&<>"']/g, (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])
    );
  }

  // Only absolute http(s) URLs become links; anything else is shown as plain text.
  function safeUrl(u) {
    if (typeof u !== "string" || !u || u.length > 2048 || /[\s<>"'`\\\u0000-\u001f]/.test(u)) return null;
    try {
      const url = new URL(u);
      return (url.protocol === "http:" || url.protocol === "https:") && url.hostname ? url.href : null;
    } catch (_) {
      return null;
    }
  }

  function extLink(url, text) {
    const href = safeUrl(url);
    if (!href) return esc(text);
    return `<a href="${esc(href)}" target="_blank" rel="noopener noreferrer nofollow">${esc(text)}` +
      `<span class="sr-only"> (opens in a new tab)</span></a>`;
  }

  function host(url) {
    const href = safeUrl(url);
    return href ? new URL(href).hostname.replace(/^www\./, "") : "";
  }

  const DATE_FMT = { year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
                     timeZoneName: "short" };
  function when(iso) {
    if (!iso) return "unknown time";
    const d = new Date(iso);
    if (isNaN(d)) return esc(iso);
    return `<time datetime="${esc(d.toISOString())}">${esc(d.toLocaleString(undefined, DATE_FMT))}</time>`;
  }
  function clock(date) {
    return date ? esc(date.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" })) : "";
  }

  function plural(n, one, many) {
    return `${n} ${n === 1 ? one : many || one + "s"}`;
  }
  function listNames(names) {
    if (names.length <= 1) return names.join("");
    return names.slice(0, -1).join(", ") + " and " + names[names.length - 1];
  }
  // Alpaca reports some publishers in lower case ("benzinga"); show a capitalised name, never a different one.
  function publisher(name) {
    if (!name) return "Unknown publisher";
    return name === name.toLowerCase() ? name.charAt(0).toUpperCase() + name.slice(1) : name;
  }
  function paragraphs(text) {
    return String(text || "").split(/\n\s*\n/).map((p) => p.trim()).filter(Boolean)
      .map((p) => `<p>${esc(p)}</p>`).join("");
  }
  function notice(kind, title, body, iconName) {
    return `<div class="notice ${kind}" role="note"><h4>${icon(iconName || (kind === "info" ? "info" : "warn"))}` +
      `${esc(title)}</h4>${body}</div>`;
  }

  // ------------------------------------------------------------------ event list card
  const CONFLICT_BADGE = {
    under_review: "Conflicting reports—under review",
    correction_pending: "Grouping correction in progress",
    disputed: "Sources disagree",
  };

  function eventBadges(e, ctx) {
    const badges = [];
    if (e.is_sample) badges.push(`<span class="badge sample">Sample</span>`);
    if (ctx.isNew(e.event_id)) badges.push(`<span class="badge new">New</span>`);
    else if (ctx.isUpdated(e.event_id)) badges.push(`<span class="badge info">Updated</span>`);
    const status = e.conflict && e.conflict.status;
    if (CONFLICT_BADGE[status]) badges.push(`<span class="badge warn">${icon("warn")}${CONFLICT_BADGE[status]}</span>`);
    // While a review is open the update is blocked, not on its way: the conflict badge says so instead.
    const blocked = ["under_review", "correction_pending"].includes(status);
    if (e.update_pending && e.summary_state === "published" && !blocked) {
      badges.push(`<span class="badge info">${icon("info")}Newer summary queued</span>`);
    }
    return badges.length ? `<div class="badges">${badges.join("")}</div>` : "";
  }

  function eventHead(e, ctx) {
    const id = esc(e.event_id);
    return `<h3 class="card-title" id="event-${id}-title">${esc(e.title || "Untitled event")}</h3>
      ${eventBadges(e, ctx)}
      <p class="meta">${esc(plural(e.documents, "source"))}<span class="sep">·</span>Source times: first
        ${when(e.first_source_time)}, latest ${when(e.last_source_time)}</p>`;
  }

  // Shown above the summary on every card; the full claims and quotes are in the details.
  function eventConflict(e) {
    const c = e.conflict;
    if (!c) return "";
    const many = c.count > 1 ? ` <strong>${esc(c.count)} disputes</strong> are active on this event.` : "";
    const open = " Open the details to compare the competing claims and quotes.";
    switch (c.status) {
      case "under_review":
        return notice("warn", "Conflicting reports—under review", `<p>${e.summary
          ? "The summary below is the last published version. It stays visible, unchanged, until the review is complete."
          : "No summary is published while the review is open."}${many}${open}</p>`);
      case "correction_pending":
        return notice("warn", "Grouping correction in progress", `<p>A reviewer decided that a report in this event
          describes a different event, and the correction is not complete yet${e.summary
          ? ", so the summary below may still reflect that report" : ""}.${many}${open}</p>`);
      case "disputed":
        return notice("warn", "Sources disagree", `<p>A review confirmed that the sources disagree. The review does
          not establish which source is correct.${many}${open}</p>`);
      default:
        return "";
    }
  }

  function summaryStateNote(e, ctx) {
    let text;
    let cls = "";
    switch (e.summary_state) {
      case "awaiting_review":
        text = "No summary yet: the sources disagree, and a summary is published only after the review is complete.";
        break;
      case "pending":
        return `<p class="state-note">${icon("info")}Summary pending: a summary job is queued or in progress. Nothing
          has been published yet.${e.retry_after ? ` The next attempt is due after ${when(e.retry_after)}.` : ""}</p>`;
      case "failed":
        text = "Summary unavailable: generating it failed after repeated attempts. The sources are listed in the details.";
        cls = " failed";
        break;
      case "not_available":
        text = "No summary: this event has no text that can be summarized.";
        break;
      case "not_requested":
        text = ctx.summariesEnabled === false
          ? "No summary: event summaries are turned off (SUMMARY_ENABLED=false)."
          : "No summary has been requested for this event yet.";
        break;
      default:
        text = "Summary status unknown.";
    }
    return `<p class="state-note${cls}">${icon(cls ? "error" : "info")}${esc(text)}</p>`;
  }

  function eventSummary(e, ctx) {
    const s = e.summary;
    if (!s) return summaryStateNote(e, ctx);
    const points = (s.key_points || []).map((p) => (typeof p === "string" ? p : p && p.text)).filter(Boolean);
    return `<section class="summary" aria-label="AI-generated summary">
      <p class="summary-label">AI-generated summary · version ${esc(s.version)} · updated ${when(s.published_at)}</p>
      ${s.headline ? `<p class="summary-headline">${esc(s.headline)}</p>` : ""}
      ${paragraphs(s.overview)}
      ${points.length ? `<ul>${points.map((p) => `<li>${esc(p)}</li>`).join("")}</ul>` : ""}
      <p class="muted small">Written by a language model from the linked sources. The details show the quote each
        statement is based on.</p>
    </section>`;
  }

  // ------------------------------------------------------------------ event details: conflict notice
  function sourceLine(s) {
    const name = s.name ? publisher(s.name) : "Unnamed source";
    const title = s.title || "Original source";
    const linked = safeUrl(s.url) ? extLink(s.url, title) : `${esc(title)} <span class="muted">(no valid link)</span>`;
    return `${linked}: ${esc(name)}, published ${when(s.published_at)}`;
  }

  function claimText(claims) {
    if (claims.length === 1) return esc(claims[0].value);
    return claims.map((c) => `${esc(c.aspect)}: ${esc(c.value)}`).join("; ");
  }

  const quotes = (list) => list.map((q) => `<blockquote class="quote">“${esc(q)}”</blockquote>`).join("");

  const DISPUTE_STATUS = {
    under_review: "Under review.",
    correction_pending: "A reviewer decided this report describes a different event. The correction has not been completed yet.",
    disputed: "Review complete: the reviewer confirmed the sources disagree. The review does not establish which source is correct.",
    separated: "A reviewer decided this report describes a different event; it was moved to its own event.",
    cleared: "A reviewer decided these reports do not conflict.",
  };

  function dispute(d) {
    const parts = [d.precision === "claim_pair"
      ? `<p><strong>Sources disagree about the ${esc(listNames(d.topic))}.</strong></p>`
      : `<p><strong>${esc(d.notice)}</strong></p>`];
    const items = d.sources.map((s) => {
      const name = esc(s.name ? publisher(s.name) : "An unnamed source");
      const claim = s.claims.length ? `<p class="claim">${name} reports ${claimText(s.claims)}.</p>` : "";
      const q = s.quotes.length ? quotes(s.quotes) : `<p class="muted small">No quotation from this source was stored.</p>`;
      return `<li class="dispute-source">${claim}${q}<p class="small">Source: ${sourceLine(s)}</p></li>`;
    });
    if (d.unattributed_quotes.length) {
      items.push(`<li class="dispute-source"><p class="muted small">Quoted from the event's earlier sources (the exact
        source could not be identified):</p>${quotes(d.unattributed_quotes)}</li>`);
    }
    parts.push(`<ul class="dispute-sources">${items.join("")}</ul>`);
    const decided = d.outcome && d.outcome.final ? ` Decided ${when(d.outcome.decided_at)}.` : "";
    const status = d.review_status === "in_review"
      ? "Under review: more evidence is needed." : DISPUTE_STATUS[d.display_status] || "Under review.";
    parts.push(`<p class="small">Conflict detected ${when(d.detected_at)}. ${esc(status)}${decided}
      <span class="muted">Review ${esc(String(d.review_id).slice(0, 8))}.</span></p>`);
    return `<div class="dispute">${parts.join("")}</div>`;
  }

  const NOTICE_TITLE = {
    under_review: "Conflicting reports—under review",
    correction_pending: "Grouping correction in progress",
    disputed: "Sources disagree",
  };

  function conflictDetails(d) {
    const c = d.conflict;
    if (!c) return "";
    const active = c.disputes.filter((x) => x.active);
    let html = "";
    if (active.length) {
      const summaryLine = d.summary
        ? `<p class="small">The summary shown above was published ${when(d.summary.published_at)}.` +
          (c.summary_not_updated_for_conflict
            ? " This summary has not yet been updated to reflect the disagreement." : "") + "</p>"
        : "";
      html += notice("warn", `${NOTICE_TITLE[c.status] || "Sources disagree"}${active.length > 1
        ? ` (${active.length} disputes)` : ""}`, active.map(dispute).join("") + summaryLine);
    }
    if (c.pending_update) {
      const p = c.pending_update;
      const next = {
        queued: "an updated summary was queued",
        generating: "an updated summary is being prepared",
        failed: "an updated summary was requested but could not be generated yet",
        blocked_by_review: "an updated summary will be prepared once the remaining review(s) are complete",
      }[p.status] || "an updated summary is pending";
      const tail = d.summary
        ? " It has not been published yet: the summary shown above does not include the reviewer's findings."
        : " It has not been published yet.";
      html += notice("info", "Summary update after review", `<p>After the review decision on ${when(p.decided_at)},
        ${esc(next)}.${esc(tail)}</p>`);
    }
    return html;
  }

  function resolvedReviews(d) {
    const history = ((d.conflict && d.conflict.disputes) || []).filter((x) => !x.active);
    if (!history.length) return "";
    return `<details class="more detail-section" data-key="history"><summary>${icon("ok")}Resolved reviews
      (${history.length})</summary>${history.map(dispute).join("")}</details>`;
  }

  // ------------------------------------------------------------------ event details: evidence and sources
  const KIND = { headline: "Headline", overview: "Overview", key_point: "Key point" };
  const REASON = { initial: "first summary", new_information: "new information", review_resolution: "after a review",
                   manual_refresh: "manual refresh", conflict: "conflict" };

  function evidence(s) {
    const items = (s.statements || []).map((st) => {
      const cites = (st.citations || []).map((c) => `${quotes([c.quote])}<p class="quote-source">${
        sourceLine(c.source || {})}</p>`).join("");
      return `<li><p><span class="statement-kind">${esc(KIND[st.kind] || st.kind)}</span><br>${esc(st.text)}</p>
        ${cites || `<p class="muted small">No quotation is stored for this statement.</p>`}</li>`;
    }).join("");
    const limits = (s.limitations || []).length
      ? `<p class="muted small">Limitations recorded with this summary: ${s.limitations.map(esc).join("; ")}</p>` : "";
    return `<section class="detail-section"><h4>Evidence behind the summary (version ${esc(s.version)})</h4>
      <ol class="statements">${items}</ol>${limits}<p class="muted small">${esc(s.note || "")}</p></section>`;
  }

  function sourceKicker(s) {
    if (s.record_type === "sec_filing") {
      return `<span class="badge type-sec">SEC filing</span> ${esc([s.form, s.name].filter(Boolean).join(" · "))}`;
    }
    return `<span class="badge type-news">News</span> ${esc(publisher(s.name))} · via Alpaca`;
  }

  function sources(d) {
    const disputed = new Set(((d.conflict && d.conflict.disputes) || []).filter((x) => x.active)
      .flatMap((x) => x.sources.filter((s) => s.side === "incoming").map((s) => s.revision_id)));
    const items = (d.sources || []).map((s) => {
      const flags = [];
      if (s.cited_in_current_summary) flags.push(`<span class="badge">Cited in the summary</span>`);
      if (s.added_after_current_summary) flags.push(`<span class="badge info">Added after this summary version</span>`);
      if (disputed.has(s.revision_id)) flags.push(`<span class="badge warn">${icon("warn")}Disputed report</span>`);
      const title = s.title || "Original source";
      return `<li><p class="kicker">${sourceKicker(s)}</p>
        <div>${safeUrl(s.url) ? extLink(s.url, title) : `${esc(title)} <span class="muted">(no valid link)</span>`}</div>
        <p class="meta">Published ${when(s.published_at)}</p>
        ${flags.length ? `<div class="badges">${flags.join("")}</div>` : ""}</li>`;
    }).join("");
    return `<section class="detail-section"><h4>Sources (${esc((d.sources || []).length)})</h4>
      <ul class="source-list">${items}</ul></section>`;
  }

  function versions(d) {
    if (!(d.versions || []).length) return "";
    const items = d.versions.map((v) => `<li>Version ${esc(v.version)}: ${esc(REASON[v.generation_reason] ||
      String(v.generation_reason || "").replace(/_/g, " "))}, published ${when(v.published_at)}${v.status === "published"
      ? " <strong>(current)</strong>" : ` <span class="muted">(${esc(v.status)})</span>`}</li>`).join("");
    return `<details class="more detail-section" data-key="versions"><summary>Summary versions
      (${esc(d.versions.length)})</summary><ul class="versions">${items}</ul></details>`;
  }

  function eventDetails(d) {
    const pending = d.pending_relations > 0
      ? notice("info", "Possibly related reports", `<p>${esc(plural(d.pending_relations, "report"))} might describe
        this event, but grouping could not confirm it, so ${d.pending_relations === 1 ? "it is" : "they are"} not
        included here or in the summary.</p>`) : "";
    return conflictDetails(d) + pending + (d.summary ? evidence(d.summary) : "") + sources(d) + versions(d) +
      resolvedReviews(d);
  }

  // ------------------------------------------------------------------ article card
  function groupingLine(a, ctx) {
    if (a.events && a.events.length) {
      const links = a.events.map((ev) => `<a href="#event=${encodeURIComponent(ev.event_id)}" data-event-link="${
        esc(ev.event_id)}">${esc(ev.title || "Untitled event")}</a>`).join(", ");
      return `<p class="small">Part of event: ${links}</p>`;
    }
    const text = {
      completed: "Grouped, but not part of a current event.",
      partial: "Not grouped yet: grouping analysis is incomplete and pending a retry.",
      pending: "Not grouped yet: grouping analysis is pending a retry.",
      failed: "Not grouped: grouping analysis failed for this record.",
      skipped: "Not grouped: this record has no text that can be compared.",
    }[a.grouping_status] || (ctx.groupingEnabled === false
      ? "Not grouped: event grouping is turned off." : "Not grouped into an event.");
    return `<p class="small muted">${esc(text)}</p>`;
  }

  function articleCard(a, ctx) {
    const isSec = a.record_type === "sec_filing";
    const badges = [];
    if (a.is_sample) badges.push(`<span class="badge sample">Sample</span>`);
    if (ctx.isNew(a.document_id)) badges.push(`<span class="badge new">New</span>`);
    const kicker = isSec
      ? `<span class="badge type-sec">SEC filing</span><span>${esc(a.form || "Filing")} · ${esc(a.issuer ||
          "Unknown issuer")} · from SEC EDGAR</span>`
      : `<span class="badge type-news">News</span><span>${esc(publisher(a.publisher))} · via Alpaca</span>`;
    const meta = [];
    if (isSec) {
      if (a.filing_date) meta.push(`Filed ${esc(a.filing_date)}`);
      if (a.published_at) meta.push(`accepted ${when(a.published_at)}`);
      if ((a.item_codes || []).length) meta.push(`Items ${esc(a.item_codes.join(", "))}`);
    } else {
      meta.push(`Published ${when(a.published_at || a.source_time)}`);
      const updated = a.source_updated_at && a.published_at &&
        Math.abs(new Date(a.source_updated_at) - new Date(a.published_at)) > 60000;
      if (updated) meta.push(`updated by the source ${when(a.source_updated_at)}`);
    }
    meta.push(`retrieved ${when(a.retrieved_at)}`);
    if (a.revisions > 1) meta.push(`${esc(a.revisions)} stored revisions`);
    let excerpt = "";
    if (!isSec) {
      excerpt = a.excerpt
        ? `<blockquote class="excerpt"><p class="excerpt-label">Excerpt supplied with the article (not AI-generated)</p>
            <p>${esc(a.excerpt)}${a.excerpt_truncated ? "…" : ""}</p></blockquote>`
        : `<p class="small muted">No excerpt was supplied with this article.</p>`;
    }
    const links = [];
    if (safeUrl(a.url)) {
      links.push(extLink(a.url, isSec ? "Open the filing on sec.gov" : `Read the original on ${host(a.url)}`));
    } else {
      links.push(`<span class="muted">Original link unavailable (not a valid web address).</span>`);
    }
    if (isSec && safeUrl(a.filing_index_url)) links.push(extLink(a.filing_index_url, "Filing index"));
    return `<p class="kicker">${kicker}</p>
      <h3 class="card-title">${esc(a.title || "Untitled record")}</h3>
      ${badges.length ? `<div class="badges">${badges.join("")}</div>` : ""}
      <p class="meta">${meta.join('<span class="sep">·</span>')}</p>
      ${excerpt}
      <p class="links">${links.join("")}</p>
      ${groupingLine(a, ctx)}`;
  }

  // ------------------------------------------------------------------ header and empty states
  function collectorStatus(status) {
    const c = status && status.collector;
    if (!c) return { cls: "status-off", html: `${icon("off")}Collector status unavailable` };
    switch (c.state) {
      case "running": {
        const parts = [];
        let secFailed = false;
        if (c.alpaca && c.alpaca.enabled) {
          parts.push(c.alpaca.stream_connected ? "Alpaca stream connected"
            : "Alpaca stream reconnecting" + (c.alpaca.disconnected_at ? ` (since ${when(c.alpaca.disconnected_at)})` : ""));
        }
        if (c.sec && c.sec.enabled) {
          const latest = (times) => Object.values(times || {}).filter(Boolean)
            .sort((x, y) => new Date(x) - new Date(y)).pop();
          const ok = latest(c.sec.last_poll_ok_at);
          const bad = latest(c.sec.last_poll_error_at);
          secFailed = Boolean(bad && (!ok || new Date(bad) > new Date(ok)));
          parts.push(secFailed ? `SEC check failed ${when(bad)}` : ok ? `SEC checked ${when(ok)}` : "SEC first check pending");
        }
        const degraded = (c.alpaca && c.alpaca.enabled && !c.alpaca.stream_connected) || secFailed;
        return { cls: degraded ? "status-warn" : "status-ok",
                 html: `${icon(degraded ? "warn" : "ok")}Collectors running${parts.length ? ": " + parts.join(" · ") : ""}` };
      }
      case "starting":
        // Preparing (database, embedding model, ledger): nothing is being collected yet.
        return { cls: "status-warn", html: `${icon("info")}Collectors starting${c.stage ? `: ${esc(c.stage)}` : ""}
          (since ${when(c.stage_started_at || c.started_at)}); not collecting yet` };
      case "stale":
        return { cls: "status-warn", html: `${icon("warn")}Collector status unknown: no heartbeat since ${when(c.updated_at)}` };
      case "stopped":
        return { cls: "status-off", html: c.stage
          ? `${icon("off")}Collectors stopped during startup (${esc(c.stage)}) ${when(c.updated_at)}`
          : `${icon("off")}Collectors stopped ${when(c.updated_at)}` };
      case "not_running":
        return { cls: "status-off", html: `${icon("off")}Collectors not running` };
      default:
        return { cls: "status-off", html: `${icon("off")}Collector status unavailable` };
    }
  }

  function processingStatus(status) {
    if (!status) return "";
    const c = status.collector || {};
    const running = c.state === "running" && c.processing;
    const grouping = running ? c.processing.similarity : status.features && status.features.similarity_enabled;
    const summaries = running ? c.processing.summaries : status.features && status.features.summary_enabled;
    const word = (v) => (v === true ? "on" : v === false ? "off" : "unknown");
    return `Event grouping ${word(grouping)} · AI summaries ${word(summaries)}${running ? "" : " (as configured)"}`;
  }

  function eventsEmpty(status, sample) {
    const counts = status && status.counts;
    const sampleHint = sample ? "" : `<p>Sample events are hidden. Tick <strong>Include sample data</strong> to show
      the fictional demo event.</p>`;
    if (counts && counts.articles > 0 && counts.events === 0) {
      const grouping = counts.grouping || {};
      const waiting = (grouping.pending || 0) + (grouping.partial || 0);
      return `<div class="state"><h3>No events yet</h3>
        <p>${esc(plural(counts.articles, "stored record"))} (${esc(counts.news)} news, ${esc(counts.sec)} SEC) ${
          counts.articles === 1 ? "has" : "have"} not been grouped into events${waiting
          ? `; grouping analysis is pending for ${esc(waiting)}` : ""}. The <strong>All articles</strong> tab lists them.</p>
        <p>New records are grouped as the ingestion worker stores them (when event grouping is on). Earlier records with
          pending analysis are retried with <code>uv run datasources-similarity retry</code> while the worker is
          stopped; that can make paid model calls.</p>${sampleHint}</div>`;
    }
    if (counts && counts.articles === 0) {
      return `<div class="state"><h3>Nothing stored yet</h3><p>Articles and events appear here once the collectors
        store them.</p>${sampleHint}</div>`;
    }
    return `<div class="state"><h3>No events match</h3><p>No events for this company.</p>${sampleHint}</div>`;
  }

  function articlesEmpty(source, sample) {
    const what = { news: "news articles", sec: "SEC filings" }[source] || "records";
    return `<div class="state"><h3>No stored ${esc(what)}</h3><p>Nothing matches the current filters.</p>${sample ? ""
      : "<p>Sample records are hidden unless <strong>Include sample data</strong> is ticked.</p>"}</div>`;
  }

  function loadError(what, message) {
    return `<div class="state error" role="alert"><h3>${icon("error")}Could not load ${esc(what)}</h3>
      <p>${esc(message)}</p><p>The page retries automatically.</p></div>`;
  }

  const api = { esc, safeUrl, extLink, when, clock, plural, eventHead, eventConflict, eventSummary, eventDetails,
                articleCard, collectorStatus, processingStatus, eventsEmpty, articlesEmpty, loadError, icon };
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  else root.StockNewsRender = api;
})(this);
