// Rendering for the Stock News pages: pure functions from API data to HTML strings.
// Every piece of source, model or database text goes through esc(); only absolute
// http(s) URLs become links. Nothing here fetches data or calls a model. A card's
// preview is cut from the stored summary at a sentence boundary (or is its stored
// key points); no text is rewritten or added. The conflict notice follows the stored
// review records exactly (see summary/conflicts.py).

(function (root) {
  "use strict";

  // One icon family for every page: 24px stroke icons (shapes from Lucide, ISC licence), sized and coloured by CSS.
  // An icon is always decorative (aria-hidden) and sits next to text that carries the meaning.
  const ICONS = {
    warn: '<path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4"/><path d="M12 17h.01"/>',
    info: '<circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/>',
    ok: '<circle cx="12" cy="12" r="10"/><path d="m9 12 2 2 4-4"/>',
    off: '<circle cx="12" cy="12" r="10"/><rect x="9" y="9" width="6" height="6" rx="1"/>',
    error: '<circle cx="12" cy="12" r="10"/><path d="m15 9-6 6"/><path d="m9 9 6 6"/>',
    skip: '<circle cx="12" cy="12" r="10"/><path d="M8 12h8"/>',
    help: '<circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/><path d="M12 17h.01"/>',
    check: '<path d="M20 6 9 17l-5-5"/>',
    x: '<path d="M18 6 6 18"/><path d="m6 6 12 12"/>',
    minus: '<path d="M5 12h14"/>',
    square: '<rect x="6" y="6" width="12" height="12" rx="2"/>',
    dot: '<circle cx="12" cy="12" r="3"/>',
    star: '<path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01z"/>',
    ai: '<path d="M9.94 15.5A2 2 0 0 0 8.5 14.06l-6.14-1.58a.5.5 0 0 1 0-.96L8.5 9.94A2 2 0 0 0 9.94 8.5l1.58-6.14a.5.5 0 0 1 .96 0l1.58 6.14a2 2 0 0 0 1.44 1.44l6.14 1.58a.5.5 0 0 1 0 .96l-6.14 1.58a2 2 0 0 0-1.44 1.44l-1.58 6.14a.5.5 0 0 1-.96 0z"/><path d="M20 3v4"/><path d="M22 5h-4"/>',
    arrowRight: '<path d="M5 12h14"/><path d="m12 5 7 7-7 7"/>',
    arrowLeft: '<path d="m12 19-7-7 7-7"/><path d="M19 12H5"/>',
    chevronDown: '<path d="m6 9 6 6 6-6"/>',
  };
  const svg = (name) => `<svg viewBox="0 0 24 24" focusable="false">${ICONS[name] || ICONS.dot}</svg>`;
  const icon = (name, cls) =>
    `<span class="icon${cls ? ` ${cls}` : ""}" data-icon="${name}" aria-hidden="true">${svg(name)}</span>`;

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

  // Exact times (sources, quotes, history) carry the time zone; the card shows a short form with the exact
  // time as its tooltip.
  const DATE_FMT = { year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
                     timeZoneName: "short" };
  const SHORT_FMT = { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" };
  function when(iso) {
    if (!iso) return "unknown time";
    const d = new Date(iso);
    if (isNaN(d)) return esc(iso);
    return `<time datetime="${esc(d.toISOString())}">${esc(d.toLocaleString(undefined, DATE_FMT))}</time>`;
  }
  function shortWhen(iso) {
    const d = new Date(iso);
    if (!iso || isNaN(d)) return "";
    const fmt = d.getFullYear() === new Date().getFullYear() ? SHORT_FMT : { ...SHORT_FMT, year: "numeric" };
    return `<time datetime="${esc(d.toISOString())}" title="${esc(d.toLocaleString(undefined, DATE_FMT))}">${
      esc(d.toLocaleString(undefined, fmt))}</time>`;
  }
  function clock(date) {
    return date ? esc(date.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" })) : "";
  }
  function ago(date, now) {
    const minutes = Math.floor((now - date) / 60000);
    if (minutes < 1) return "just now";
    if (minutes < 60) return `${plural(minutes, "minute")} ago`;
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `${plural(hours, "hour")} ago`;
    return `on ${date.toLocaleString(undefined, SHORT_FMT)}`;
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

  // ------------------------------------------------------------------ summary preview
  // Where sentences end in stored text. A full stop after an initial or a common abbreviation ("U.S.", "Inc.",
  // "p.m.", "Aug.") is not an end. Only used to choose where a preview stops.
  const ABBREVIATION = new RegExp("(?:\\b[A-Z]|\\b(?:Inc|Corp|Co|Cos|Ltd|Plc|Jr|Sr|Mr|Mrs|Ms|Dr|Prof|St|Ave|vs|" +
    "No|Nos|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec|approx|est|[ap]\\.m|e\\.g|i\\.e))\\.$");
  const SENTENCE_END = "([.!?])[\"'”’)\\]]*\\s+(?=[\"'“‘(\\[]?[A-Z0-9$€£¥])|[。！？]";

  function sentenceEnds(text) {
    const ends = [];
    const re = new RegExp(SENTENCE_END, "g");
    let m;
    while ((m = re.exec(text))) {
      if (m[1] === "." && ABBREVIATION.test(text.slice(0, m.index + 1))) continue;
      ends.push(m.index + m[0].trimEnd().length);
    }
    return ends;
  }

  const PREVIEW_CHARS = 280; // whole sentences up to about this length (always at least one)
  const BULLET_CHARS = 180;  // stored key points this short can stand alone as preview bullets

  function keyPoints(s) {
    return (s.key_points || []).map((p) => (typeof p === "string" ? p : p && p.text))
      .map((p) => String(p || "").trim()).filter(Boolean);
  }

  // {html, complete}: two or three stored key points when they are short, otherwise the overview's opening
  // sentences. `complete` means the preview already shows the whole summary.
  function preview(s) {
    const overview = String(s.overview || "").trim();
    const points = keyPoints(s);
    const lead = points.slice(0, 3);
    if (lead.length && (!overview || (lead.length >= 2 && lead.every((p) => p.length <= BULLET_CHARS)))) {
      return { html: `<ul class="preview-points">${lead.map((p) => `<li>${esc(p)}</li>`).join("")}</ul>`,
               complete: !overview && points.length === lead.length };
    }
    const first = overview.split(/\n\s*\n/)[0];
    const ends = [...sentenceEnds(first), first.length];
    const fit = ends.filter((end) => end <= PREVIEW_CHARS);
    const cut = fit.length ? fit[fit.length - 1] : ends[0];
    return { html: `<p>${esc(first.slice(0, cut).trim())}</p>`, complete: cut >= overview.length && !points.length };
  }

  // ------------------------------------------------------------------ news card
  // Ticker chips; companies the reader follows come first and are marked with a star and in words.
  function tickerChips(e, ctx, max) {
    const followed = new Set();
    const names = {};
    for (const c of e.companies || []) {
      for (const s of c.symbols || []) {
        names[s] = c.name;
        if (ctx.followed && ctx.followed.has(c.cik)) followed.add(s);
      }
    }
    const base = (e.symbols || []).length ? e.symbols : ctx.company ? [ctx.company] : [];
    const symbols = [...base].sort((a, b) => Number(followed.has(b)) - Number(followed.has(a)));
    const chip = (s) => followed.has(s)
      ? `<span class="ticker followed" title="${esc(`You follow ${names[s] || s}`)}">${icon("star")}${
          esc(s)}<span class="sr-only"> (you follow this company)</span></span>`
      : `<span class="ticker"${names[s] ? ` title="${esc(names[s])}"` : ""}>${esc(s)}</span>`;
    return symbols.slice(0, max).map(chip).join("") +
      (symbols.length > max ? `<span class="ticker more">+${esc(symbols.length - max)}</span>` : "");
  }

  function cardHead(e, ctx) {
    const id = esc(e.event_id);
    const headline = (e.summary && e.summary.headline) || e.title || "Untitled story";
    const tickers = tickerChips(e, ctx, 3);
    const time = e.last_source_time
      ? `<span>${e.documents > 1 ? "Latest report" : "Published"} ${shortWhen(e.last_source_time)}</span>` : "";
    const sample = e.is_sample ? `<span class="badge sample">Sample</span>` : "";
    const meta = [tickers && `<span class="tickers">${tickers}</span>`, time, sample].filter(Boolean)
      .join(`<span class="sep" aria-hidden="true">·</span>`);
    return `<h3 class="card-title" id="event-${id}-title">${esc(headline)}</h3>
      ${meta ? `<p class="card-meta">${meta}</p>` : ""}`;
  }

  const CONFLICT_LABEL = {
    under_review: "Conflicting reports—under review",
    correction_pending: "Grouping correction in progress",
    disputed: "Sources disagree",
  };

  // A compact warning on the collapsed card; its button opens the Sources panel with the competing claims.
  function conflictLine(e) {
    const c = e.conflict;
    const label = c && CONFLICT_LABEL[c.status];
    if (!label) return "";
    const action = c.status === "correction_pending" ? "See details" : "Compare the claims";
    return `<p class="conflict-line">${icon("warn")}<strong>${label}</strong>${c.count > 1
      ? ` <span>(${esc(c.count)} disputes)</span>` : ""}<button type="button" class="inline-toggle" data-action="sources"
      data-focus="conflict" aria-expanded="false" aria-controls="event-${esc(e.event_id)}-sources"
      aria-describedby="event-${esc(e.event_id)}-title">${action}</button></p>`;
  }

  const NO_SUMMARY = {
    pending: "Preparing summary…",
    awaiting_review: "The summary is on hold until the conflicting reports are reviewed.",
    failed: "A summary could not be prepared for this story.",
    not_available: "This story has no text that can be summarized.",
  };

  // No AI summary yet: a short status and the most recent original source. Nothing here is labelled as AI text.
  function noSummary(e, ctx) {
    const text = NO_SUMMARY[e.summary_state] ||
      (ctx.summariesEnabled === false ? "AI summaries are turned off." : "No summary yet.");
    const src = e.latest_source;
    let link = "";
    if (src && safeUrl(src.url)) {
      const what = src.record_type === "sec_filing" ? "Original filing" : "Original article";
      link = `<p class="original-link">${what}: ${extLink(src.url, src.title || host(src.url))}${src.name
        ? ` <span class="muted">· ${esc(publisher(src.name))}</span>` : ""}</p>`;
    }
    return `<p class="summary-pending">${esc(text)}</p>${link}`;
  }

  function fullSummary(e) {
    const s = e.summary;
    const points = keyPoints(s);
    const blocked = e.conflict && ["under_review", "correction_pending"].includes(e.conflict.status);
    return `<p class="ai-label">${icon("ai")}AI summary</p>${paragraphs(s.overview)}
      ${points.length ? `<ul class="summary-points">${points.map((p) => `<li>${esc(p)}</li>`).join("")}</ul>` : ""}
      <p class="summary-note">Written by AI from the sources listed under Sources${s.published_at
        ? `, ${shortWhen(s.published_at)}` : ""}. Check the originals before relying on it.</p>
      ${e.update_pending && !blocked ? `<p class="summary-note">An updated summary is being prepared.</p>` : ""}`;
  }

  // {preview, full, more}: what the collapsed card shows, the complete stored summary for "Read more", and
  // whether "Read more" would show anything the preview does not.
  function summaryView(e, ctx) {
    if (!e.summary) return { preview: noSummary(e, ctx), full: "", more: false };
    const p = preview(e.summary);
    return { preview: `<p class="ai-label">${icon("ai")}AI summary</p>${p.html}`, full: fullSummary(e), more: !p.complete };
  }

  // ------------------------------------------------------------------ sources panel: conflict notice
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
      html += notice("warn", `${CONFLICT_LABEL[c.status] || "Sources disagree"}${active.length > 1
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

  // ------------------------------------------------------------------ sources panel: sources, quotes, history
  const KIND = { headline: "Headline", overview: "Overview", key_point: "Key point" };
  const REASON = { initial: "first summary", new_information: "new information", review_resolution: "after a review",
                   manual_refresh: "manual refresh", conflict: "conflict" };

  function sourceList(d) {
    const disputed = new Set(((d.conflict && d.conflict.disputes) || []).filter((x) => x.active)
      .flatMap((x) => x.sources.filter((s) => s.side === "incoming").map((s) => s.revision_id)));
    const items = (d.sources || []).map((s) => {
      const isSec = s.record_type === "sec_filing";
      const name = isSec ? [s.name || "SEC filing", s.form].filter(Boolean).join(" · ") : publisher(s.name);
      const title = s.title || "Original source";
      const flags = [];
      if (s.cited_in_current_summary) flags.push(`<span class="badge">Cited in the summary</span>`);
      if (s.added_after_current_summary) flags.push(`<span class="badge info">Newer than the summary</span>`);
      if (disputed.has(s.revision_id)) flags.push(`<span class="badge warn">${icon("warn")}Disputed report</span>`);
      return `<li><p class="source-name"><strong>${esc(name)}</strong> ${isSec
          ? `<span class="badge type-sec">SEC filing</span>` : `<span class="muted small">via Alpaca</span>`}</p>
        <p class="source-title">${safeUrl(s.url) ? extLink(s.url, title)
          : `${esc(title)} <span class="muted">(no valid link)</span>`}</p>
        <p class="meta">${isSec ? "Filed" : "Published"} ${when(s.published_at)}</p>
        ${flags.length ? `<div class="badges">${flags.join("")}</div>` : ""}</li>`;
    }).join("");
    return `<section class="detail-section"><h4>Original sources (${esc((d.sources || []).length)})</h4>
      <ul class="source-list">${items}</ul></section>`;
  }

  function evidence(s) {
    if (!s) return "";
    const items = (s.statements || []).map((st) => {
      const cites = (st.citations || []).map((c) => `${quotes([c.quote])}<p class="quote-source">${
        sourceLine(c.source || {})}</p>`).join("");
      return `<li><p><span class="statement-kind">${esc(KIND[st.kind] || st.kind)}</span><br>${esc(st.text)}</p>
        ${cites || `<p class="muted small">No quotation is stored for this statement.</p>`}</li>`;
    }).join("");
    const limits = (s.limitations || []).length
      ? `<p class="muted small">Limitations recorded with this summary: ${s.limitations.map(esc).join("; ")}</p>` : "";
    return `<section class="detail-section"><h4>Supporting quotes</h4>
      <p class="muted small">Each statement in the AI summary and the passage from the source it is based on.</p>
      <ol class="statements">${items}</ol>${limits}</section>`;
  }

  function related(d) {
    if (!(d.pending_relations > 0)) return "";
    return `<p class="muted small">${esc(plural(d.pending_relations, "more report"))} might be about this story, but
      grouping could not confirm it, so ${d.pending_relations === 1 ? "it is" : "they are"} not included here or in
      the summary.</p>`;
  }

  function versions(d) {
    if (!(d.versions || []).length) return "";
    const items = d.versions.map((v) => `<li>Version ${esc(v.version)}: ${esc(REASON[v.generation_reason] ||
      String(v.generation_reason || "").replace(/_/g, " "))}, published ${when(v.published_at)}${v.status === "published"
      ? " <strong>(current)</strong>" : ` <span class="muted">(${esc(v.status)})</span>`}</li>`).join("");
    return `<details class="more detail-section" data-key="versions"><summary>Summary history
      (${esc(d.versions.length)})</summary><ul class="versions">${items}</ul></details>`;
  }

  // Everything behind "Sources (N)": the competing claims first when sources disagree, then each original
  // source, the quote behind each summary statement, and the summary's version history.
  function sourcesPanel(d) {
    return conflictDetails(d) + sourceList(d) + evidence(d.summary) + related(d) + versions(d) + resolvedReviews(d);
  }

  // ------------------------------------------------------------------ feed states
  // `label` names what the feed is narrowed to: a ticker, "the companies you follow", or nothing.
  function feedEmpty(status, label) {
    const counts = status && status.counts;
    if (counts && counts.articles === 0) {
      return `<div class="state"><h3>No news yet</h3><p>Stories appear here as articles are collected.</p></div>`;
    }
    if (counts && counts.events === 0) {
      return `<div class="state"><h3>No summaries yet</h3><p>Articles have been collected but not grouped into
        stories yet. They appear here once they are.</p></div>`;
    }
    return `<div class="state"><h3>No news${label ? ` for ${esc(label)}` : ""} yet</h3><p>New stories appear
      here automatically.</p></div>`;
  }

  // "My watchlist" with nothing followed: say so and offer the two ways on.
  function watchlistEmpty(ignored) {
    return `<div class="state watchlist-empty"><h3>Your watchlist is empty</h3>
      <p>Follow companies to see only their news here. Your watchlist is saved in this browser.</p>${ignored
        ? `<p class="small">${esc(plural(ignored, "company"))} you followed earlier ${ignored === 1 ? "is" : "are"} no
           longer offered here.</p>` : ""}
      <p class="state-actions"><button type="button" class="button" data-open-follow>Choose companies to follow</button>
        <button type="button" class="button secondary" data-view="all">Show all companies</button></p></div>`;
  }

  // ------------------------------------------------------------------ story page (event.html): timeline
  // Every label below comes from a stored record (see summary/timeline.py); times say which stored time they are.
  const TIME_BASIS = { published: "Published", source_updated: "Updated by the source", sec_accepted: "Filed",
                       detected: "Detected", decided: "Decided", summary_published: "Summary published" };
  const ENTRY_LABEL = {
    first_report: "First report", additional_coverage: "Additional coverage", substantive_update: "Substantive update",
    conflicting_report: "Conflicting report", conflict_detected: "Conflict detected", review_decision: "Review decision",
    summary_published: "AI summary published", summary_revised: "AI summary revised",
  };
  const DECISION = {
    confirmed_conflict: "The reviewer confirmed that the sources disagree. The review does not establish which source is correct.",
    not_a_conflict: "The reviewer decided these reports do not conflict.",
    different_events: "The reviewer decided this report describes a different story, so it was moved out of this one.",
    insufficient_evidence: "More evidence is needed; the review stays open.",
  };

  // A date with no time of day ("2026-08-26"), shown as that calendar date whatever the reader's time zone.
  function dateOnly(iso) {
    const d = new Date(`${iso}T00:00:00Z`);
    if (!iso || isNaN(d)) return esc(iso || "unknown date");
    return `<time datetime="${esc(iso)}">${esc(d.toLocaleDateString(undefined, { year: "numeric", month: "short",
      day: "numeric", timeZone: "UTC" }))}</time>`;
  }

  function entryWhen(e) {
    if (e.time) return `<span class="tl-basis">${esc(TIME_BASIS[e.time_basis] || "Time")}</span> ${when(e.time)}`;
    if (e.date) return `<span class="tl-basis">Filed</span> ${dateOnly(e.date)} <span class="muted">(date only; the
      filing gives no time)</span>`;
    return `<span class="tl-basis">${e.revised_report ? "No update time" : "No publication time"} given by the
      source</span>`;
  }

  function reportLink(source) {
    const title = source.title || "Original source";
    return safeUrl(source.url) ? extLink(source.url, title) : `${esc(title)} <span class="muted">(no valid link)</span>`;
  }

  function reportEntry(e) {
    const s = e.source;
    const isSec = s.record_type === "sec_filing";
    const name = isSec ? [s.name || "SEC filing", s.form].filter(Boolean).join(" · ") : publisher(s.name);
    const aspects = (e.aspects || []).length ? listNames(e.aspects) : "";
    let label = ENTRY_LABEL[e.kind];
    let says;
    if (e.kind === "first_report") {
      says = "This story was started from this report." + (e.earlier_report_exists
        ? " A report with an earlier time was collected afterwards and appears above." : "");
    } else if (e.kind === "additional_coverage") {
      if (e.revised_report) label = "Revised report";
      says = e.revised_report ? "The source revised this report; grouping found no new information in it."
        : e.match === "identical_text" ? "The same text as a report already in this story."
          : "Repeats information already reported.";
    } else if (e.kind === "substantive_update") {
      says = `Adds new information or a later development${aspects ? `: ${aspects}` : ""}.`;
    } else {
      says = `Disagrees with this story's earlier sources${aspects ? ` about the ${aspects}` : ""}.`;
    }
    const lines = [`<p class="tl-title">${reportLink(s)}</p>`,
      `<p class="meta">${esc(name)} ${isSec ? `<span class="badge type-sec">SEC filing</span>`
        : `<span class="muted">via Alpaca</span>`}</p>`, `<p>${esc(says)}</p>`];
    if ((e.sections || []).length) {
      lines.push(`<p class="small">Part of the filing about this story: ${e.sections.map(esc).join("; ")}</p>`);
    }
    if (e.revised_report) {
      lines.push(`<p class="small muted">Version ${esc(s.revision_number)} of ${esc(s.revisions_total)} stored for this
        report${s.published_at ? `; first published ${when(s.published_at)}` : ""}.</p>`);
    }
    if (s.current_revision_in_event === false) {
      lines.push(`<p class="small muted">A later version of this report is not part of this story.</p>`);
    }
    if ((e.quotes || []).length) {
      lines.push(`<p class="small muted">Quoted from this report:</p>${quotes(e.quotes)}`);
    }
    if (e.kind === "conflicting_report") {
      lines.push(`<p class="small">${esc(e.review ? reviewText(e.review) : "No review has been opened.")}</p>`);
    }
    if (!e.time && !e.date && s.retrieved_at) {
      lines.push(`<p class="small muted">Collected by this site ${when(s.retrieved_at)} (not a publication time).</p>`);
    }
    return { label, body: lines.join("") };
  }

  function reviewText(review) {
    if (review.review_status === "in_review") return "Under review: more evidence is needed.";
    return DISPUTE_STATUS[review.display_status] || "Under review.";
  }

  function systemEntry(e) {
    if (e.kind === "conflict_detected") {
      const r = e.report || {};
      const topic = e.review && (e.review.topic || []).length ? ` about the ${listNames(e.review.topic)}` : "";
      return { label: ENTRY_LABEL[e.kind], body: `<p>Grouping found that ${reportLink(r)}${r.name
        ? ` (${esc(publisher(r.name))})` : ""} disagrees with this story's earlier sources${esc(topic)}.</p>
        <p class="small">${esc(e.review ? reviewText(e.review) : "No review has been opened for it.")}</p>` };
    }
    if (e.kind === "review_decision") {
      const moved = e.moved_to_event_id && /^[0-9a-f-]{36}$/i.test(e.moved_to_event_id)
        ? ` <a href="/events/${encodeURIComponent(e.moved_to_event_id)}">See the separate story</a>.` : "";
      return { label: e.final ? "Review completed" : ENTRY_LABEL[e.kind],
               body: `<p>${esc(DECISION[e.decision] || "A review decision was recorded.")}${moved}</p>` };
    }
    const reason = REASON[e.generation_reason] || String(e.generation_reason || "").replace(/_/g, " ");
    return { label: ENTRY_LABEL[e.kind], body: `<p>Version ${esc(e.version)}${e.current ? " (current)" : ""}: “${
      esc(e.headline)}”</p><p class="small muted">Reason: ${esc(reason)}. Written by AI from ${esc(plural(e.sources,
      "source"))}.</p>` };
  }

  function timelineItem(e) {
    const { label, body } = e.source ? reportEntry(e) : systemEntry(e);
    const system = e.source ? "" : " tl-system";
    return `<li class="tl-item tl-${esc(e.kind)}${system}" data-kind="${esc(e.kind)}">
      <p class="tl-when">${entryWhen(e)}</p>
      <p class="tl-label"><span class="tl-badge">${esc(label)}</span></p>${body}</li>`;
  }

  function timeline(t) {
    const dated = t.entries.filter((e) => e.time || e.date);
    const undated = t.entries.filter((e) => !e.time && !e.date);
    let html = dated.length ? `<ol class="timeline">${dated.map(timelineItem).join("")}</ol>`
      : `<p class="state-note">No dated reports are stored for this story.</p>`;
    if (undated.length) {
      html += `<h4 class="tl-undated-head">Without a stated time</h4><p class="muted small">The source gave no
        publication or update time for these, so their place in the order above is unknown.</p>
        <ol class="timeline undated">${undated.map(timelineItem).join("")}</ol>`;
    }
    const x = t.excluded || {};
    const notes = [];
    if (x.pending_relations > 0) {
      notes.push(`${plural(x.pending_relations, "more report")} might be about this story, but grouping could not
        confirm it, so ${x.pending_relations === 1 ? "it is" : "they are"} not listed.`);
    }
    if (x.detached_reports > 0) {
      notes.push(`${plural(x.detached_reports, "report")} moved to a separate story after a review ${
        x.detached_reports === 1 ? "is" : "are"} not listed as part of this story.`);
    }
    return html + notes.map((n) => `<p class="muted small">${esc(n)}</p>`).join("");
  }

  // ------------------------------------------------------------------ story page (event.html): the page
  function companiesLine(companies, ctx) {
    if (!(companies || []).length) return "";
    const items = companies.map((c) => {
      const followed = ctx.followed && ctx.followed.has(c.cik);
      const symbol = (c.symbols || [])[0];
      return `<li class="story-company${followed ? " followed" : ""}">${symbol ? `<span class="ticker${followed
        ? " followed" : ""}">${followed ? icon("star") : ""}${esc(symbol)}</span>` : ""}${
        esc(c.name || `CIK ${c.cik}`)}${followed ? `<span class="muted small"> · you follow this company</span>` : ""}</li>`;
    }).join("");
    return `<div class="story-companies"><span class="muted">About</span><ul>${items}</ul></div>`;
  }

  function storyStatus(d) {
    const c = d.conflict || {};
    const label = CONFLICT_LABEL[c.status];
    const parts = [];
    if (label) parts.push(`<p class="conflict-line">${icon("warn")}<strong>${label}</strong>${c.count > 1
      ? ` <span>(${esc(c.count)} disputes)</span>` : ""}<button type="button" class="inline-toggle"
      data-scroll-to="conflict-details">Compare the claims</button></p>`);
    else if (c.status === "resolved") parts.push(`<p class="muted small">${icon("ok")}Earlier conflict reviews are
      resolved (see the timeline).</p>`);
    return parts.join("");
  }

  function storySummary(d, ctx) {
    const s = d.summary;
    if (!s) {
      const text = NO_SUMMARY[d.summary_state] ||
        (ctx.summariesEnabled === false ? "AI summaries are turned off." : "No summary yet.");
      return `<p class="summary-pending">${esc(text)}</p>`;
    }
    const points = (s.key_points || []).map((p) => p.text).filter(Boolean);
    const blocked = d.conflict && ["under_review", "correction_pending"].includes(d.conflict.status);
    return `<p class="ai-label">${icon("ai")}AI summary · version ${esc(s.version)}</p>${paragraphs(s.overview)}
      ${points.length ? `<ul class="summary-points">${points.map((p) => `<li>${esc(p)}</li>`).join("")}</ul>` : ""}
      <p class="summary-note">Written by AI from the sources listed below, published ${when(s.published_at)}. Each
        statement's supporting quote is under “Supporting quotes”. Check the originals before relying on it.</p>
      ${d.update_pending && !blocked ? `<p class="summary-note">An updated summary is being prepared.</p>` : ""}`;
  }

  // The timeline section's content: `t` is the timeline response, {error} when it failed, or null while loading.
  function timelineBlock(t) {
    if (t && t.error) {
      return `${loadError("the timeline", t.error, false)}<p><button type="button" class="button secondary"
        data-retry-timeline>Try again</button></p>`;
    }
    return t ? timeline(t) : `<p class="muted" role="status">Loading the timeline…</p>`;
  }

  // The whole story: headline, companies, status, summary, conflict notices, timeline, sources and quotes.
  function storyPage(d, t, ctx) {
    const headline = (d.summary && d.summary.headline) || d.title || "Untitled story";
    const sample = d.is_sample ? `<span class="badge sample">Sample</span>` : "";
    const merged = d.superseded_by ? `<div class="notice info" role="note"><h4>${icon("info")}This story was merged
      into another story</h4><p><a href="/events/${encodeURIComponent(d.superseded_by)}${d.is_sample ? "?sample=1"
      : ""}">Open the story it was merged into</a>. What is shown here is kept for reference.</p></div>` : "";
    return `${merged}<header class="story-head">${sample ? `<p class="kicker">${sample}</p>` : ""}
        <h2 class="story-title" id="story-title" tabindex="-1">${esc(headline)}</h2>
        ${companiesLine(d.companies, ctx)}${storyStatus(d)}</header>
      <section class="story-section" aria-labelledby="summary-heading"><h3 id="summary-heading">Summary</h3>
        ${storySummary(d, ctx)}<div id="conflict-details">${conflictDetails(d)}</div></section>
      <section class="story-section" aria-labelledby="timeline-heading"><h3 id="timeline-heading">Timeline</h3>
        <p class="muted small">Reports and updates in the order they happened, by each source's own publication or
        update time. Summary and review times are when this site published or recorded them.</p>
        <div id="timeline-slot" aria-live="polite">${timelineBlock(t)}</div></section>
      <section class="story-section" aria-labelledby="sources-heading"><h3 id="sources-heading">Sources and
        evidence</h3>${sourceList(d)}${evidence(d.summary)}${related(d)}${resolvedReviews(d)}</section>`;
  }

  function loadError(what, message, retrying = true) {
    return `<div class="state error" role="alert"><h3>${icon("error")}Could not load ${esc(what)}</h3>
      ${retrying ? "<p>The page tries again automatically.</p>" : ""}<p class="small">Details: ${esc(message)}</p></div>`;
  }

  // ------------------------------------------------------------------ data tools page (tools.html)
  function groupingLine(a, ctx) {
    if (a.events && a.events.length) {
      const sample = a.is_sample ? "&sample=1" : "";
      const links = a.events.map((ev) => `<a href="/#event=${encodeURIComponent(ev.event_id)}${sample}">${
        esc(ev.title || "Untitled event")}</a>`).join(", ");
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

  function articlesEmpty(source, sample) {
    const what = { news: "news articles", sec: "SEC filings" }[source] || "records";
    return `<div class="state"><h3>No stored ${esc(what)}</h3><p>Nothing matches the current filters.</p>${sample ? ""
      : "<p>Sample records are hidden unless <strong>Include sample data</strong> is ticked.</p>"}</div>`;
  }

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

  // ------------------------------------------------------------------ processing activity (/api/activity)
  // What the ingestion worker and the startup summary catch-up report about each article, filing or story, step
  // by step. Durations are measured against the server's clock (`now`), not this browser's.
  const STEP_MARK = { done: "check", skipped: "minus", failed: "x", interrupted: "square", stale: "help" };
  const STEP_WORD = { running: "In progress", done: "Done", skipped: "Skipped", failed: "Failed",
                      interrupted: "Interrupted" };
  const KIND_WORD = { article: "Article", filing: "SEC filing", event: "Story" };
  const EVENT_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

  function duration(ms) {
    if (!(ms >= 0)) return "";
    if (ms < 1000) return "under 1 s";
    const s = Math.round(ms / 1000);
    return s < 60 ? `${s} s` : `${Math.floor(s / 60)} min ${s % 60} s`;
  }

  function activityStep(step, now, stale) {
    const running = step.state === "running";
    const end = running ? now : new Date(step.finished_at);
    const took = duration(end - new Date(step.started_at));
    const state = running && stale ? "stale" : step.state;
    const mark = state === "running" ? `<span class="spinner" aria-hidden="true"></span>`
      : `<span class="step-mark" aria-hidden="true">${svg(STEP_MARK[state] || "dot")}</span>`;
    const ai = step.model ? ` <span class="badge info" title="AI model: ${esc(step.model)}">AI</span>` : "";
    const result = step.result ? ` <span class="step-result">· ${esc(step.result)}</span>`
      : state === "running" ? ` <span class="step-result">…</span>` : "";
    const time = state === "running" ? `${took} so far` : state === "stale" ? "no answer" : step.started_at === step.finished_at ? "" : took;
    return `<li class="step step-${esc(state)}">${mark}<span class="sr-only">${esc(STEP_WORD[step.state] || step.state)}:
      </span><span class="step-label">${esc(step.label)}</span>${ai}${result}${time ? ` <span class="step-time">${
      esc(time)}</span>` : ""}</li>`;
  }

  function activityHead(item) {
    const tickers = (item.symbols || []).slice(0, 4).map((s) => `<span class="ticker">${esc(s)}</span>`).join("");
    const story = item.event_id && EVENT_ID.test(item.event_id)
      ? ` <a class="small" href="/events/${encodeURIComponent(item.event_id)}">Open the story</a>` : "";
    return `<span class="badge">${esc(KIND_WORD[item.kind] || "Item")}</span>${tickers ? ` <span class="tickers">${
      tickers}</span>` : ""} <span class="activity-title">${esc(item.title)}</span>${story}`;
  }

  // The item being worked on now: its steps so far, the running one with a spinner and its time so far.
  function activityCurrent(proc, now) {
    const item = proc && proc.current;
    if (!item) return "";
    const stale = proc.state === "stale";
    const steps = (item.steps || []).map((s) => activityStep(s, now, stale)).join("") ||
      `<li class="step step-running"><span class="spinner" aria-hidden="true"></span>Starting…</li>`;
    return `<div class="activity-now${stale ? " stale" : ""}"><p class="kicker">${stale ? "Last reported" : "Working on"}
      · started ${shortWhen(item.started_at)}</p><p class="activity-item-head">${activityHead(item)}</p>
      <ol class="steps">${steps}</ol>${stale ? `<p class="small status-warn">${icon("warn")}The process stopped answering
      during this step. Unfinished work is picked up again later.</p>` : ""}</div>`;
  }

  // Finished items, newest first; each opens to show its steps. The page keeps which ones the reader opened.
  function activityRecent(proc, now) {
    const items = (proc && proc.recent) || [];
    if (!items.length) return "";
    const MARK = { done: ["status-ok", "ok", "Done"], skipped: ["status-off", "skip", "Nothing needed doing"],
                   failed: ["status-error", "error", "Needs another try"], interrupted: ["status-warn", "warn", "Interrupted"] };
    return `<h4 class="activity-recent-head">Recently finished</h4><ul class="activity-recent">${items.map((item) => {
      const [cls, mark, word] = MARK[item.state] || MARK.done;
      return `<li><details class="activity-item" data-key="${esc(item.id)}">
        <summary><span class="${cls}">${icon(mark)}<span class="sr-only">${esc(word)}: </span></span>${
        activityHead(item)}${item.result ? ` <span class="step-result">· ${
        esc(item.result)}</span>` : ""} <span class="step-time">${shortWhen(item.finished_at)}</span></summary>
        <ol class="steps">${(item.steps || []).map((s) => activityStep(s, now, false)).join("") ||
        `<li class="step muted">No steps were reported.</li>`}</ol></details></li>`;
    }).join("")}</ul>`;
  }

  // One line on the state of the process (and the catch-up's "event 2 of 5").
  function activityStatus(proc, name, now) {
    const state = proc ? proc.state : "not_running";
    const p = proc && proc.state === "running" ? proc.progress : null;
    const progress = p && p.total ? ` · ${esc(Math.min(p.done + (proc.current ? 1 : 0), p.total))} of ${
      esc(plural(p.total, "story", "stories"))}` : "";
    switch (state) {
      case "running":
        return { cls: "status-ok", html: proc.current ? `${icon("ok")}${esc(name)} is working${progress}`
          : `${icon("ok")}${esc(name)} is running and waiting for new articles and filings` };
      case "stale":
        return { cls: "status-warn", html: `${icon("warn")}${esc(name)} has not answered since ${when(proc.updated_at)};
          it may have stopped` };
      case "finished":
        return { cls: "status-ok", html: `${icon("ok")}${esc(name)} finished ${when(proc.updated_at)}${
          proc.note ? `: ${esc(proc.note)}` : ""}` };
      case "stopped":
        return { cls: "status-off", html: `${icon("off")}${esc(name)} stopped ${when(proc.updated_at)}${
          proc.note ? `: ${esc(proc.note)}` : ""}` };
      default:
        return { cls: "status-off", html: `${icon("off")}${esc(name)} has not run with this output folder yet` };
    }
  }

  // The reader page's one line: the step running right now, or "" when nothing is being processed.
  function activityLine(activity) {
    const procs = activity ? [activity.worker, activity.summary] : [];
    const busy = procs.find((p) => p && p.state === "running" && p.current);
    if (!busy) return "";
    const item = busy.current;
    const step = [...(item.steps || [])].reverse().find((s) => s.state === "running");
    const what = step ? `${step.label}${step.model ? " (AI)" : ""}` : "Processing";
    return `<span class="spinner" aria-hidden="true"></span>${esc(what)}: <span class="activity-title">${
      esc(item.title)}</span> <a href="/tools.html#activity">See activity</a>`;
  }

  const api = { esc, safeUrl, extLink, when, clock, ago, plural, icon, notice, sentenceEnds, preview, cardHead,
                tickerChips, conflictLine, summaryView, sourcesPanel, feedEmpty, watchlistEmpty, loadError,
                articleCard, articlesEmpty, collectorStatus, processingStatus, dateOnly, timeline, timelineBlock,
                storyPage, activityCurrent, activityRecent, activityStatus, activityLine };
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  else root.StockNewsRender = api;
})(this);
