// The feed's search and filters, shared by the feed (app.js) and the story page (event.js, for "Back to the news").
// They live in the feed's address: q=<text>, when=24h|7d|30d|custom (with from/to=YYYY-MM-DD for custom dates),
// filing=1 (stories with an SEC filing) and disagree=1 (stories whose sources disagree). Anything else is ignored.
// The server applies them before paging (GET /api/events: q, since, until, has_filing, conflict); nothing here
// talks to the server.

(function (root) {
  "use strict";

  const SPANS = { "24h": 864e5, "7d": 7 * 864e5, "30d": 30 * 864e5 };
  const SPAN_TEXT = { "24h": "in the past 24 hours", "7d": "in the past 7 days", "30d": "in the past 30 days" };
  const DAY = /^(\d{4})-(\d{2})-(\d{2})$/;
  const MAX_QUERY = 200; // the API's limit

  function empty() {
    return { q: "", when: "", from: "", to: "", filing: false, disagree: false };
  }

  // A calendar date (YYYY-MM-DD) that exists, else "".
  function day(value) {
    const m = DAY.exec(value || "");
    if (!m) return "";
    const d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
    return d.getFullYear() === Number(m[1]) && d.getMonth() === Number(m[2]) - 1 && d.getDate() === Number(m[3])
      ? value : "";
  }

  function cleanQuery(text) {
    return String(text || "").replace(/\s+/g, " ").trim().slice(0, MAX_QUERY);
  }

  // Filters from address parameters (URLSearchParams), validated; custom dates in the wrong order are swapped.
  function read(params) {
    const f = empty();
    f.q = cleanQuery(params.get("q"));
    const when = params.get("when") || "";
    if (SPANS[when]) {
      f.when = when;
    } else if (when === "custom") {
      let from = day(params.get("from"));
      let to = day(params.get("to"));
      if (from && to && from > to) [from, to] = [to, from];
      if (from || to) Object.assign(f, { when: "custom", from, to });
    }
    f.filing = params.get("filing") === "1";
    f.disagree = params.get("disagree") === "1";
    return f;
  }

  function write(f, params) {
    if (f.q) params.set("q", f.q);
    if (f.when) params.set("when", f.when);
    if (f.when === "custom") {
      if (f.from) params.set("from", f.from);
      if (f.to) params.set("to", f.to);
    }
    if (f.filing) params.set("filing", "1");
    if (f.disagree) params.set("disagree", "1");
    return params;
  }

  const active = (f) => Boolean(f.q || f.when || f.filing || f.disagree);

  function localMidnight(value) {
    const [y, m, d] = value.split("-").map(Number);
    return new Date(y, m - 1, d);
  }

  // The API parameters for these filters at `now` (custom dates are whole days in this browser's time zone).
  function api(f, now, params) {
    if (f.q) params.set("q", f.q);
    if (SPANS[f.when]) params.set("since", new Date(now - SPANS[f.when]).toISOString());
    if (f.when === "custom") {
      if (f.from) params.set("since", localMidnight(f.from).toISOString());
      if (f.to) {
        const end = localMidnight(f.to);
        end.setDate(end.getDate() + 1);
        params.set("until", end.toISOString());
      }
    }
    if (f.filing) params.set("has_filing", "true");
    if (f.disagree) params.set("conflict", "true");
    return params;
  }

  function dateText(value) {
    return localMidnight(value).toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" });
  }

  // Plain phrases for what the filters keep ("matching “nvidia”", "in the past 7 days", ...), for the feed's note.
  function describe(f) {
    const parts = [];
    if (f.q) parts.push(`matching “${f.q}”`);
    if (SPAN_TEXT[f.when]) parts.push(SPAN_TEXT[f.when]);
    if (f.when === "custom") {
      parts.push(f.from && f.to ? (f.from === f.to ? `on ${dateText(f.from)}` : `from ${dateText(f.from)} to ${
        dateText(f.to)}`) : f.from ? `from ${dateText(f.from)}` : `up to ${dateText(f.to)}`);
    }
    if (f.filing) parts.push("with an SEC filing");
    if (f.disagree) parts.push("where sources disagree");
    return parts;
  }

  const same = (a, b) => ["q", "when", "from", "to", "filing", "disagree"].every((k) => a[k] === b[k]);

  const api_ = { MAX_QUERY, empty, read, write, active, api, describe, same, cleanQuery, day };
  if (typeof module !== "undefined" && module.exports) module.exports = api_;
  else root.StockNewsFilters = api_;
})(this);
