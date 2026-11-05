"""The reader pages in a real headless browser (Edge or Chrome over DevTools; skipped when neither is installed).

Watchlist: following companies, the two feed views, persistence across reloads, empty / unreadable / outdated
saved selections and blocked storage. Story page: the link from a card, direct loads, an unknown or malformed
event, a failed API request, and going back to the feed with its view. Mobile width: no sideways scrolling.
Feed tools: "New"/"Updated" since the last visit, day headers, search and filters (also through the story
page), the filing and update lines, loading placeholders and the guided tours.

The pages are served from a temporary database (``conftest.temporary_database``, dropped afterwards) holding the
registered coverage companies and sample stories grouped through the real graph with scripted models. The usual
guard makes any model client or non-loopback socket fail.
"""

from __future__ import annotations

import json
import uuid
from datetime import timedelta

import pytest

from cdp_browser import Browser, find_browser
from conftest import temporary_database

psycopg = pytest.importorskip("psycopg")

from datasources.db.companies import coverage_companies, register_companies  # noqa: E402
from datasources.ingest.graph import run_pipeline  # noqa: E402
from datasources.similarity.lock import GraphWriterLock  # noqa: E402
from datasources.similarity.pipeline import PipelineContext  # noqa: E402
from test_conflict_notice import live_server, no_model_calls, offline_reads  # noqa: E402,F401  (autouse guard)
from test_similarity_pipeline import anchor, article, envelope, new_id, scripted  # noqa: E402
from test_similarity_pipeline import runtime as grouping_runtime  # noqa: E402

NVDA, MSFT = "0001045810", "0000789019"
SHARED = "Shared Example Partnership"
KEY = "stocknews.watchlist"


@pytest.fixture(scope="module")
def browser_path():
    path = find_browser()
    if path is None:
        pytest.skip("no Edge/Chrome available for the browser checks")
    return path


@pytest.fixture(scope="module")
def site(browser_path):
    with temporary_database("reader") as db:
        with db.transaction() as conn:
            register_companies(conn, coverage_companies())
        lock = GraphWriterLock(db, is_sample=True, purpose="pytest reader pages").acquire()
        try:
            t0 = anchor()

            def same(segment, event):
                both = SHARED in " ".join(u.text for u in segment.units) and SHARED in " ".join(
                    p.text for p in event.passages)
                return "same_event_repeated" if both else "different_event"

            rt = grouping_runtime(db, scripted(same))

            def story(headline, body, hours, symbols):
                raw = {**article(headline, body, t0 + timedelta(hours=hours), article_id=new_id()), "symbols": symbols}
                (outcome,) = run_pipeline([envelope(raw)], PipelineContext(database=db, analyzer=rt)).outcomes
                with db.connection() as conn:
                    return str(conn.execute("SELECT event_id FROM event_membership WHERE revision_id = %s",
                                            (outcome.revision_id,)).fetchone()["event_id"])

            msft = story("Microsoft opens an example campus", "Microsoft opened an example campus.", 1, ["MSFT"])
            shared = story(f"NVIDIA and Microsoft form the {SHARED}",
                           f"NVIDIA and Microsoft formed the {SHARED} for example servers.", 2, ["NVDA", "MSFT"])
            assert story(f"More on the {SHARED}", f"Analysts discussed the {SHARED} and its servers.", 3,
                         ["MSFT", "NVDA"]) == shared
            nvda = [story(f"NVIDIA example launch {i}", f"NVIDIA launched example product {i}.", 3 + i, ["NVDA"])
                    for i in (1, 2)]
            with live_server(db) as base, offline_reads():
                yield {"base": base, "msft": msft, "shared": shared, "nvda": nvda[::-1], "story": story, "t0": t0}
        finally:
            from datasources import server

            server.app.dependency_overrides.pop(server.get_database, None)  # live_server leaves it set
            lock.release()


@pytest.fixture
def page(browser_path):
    browser = Browser(browser_path)
    try:
        yield browser
    finally:
        browser.close()


CARDS = "[...document.querySelectorAll('#events-list .news-card')].map(c => c.dataset.id)"
READY = "document.body.dataset.eventsReady"


def feed(page, url: str, ready: str = "list") -> list[str]:
    page.goto(url)
    page.wait_for(f"{READY} === {json.dumps(ready)} && document.getElementById('events-list').getAttribute("
                  "'aria-busy') === 'false'")
    return page.evaluate(CARDS)


def open_follow_panel(page) -> None:
    if page.evaluate("document.getElementById('follow-panel').hidden"):
        page.evaluate("document.getElementById('follow-toggle').click()")
    page.wait_for("document.querySelectorAll('#follow-body input[data-cik]').length === 11")


def stored(page):
    raw = page.evaluate(f"localStorage.getItem({json.dumps(KEY)})")
    return json.loads(raw) if raw else None


def test_following_companies_filters_the_feed_and_persists_in_this_browser(site, page):
    base = site["base"]
    all_cards = feed(page, f"{base}/#sample=1")
    assert all_cards == site["nvda"] + [site["shared"], site["msft"]]  # All companies by default, newest first
    assert page.evaluate("document.body.dataset.view") == "all"

    open_follow_panel(page)
    note = page.evaluate("document.getElementById('follow-panel').innerText")
    assert "saved in this browser on this device only" in note and "does not sync" in note
    page.evaluate(f"(b => {{ b.focus(); b.click(); }})(document.querySelector('#follow-body input[data-cik=\"{MSFT}\"]'))")
    assert stored(page)["companies"] == [MSFT] and stored(page)["version"] == 1
    assert page.evaluate("document.getElementById('follow-toggle-label').textContent") == "Following 1"
    assert page.evaluate(f"document.activeElement.dataset.cik") == MSFT  # ticking never moves focus

    page.evaluate("document.querySelector('.view-switch [data-view=watchlist]').click()")
    page.wait_for(f"document.body.dataset.view === 'watchlist' && {READY} === 'list' && {CARDS}.length === 2")
    assert page.evaluate(CARDS) == [site["shared"], site["msft"]]  # each story once, NVIDIA-only ones left out
    assert "view=watchlist" in page.evaluate("location.hash")
    followed = page.evaluate(f"[...document.querySelectorAll('#event-{site['shared']} .ticker')].map(t => "
                             "[t.textContent.trim(), t.classList.contains('followed')])")
    assert ["MSFT (you follow this company)", True] in followed and ["NVDA", False] in followed

    page.reload()  # a refresh keeps the view and the selection
    page.wait_for(f"document.body.dataset.view === 'watchlist' && {READY} === 'list' && {CARDS}.length === 2")
    open_follow_panel(page)
    assert page.evaluate(f"document.querySelector('#follow-body input[data-cik=\"{MSFT}\"]').checked") is True
    assert page.evaluate(f"document.querySelector('#follow-body input[data-cik=\"{NVDA}\"]').checked") is False

    page.goto(f"{base}/")  # no view in the address: the last view chosen in this browser
    page.wait_for(f"document.body.dataset.view === 'watchlist' && {READY} === 'list'")
    assert page.evaluate(CARDS) == []  # sample stories need #sample=1; this database has no live ones
    assert "No news" in page.evaluate("document.getElementById('events-state').innerText")

    page.goto(f"{base}/#sample=1&view=watchlist")
    page.wait_for(f"{CARDS}.length === 2")
    open_follow_panel(page)
    page.evaluate("document.querySelector('[data-follow-clear]').click()")
    page.wait_for(f"{READY} === 'empty-watchlist'")
    text = page.evaluate("document.getElementById('events-state').innerText")
    assert "Your watchlist is empty" in text and "Choose companies to follow" in text
    assert stored(page)["companies"] == []


def test_unreadable_and_outdated_saved_watchlists_are_handled(site, page):
    base = site["base"]
    page.goto(f"{base}/#sample=1")
    page.wait_for(f"{READY} === 'list'")
    page.evaluate(f"localStorage.setItem({json.dumps(KEY)}, '{{not json')")
    feed(page, f"{base}/?r1#sample=1&view=watchlist", ready="empty-watchlist")  # a new query: a real reload
    page.wait_for("document.getElementById('company-select').options.length === 12")  # the company list is in
    page.evaluate("document.querySelector('#events-state [data-open-follow]').click()")
    page.wait_for("document.activeElement && document.activeElement.matches('#follow-body input[data-cik]')")
    assert "could not be read" in page.evaluate("document.getElementById('follow-body').innerText")
    assert page.evaluate(f"localStorage.getItem({json.dumps(KEY)})") == "{not json"  # left alone until a change

    mixed = {"version": 1, "companies": [MSFT, "bogus", 42, "0000000001"]}
    page.evaluate(f"localStorage.setItem({json.dumps(KEY)}, {json.dumps(json.dumps(mixed))})")
    cards = feed(page, f"{base}/?again#sample=1&view=watchlist")
    assert cards == [site["shared"], site["msft"]]
    assert page.evaluate("document.getElementById('watchlist-count').textContent") == "(1)"
    requests = page.evaluate("performance.getEntriesByType('resource').map(r => r.name)"
                             ".filter(n => n.includes('/api/events?'))")
    assert requests and all(f"company={MSFT}" in r and "0000000001" not in r for r in requests)
    open_follow_panel(page)
    assert "no longer offered here" in page.evaluate("document.getElementById('follow-body').innerText")
    page.evaluate(f"document.querySelector('#follow-body input[data-cik=\"{NVDA}\"]').click()")
    assert stored(page)["companies"] == [MSFT, NVDA]  # the first change writes back only valid companies
    page.wait_for(f"{CARDS}.length === 4")


def test_a_browser_that_blocks_storage_still_works_for_the_page_view(site, page):
    page.send("Page.addScriptToEvaluateOnNewDocument", source=(
        "Object.defineProperty(window, 'localStorage', {get() { throw new DOMException('blocked', 'SecurityError'); }});"))
    cards = feed(page, f"{site['base']}/#sample=1")
    assert len(cards) == 4
    open_follow_panel(page)
    assert "not saving site data" in page.evaluate("document.getElementById('follow-body').innerText")
    page.evaluate(f"document.querySelector('#follow-body input[data-cik=\"{NVDA}\"]').click()")
    page.evaluate("document.querySelector('.view-switch [data-view=watchlist]').click()")
    page.wait_for(f"document.body.dataset.view === 'watchlist' && {CARDS}.length === 3")


def test_story_page_from_a_card_and_back_to_the_same_feed_view(site, page):
    base, shared = site["base"], site["shared"]
    page.goto(f"{base}/#sample=1")
    page.wait_for(f"{READY} === 'list'")
    page.evaluate(f"localStorage.setItem({json.dumps(KEY)}, {json.dumps(json.dumps({'version': 1, 'companies': [MSFT]}))})")
    feed(page, f"{base}/?w#sample=1&view=watchlist")
    href = page.evaluate(f"document.querySelector('#event-{shared} [data-story-link]').getAttribute('href')")
    assert href == f"/events/{shared}?sample=1#from=view%3Dwatchlist%26sample%3D1"
    page.evaluate(f"document.querySelector('#event-{shared} [data-story-link]').click()")
    page.wait_for(f"location.pathname === '/events/{shared}' && document.body.dataset.storyState === 'ready'")

    text = page.evaluate("document.getElementById('story').innerText")
    assert f"NVIDIA and Microsoft form the {SHARED}" in text
    for label in ("First report", "Additional coverage", "Timeline", "Original sources"):
        assert label in text, label
    assert "you follow this company" in page.evaluate("document.querySelector('.story-companies').innerText")
    kinds = page.evaluate("[...document.querySelectorAll('.timeline .tl-item')].map(i => i.dataset.kind)")
    assert kinds == ["first_report", "additional_coverage"]
    assert page.evaluate("document.getElementById('back-link').getAttribute('href')") == (
        f"/#view=watchlist&sample=1&story={shared}")
    assert page.evaluate("document.activeElement.id") == "story-title"

    page.evaluate("document.getElementById('back-link').click()")  # back to the same view, at the same story
    page.wait_for(f"location.pathname === '/' && document.body.dataset.view === 'watchlist' && {READY} === 'list' "
                  f"&& document.activeElement && document.activeElement.closest('.news-card') "
                  f"&& document.activeElement.closest('.news-card').dataset.id === '{shared}'")
    assert "story=" not in page.evaluate("location.hash")  # the scroll target is dropped once used

    page.goto(f"{base}/events/{shared}?sample=1")  # a direct load: back goes to the feed's default
    page.wait_for("document.body.dataset.storyState === 'ready'")
    assert page.evaluate("document.getElementById('back-link').getAttribute('href')") == "/"


@pytest.mark.parametrize("path, state, text", [
    ("/events/{random}", "not_found", "Story not found"),
    ("/events/{shared}", "not_found", "Story not found"),  # a sample story without ?sample=1
    ("/events/not-a-story", "invalid", "not a valid story link"),
])
def test_story_page_unknown_and_malformed_links(site, page, path, state, text):
    page.goto(site["base"] + path.format(random=uuid.uuid4(), shared=site["shared"]))
    page.wait_for(f"document.body.dataset.storyState === {json.dumps(state)}")
    assert text in page.evaluate("document.getElementById('story').innerText")
    assert page.evaluate("document.getElementById('back-link').getAttribute('href')") == "/"


def test_story_page_shows_an_error_and_recovers(site, page):
    page.fail_requests("*/api/events/*")
    page.goto(f"{site['base']}/events/{site['shared']}?sample=1")
    page.wait_for("document.body.dataset.storyState === 'error'")
    assert "Could not load the story" in page.evaluate("document.getElementById('story').innerText")
    page.fail_requests(None)
    page.evaluate("document.querySelector('[data-retry]').click()")
    page.wait_for("document.body.dataset.storyState === 'ready'")


def test_feed_and_story_page_fit_a_phone_screen(site, page):
    page.send("Emulation.setDeviceMetricsOverride", width=375, height=740, deviceScaleFactor=2, mobile=True)
    feed(page, f"{site['base']}/#sample=1")
    open_follow_panel(page)
    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
    assert page.evaluate("document.querySelector('.site-header .header-link').getAttribute('href')") == "/tools.html"
    heights = page.evaluate("[...document.querySelectorAll('.view-switch button, .follow-item, .story-link, "
                            ".header-link, .chip, #search-input, #when-select')]"
                            ".map(b => b.getBoundingClientRect().height)")
    assert heights and min(heights) >= 44  # comfortable touch targets
    feed(page, f"{site['base']}/?p#sample=1&when=custom&from=2030-01-01")  # the custom date fields too
    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
    assert min(page.evaluate("[...document.querySelectorAll('#from-date, #to-date')]"
                             ".map(b => b.getBoundingClientRect().height)")) >= 44
    page.goto(f"{site['base']}/events/{site['shared']}?sample=1")
    page.wait_for("document.body.dataset.storyState === 'ready'")
    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
    assert page.evaluate("document.querySelector('.site-header .header-link').getAttribute('href')") == "/tools.html"


def test_processing_activity_is_live_on_the_tools_page_and_named_on_the_feed(site, page, tmp_path, monkeypatch):
    from datasources.activity import ActivityTracker

    monkeypatch.setenv("INGEST_OUTPUT_DIR", str(tmp_path))  # the server reads the activity files from here
    worker = ActivityTracker(tmp_path, "worker", heartbeat=0)
    catchup = ActivityTracker(tmp_path, "summary", heartbeat=0)
    try:
        worker.start_item("article", "NVIDIA example contract", symbols=["NVDA"])
        worker.step("similar", "Looking for related stories")
        worker.end_step("done", "1 possibly related story")
        worker.step("compare", "Comparing with 1 story", model="example-model")
        catchup.start_item("event", "Example catch-up story", event_id=site["msft"])
        catchup.step("summary", "Writing the summary", model="example-model")
        catchup.end_step("failed", "rejected by the citation checks (K1: quote too long); retried later")
        catchup.finish_item()
        catchup.close("finished", note="invalid 1; 1 of at most 10 model request(s)")

        page.goto(f"{site['base']}/tools.html")
        page.wait_for("document.querySelector('#activity-worker-current .step-running .spinner') !== null")
        text = page.evaluate("document.getElementById('activity').innerText")
        assert "NVIDIA example contract" in text and "1 possibly related story" in text
        assert "Comparing with 1 story" in text and "so far" in text
        assert "The startup summary catch-up finished" in text and "invalid 1" in text
        recent = page.evaluate("document.getElementById('activity-summary-recent').innerHTML")
        assert "rejected by the citation checks" in recent and f'href="/events/{site["msft"]}"' in recent

        # The comparison ends and the article finishes: it moves to the list, which the reader can open, and an
        # opened item stays open when the list is redrawn for the next article.
        worker.step("save", "Saving and grouping")
        worker.end_step("done", "saved; added to an existing story")
        worker.finish_item()
        page.wait_for("document.querySelectorAll('#activity-worker-recent details').length === 1 && "
                      "document.getElementById('activity-worker-current').innerHTML === ''")
        page.evaluate("document.querySelector('#activity-worker-recent summary').click()")
        worker.start_item("filing", "EXAMPLE CORP 8-K", symbols=["MSFT"])
        worker.finish_item()
        page.wait_for("document.querySelectorAll('#activity-worker-recent details').length === 2")
        assert page.evaluate("[...document.querySelectorAll('#activity-worker-recent details')].map(d => d.open)") == [
            False, True]

        # The news page names the step running right now and links to the activity.
        worker.start_item("article", "Microsoft example update", symbols=["MSFT"])
        worker.step("summary", "Writing the summary", model="example-model")
        feed(page, f"{site['base']}/#sample=1")
        page.wait_for("!document.getElementById('activity-line').hidden")
        line = page.evaluate("document.getElementById('activity-line').innerText")
        assert "Writing the summary (AI)" in line and "Microsoft example update" in line
        assert page.evaluate("document.querySelector('#activity-line a').getAttribute('href')") == "/tools.html#activity"
        worker.finish_item()
        page.evaluate("document.getElementById('retry-now').click()")  # refresh now instead of in 20 s
        page.wait_for("document.getElementById('activity-line').hidden")
    finally:
        worker.close("stopped")


# ----------------------------------------------------------------------------- since the last visit, day headers
VISITS = "stocknews.visits"


def set_visits(page, since, last_seen_js: str = "new Date().toISOString()") -> None:
    page.evaluate(f"localStorage.setItem({json.dumps(VISITS)}, JSON.stringify({{version: 1, since: "
                  f"{json.dumps(since)}, last_seen: {last_seen_js}}}))")


def fresh(page) -> dict:
    return page.evaluate("Object.fromEntries([...document.querySelectorAll('#events-list .news-card')].map(c => "
                         "[c.dataset.id, (c.querySelector('.fresh-new') ? 'new' : c.querySelector('.fresh-updated') "
                         "? 'updated' : '')]))")


def test_new_and_updated_since_the_last_visit_and_day_headers(site, page):
    base, t0 = site["base"], site["t0"]
    feed(page, f"{base}/#sample=1")
    assert page.evaluate("document.querySelectorAll('.fresh-new, .fresh-updated').length") == 0  # a first visit: none
    assert page.evaluate("document.getElementById('fresh-note').hidden") is True

    # A visit that is still going on keeps its baseline: here between the shared story's two reports.
    set_visits(page, (t0 + timedelta(minutes=150)).isoformat())
    feed(page, f"{base}/?v1#sample=1")
    assert fresh(page) == {site["nvda"][0]: "new", site["nvda"][1]: "new", site["shared"]: "updated", site["msft"]: ""}
    note = page.evaluate("document.getElementById('fresh-note').innerText")
    assert "2 new stories and 1 updated since your last visit" in note
    labels = page.evaluate("[...document.querySelectorAll('#events-list .fresh-new, #events-list .fresh-updated')]"
                           ".map(b => b.textContent)")
    assert sorted(labels) == ["New since your last visit"] * 2 + ["Updated since your last visit"]  # in words

    # Day headers: one before the first story of each day, never two in a row.
    rows = page.evaluate("[...document.querySelectorAll('#events-list > *')].map(e => "
                         "e.classList.contains('day-head') ? 'H' : 'C')")
    assert rows[0] == "H" and rows.count("C") == 4 and "HH" not in "".join(rows)
    heads = page.evaluate("(() => { const R = window.StockNewsRender, now = new Date(), day = 864e5; "
                          "return [0, 1, 3].map(n => R.dayHead(R.dayKey(new Date(now - n * day).toISOString()), now)); })()")
    assert heads[0].startswith("Today ") and heads[1].startswith("Yesterday ") and "day" in heads[2]

    page.evaluate("document.querySelector('[data-mark-read]').click()")
    assert set(fresh(page).values()) == {""} and page.evaluate("document.getElementById('fresh-note').hidden")
    feed(page, f"{base}/?v2#sample=1")  # a reload in the same visit keeps them read
    assert set(fresh(page).values()) == {""}

    # Back after more than 30 minutes away: the previous visit's end is the new baseline (the sample stories are
    # dated in the future, so all of them are newer).
    set_visits(page, (t0 - timedelta(days=400)).isoformat(), "new Date(Date.now() - 31 * 60000).toISOString()")
    feed(page, f"{base}/?v3#sample=1")
    assert set(fresh(page).values()) == {"new"}


# ----------------------------------------------------------------------------- search and filters
def type_into(page, selector: str, text: str) -> None:
    page.evaluate(f"document.querySelector({json.dumps(selector)}).focus()")
    for ch in text:
        page.send("Input.dispatchKeyEvent", type="keyDown", text=ch, key=ch, unmodifiedText=ch)
        page.send("Input.dispatchKeyEvent", type="keyUp", key=ch)


def settled(page, count: int) -> list[str]:
    page.wait_for(f"{READY} === 'list' && !document.getElementById('events-list').classList.contains('is-stale') "
                  "&& document.getElementById('events-list').getAttribute('aria-busy') === 'false' "
                  f"&& {CARDS}.length === {count}")
    return page.evaluate(CARDS)


def test_search_and_filters_narrow_the_feed_and_survive_the_story_page(site, page):
    base = site["base"]
    feed(page, f"{base}/#sample=1")
    type_into(page, "#search-input", "partnership")  # case-insensitive; runs after a short pause in typing
    page.wait_for("location.hash.includes('q=partnership')")
    assert settled(page, 1) == [site["shared"]]
    assert "matching “partnership”" in page.evaluate("document.getElementById('filter-note').innerText")

    page.evaluate("document.getElementById('filter-filing').click()")  # no story here has an SEC filing
    page.wait_for("document.getElementById('events-state').innerText.includes('No stories match')")
    assert page.evaluate("document.getElementById('filter-filing').getAttribute('aria-pressed')") == "true"
    assert "with an SEC filing" in page.evaluate("document.getElementById('events-state').innerText")
    page.evaluate("document.querySelector('#events-state [data-clear-filters]').click()")
    assert len(settled(page, 4)) == 4
    assert "q=" not in page.evaluate("location.hash")
    assert page.evaluate("document.getElementById('search-input').value") == ""

    # Dates come from the address too, and the controls show them.
    feed(page, f"{base}/?d1#sample=1&when=custom&from=2000-01-01&to=2000-12-31")
    assert page.evaluate(CARDS) == []
    assert "No stories match" in page.evaluate("document.getElementById('events-state').innerText")
    assert page.evaluate("[document.getElementById('when-select').value, "
                         "document.getElementById('from-date').value]") == ["custom", "2000-01-01"]
    feed(page, f"{base}/?d2#sample=1&when=custom&from=2099-12-31&to=2030-01-01")  # put into order
    assert len(page.evaluate(CARDS)) == 4 and "from=2030-01-01" in page.evaluate("location.hash")

    # The filters apply to the watchlist view, and go to the story page and back with the reader.
    page.evaluate(f"localStorage.setItem({json.dumps(KEY)}, "
                  f"{json.dumps(json.dumps({'version': 1, 'companies': [MSFT]}))})")
    feed(page, f"{base}/?w#sample=1&view=watchlist&q=campus")
    assert page.evaluate(CARDS) == [site["msft"]]
    page.evaluate(f"document.querySelector('#event-{site['msft']} [data-story-link]').click()")
    page.wait_for("document.body.dataset.storyState === 'ready'")
    assert page.evaluate("document.getElementById('back-link').getAttribute('href')") == (
        f"/#view=watchlist&q=campus&sample=1&story={site['msft']}")


def test_filing_and_update_lines_use_plain_words_and_stored_aspects(site, page):
    feed(page, f"{site['base']}/#sample=1")
    filing, update, no_filing, no_update = page.evaluate("""(() => { const R = window.StockNewsRender;
        const filing = R.filingLine({filings: [{form: '8-K', items: [
            {code: '2.02', caption: 'Results of Operations and Financial Condition'},
            {code: '9.01', caption: 'Financial Statements and Exhibits'}, {code: '9.99', caption: null}]}]});
        const update = R.updateLine({latest_update: {time: null, name: 'benzinga',
            aspects: ['closing date', '<b>price</b>']}});
        return [filing, update, R.filingLine({filings: []}), R.updateLine({latest_update: null})]; })()""")
    assert "Earnings and financial results" in filing and "(Item 2.02)" in filing and "Item 9.99" in filing
    assert "9.01" not in filing  # the exhibit list is left out next to anything else
    assert 'title="Item 2.02: Results of Operations and Financial Condition"' in filing
    assert "from Benzinga: new information on closing date and &lt;b&gt;price&lt;/b&gt;." in update  # escaped
    assert no_filing == "" and no_update == ""


# ----------------------------------------------------------------------------- loading placeholders and the tour
TOUR = "stocknews.tour"
PAUSE = "new Promise(r => setTimeout(r, 700))"  # the smooth scroll and the spotlight glide


def test_loading_placeholders_are_announced_and_replaced(site, page):
    feed(page, f"{site['base']}/#sample=1")
    assert page.evaluate("document.querySelector('#events-state .skeleton')") is None  # gone once stories are in
    served = page.evaluate("fetch('/').then(r => r.text())")
    assert 'class="skeleton"' in served and "Loading news…" in served  # shown before the scripts have run
    cards = page.evaluate("window.StockNewsRender.skeletonCards(2, 'Loading news…')")
    lines = page.evaluate("window.StockNewsRender.skeletonLines(3, 'Loading sources…')")
    for html, words in ((cards, "Loading news…"), (lines, "Loading sources…")):
        assert 'role="status"' in html and f'<span class="sr-only">{words}</span>' in html and 'aria-hidden="true"' in html
    assert cards.count("skeleton-card") == 2


def tour(page) -> dict | None:
    return page.evaluate("""(() => { const c = document.querySelector('.tour-card'); if (!c) return null;
        const r = c.getBoundingClientRect();
        return {count: document.getElementById('tour-count').textContent,
                title: document.getElementById('tour-title').textContent,
                inside: r.left >= 0 && r.top >= 0 && r.right <= innerWidth + 0.5 && r.bottom <= innerHeight + 0.5,
                focus: c.contains(document.activeElement),
                fits: document.documentElement.scrollWidth <= innerWidth}; })()""")


def walk_tour(page) -> list[str]:
    titles = []
    while (state := tour(page)) is not None and len(titles) < 20:
        page.evaluate(PAUSE)
        state = tour(page)
        assert state["inside"] and state["focus"] and state["fits"], state
        titles.append(state["title"])
        page.evaluate("document.querySelector('.tour [data-tour=next]').click()")
    return titles


def test_the_tour_opens_by_itself_once_and_walks_through_the_feed(site, page):
    base = site["base"]
    feed(page, f"{base}/#sample=1")  # sample mode (demos and tests) never opens it by itself
    page.evaluate(PAUSE)
    assert tour(page) is None
    page.goto(f"{base}/")  # the first real visit in this browser
    page.wait_for("!!document.querySelector('.tour-card')")
    first = tour(page)
    assert (first["count"], first["title"], first["focus"]) == ("Step 1 of 9", "Welcome to StockEvent Lens", True)
    assert json.loads(page.evaluate(f"localStorage.getItem({json.dumps(TOUR)})"))["seen"] == ["feed"]
    page.send("Input.dispatchKeyEvent", type="keyDown", key="Escape", code="Escape", windowsVirtualKeyCode=27)
    assert tour(page) is None
    page.goto(f"{base}/?again")
    page.wait_for(f"{READY} === 'list'")
    page.evaluate(PAUSE)
    assert tour(page) is None  # only once

    # The Tour button: every step on screen and in focus; the story steps point at the first story.
    feed(page, f"{base}/?t#sample=1")
    page.evaluate("(b => { b.focus(); b.click(); })(document.getElementById('tour-button'))")
    page.evaluate("document.querySelector('.tour [data-tour=next]').click()")
    page.evaluate(PAUSE)
    assert tour(page)["title"] == "Each card is one story"
    offset = page.evaluate("(() => { const s = document.querySelector('.tour-spot').getBoundingClientRect(), "
                           "c = document.querySelector('#events-list .news-card').getBoundingClientRect(); "
                           "return [s.left - c.left, s.top - c.top, s.width - c.width, s.height - c.height]; })()")
    assert all(abs(a - b) <= 3 for a, b in zip(offset, [-6, -6, 12, 12])), offset  # the spotlight is on the story
    page.send("Input.dispatchKeyEvent", type="keyDown", key="ArrowLeft", code="ArrowLeft", windowsVirtualKeyCode=37)
    assert tour(page)["count"] == "Step 1 of 9"
    titles = walk_tour(page)
    assert len(titles) == 9 and titles[0] == "Welcome to StockEvent Lens" and titles[-1] == "You are all set"
    assert page.evaluate("document.activeElement.id") == "tour-button"  # focus returns where the tour began

    page.send("Emulation.setDeviceMetricsOverride", width=375, height=740, deviceScaleFactor=2, mobile=True)
    page.evaluate("scrollTo(0, 0); document.getElementById('tour-button').click()")
    assert len(walk_tour(page)) == 9  # a phone: every step fits the screen


FIRST_CARD = "document.querySelector('#events-list .news-card')"
SOURCES_OPEN = (f"(c => c.querySelector('.card-actions [data-action=sources]').getAttribute('aria-expanded') === 'true' "
                f"&& !c.querySelector('[data-slot=sources]').hidden)({FIRST_CARD})")


def tour_to(page, title: str) -> None:
    page.evaluate("document.getElementById('tour-button').click()")
    while tour(page)["title"] != title:
        page.evaluate("document.querySelector('.tour [data-tour=next]').click()")
    page.evaluate(PAUSE)


def test_the_tour_opens_the_hidden_parts_it_explains_and_closes_them_again(site, page):
    feed(page, f"{site['base']}/?parts#sample=1")
    tour_to(page, "Read the whole summary")
    # The sample stories have no AI summary yet: nothing to open, and the step says so.
    assert "already fits on its card" in page.evaluate("document.getElementById('tour-body').textContent")
    assert page.evaluate(f"{FIRST_CARD}.querySelector('[data-slot=full]').hidden")

    page.evaluate("document.querySelector('.tour [data-tour=next]').click()")
    page.wait_for(f"{SOURCES_OPEN} && !!{FIRST_CARD}.querySelector('[data-slot=sources] .source-list')")
    page.evaluate(PAUSE)
    state = tour(page)
    assert state["title"] == "Check the sources" and state["inside"] and state["focus"], state
    assert "opens this list" in page.evaluate("document.getElementById('tour-body').textContent")
    # One spotlight over the buttons and the loaded panel together.
    top, bottom = page.evaluate(f"""(c => {{ const s = document.querySelector('.tour-spot').getBoundingClientRect(),
        a = c.querySelector('.card-actions').getBoundingClientRect(),
        p = c.querySelector('[data-slot=sources]').getBoundingClientRect();
        return [s.top - (a.top - 6), s.bottom - Math.min(p.bottom + 6, innerHeight + 6)]; }})({FIRST_CARD})""")
    assert abs(top) <= 3 and abs(bottom) <= 3, (top, bottom)

    page.send("Input.dispatchKeyEvent", type="keyDown", key="Escape", code="Escape", windowsVirtualKeyCode=27)
    assert tour(page) is None and not page.evaluate(SOURCES_OPEN)  # closing the tour closes what it opened

    # Sources the reader opened stay open: the tour only closes what it opened itself.
    page.evaluate(f"{FIRST_CARD}.querySelector('.card-actions [data-action=sources]').click()")
    page.wait_for(SOURCES_OPEN)
    tour_to(page, "Check the sources")
    page.evaluate("document.querySelector('.tour [data-tour=next]').click()")
    assert tour(page)["title"] == "When sources disagree" and page.evaluate(SOURCES_OPEN)
    page.send("Input.dispatchKeyEvent", type="keyDown", key="Escape", code="Escape", windowsVirtualKeyCode=27)

    # The watchlist step opens the Follow companies panel and spotlights it with its button; leaving closes it.
    tour_to(page, "Your watchlist")
    state = tour(page)
    assert state["count"] == "Step 7 of 9" and state["inside"] and state["focus"], state
    assert not page.evaluate("document.getElementById('follow-panel').hidden")
    top, bottom = page.evaluate("""(() => { const s = document.querySelector('.tour-spot').getBoundingClientRect(),
        b = document.getElementById('follow-toggle').getBoundingClientRect(),
        p = document.getElementById('follow-panel').getBoundingClientRect();
        return [s.top - Math.max(Math.min(b.top, p.top) - 6, -6), s.bottom - Math.min(p.bottom + 6, innerHeight + 6)]; })()""")
    assert abs(top) <= 3 and abs(bottom) <= 3, (top, bottom)
    page.evaluate("document.querySelector('.tour [data-tour=next]').click()")
    assert tour(page)["title"] == "See what is new" and page.evaluate("document.getElementById('follow-panel').hidden")


def test_the_story_page_has_its_own_tour(site, page):
    page.goto(f"{site['base']}/events/{site['shared']}?sample=1")
    page.wait_for("document.body.dataset.storyState === 'ready'")
    page.evaluate(PAUSE)
    assert tour(page) is None  # only from the button
    page.evaluate("document.getElementById('tour-button').click()")
    assert walk_tour(page) == ["A story's own page", "The story", "The AI summary", "Timeline", "Sources and evidence"]
    assert page.evaluate("document.querySelector('.site-header .header-link').getAttribute('href')") == "/tools.html"
