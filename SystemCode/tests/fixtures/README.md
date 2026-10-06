# Test fixtures

Every file in this directory whose name starts with `synthetic_` is **synthetic**:
hand-written for tests, not downloaded from SEC or Alpaca. The values (company
names, numbers, article IDs, URLs on `example.test`) are invented and must not be
used as real data.

| File | Imitates | Purpose |
| --- | --- | --- |
| `synthetic_8k_financial.htm` | Inline-XBRL 8-K primary document | Tables with split `$` / `)` / `%` cells, visible and hidden iXBRL facts, script/style/comment removal, lists, exhibit links |
| `synthetic_alpaca_ws_batch.json` | Alpaca WebSocket frame (JSON array) | Operational frames mixed with `T="n"` news messages |
| `synthetic_alpaca_rest_page1.json`, `synthetic_alpaca_rest_page2.json` | Alpaca REST `/v1beta1/news` pages | Pagination via `next_page_token`, an article without `content` |
| `synthetic_similarity_labelled_examples.json` | Pairs of news passages with an expected comparison label | Paraphrase, missing information, different fiscal quarters, completion and cancellation updates, comparable-figure and denial conflicts, a vague headline, different partners. Drives the orchestration tests (with scripted model responses) and `datasources-similarity evaluate` (real backend). Fictional companies. |
| `synthetic_conflict_notice_veltrani.json` | The fictional NVIDIA / Veltrani Photonics acquisition from the bounded run `e2e-20260929-842a7f` | Four articles (USD 250 million vs USD 300 million), the recorded comparison judgments (labels, differences, verbatim evidence quotes) and published summary statements, replayed by scripted backends in `test_conflict_notice.py`; plus one extra fictional article (closing-date conflict) and a post-review summary for multi-dispute and regeneration cases. Fictional company, publishers and URLs. |

`legacy_003_drift.sql` is not data: it is schema DDL copied from a read-only dump of `stock_news`, the objects
the reverted `003_event_match_routing.sql` branch left there (an unused table and trigger function, a column,
an index, three wider checks, a view column). `test_migrations.py` applies it to temporary databases to test
baseline adoption against the legacy state `stock_news` really has. It is never a migration and is never run
against a real database.

Real (non-synthetic) fixtures used by the tests live in `data/`:

* `data/submissions_CIK0001045810.json`: an actual SEC submissions response for NVIDIA.
* `data/8k/<accession>-<document>.htm`: actual NVIDIA 8-K documents downloaded from EDGAR.
* `data/alpaca_news_sample.json` is **synthetic** too (example.com URLs, IDs 4000000x).
  It is the demo-mode sample and is never presented as live data.
