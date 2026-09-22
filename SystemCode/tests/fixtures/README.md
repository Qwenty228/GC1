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

Real (non-synthetic) fixtures used by the tests live in `data/`:

* `data/submissions_CIK0001045810.json`: an actual SEC submissions response for NVIDIA.
* `data/8k/<accession>-<document>.htm`: actual NVIDIA 8-K documents downloaded from EDGAR.
* `data/alpaca_news_sample.json` is **synthetic** too (example.com URLs, IDs 4000000x).
  It is the demo-mode sample and is never presented as live data.
