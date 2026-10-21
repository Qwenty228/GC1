# summarization/ (stage 2)

Evaluates the event-summary model. A summary has no single right answer, so **people judge it**.
The run produces every summary next to everything the model was given, plus mechanical checks that
point you at likely problems, and a score sheet to fill in.

## Run

Same setup as `consolidation/` (Docker running, `make db`, `.env`). For the local model also see the
docstring of `experiments/local.py` (Ollama with `OLLAMA_CONTEXT_LENGTH=16384`).

```bash
uv run python evaluation/summarization/experiments/local.py     # LIMIT_EVENTS = 3 first, then None
```

The top of the experiment file controls the run:

```python
LIMIT_EVENTS = 3          # summarize only the first N events; None = all (~52)
GROUPING = "answer_key"   # or "model": the same model also groups (end-to-end, slow)
ATTEMPTS = 2              # tries per event when a summary fails the citation checks
```

## What a run does

1. All 125 test articles go into **one** throwaway database, through the real pipeline, in test order.
2. **Grouping by the answer key** (default): the compare step is answered from the file names, so each
   event holds exactly its test group (original, repeat, update, conflict). A bad summary is then the
   summary model's fault, not grouping's. Retrieval still runs as in production; it misses 2 update
   articles, which become one-article events.
3. **Conflict reviews**: production holds an event's summary until a person reviews its conflict. The
   answer key decides each review (`confirmed_conflict`), so the summary must report both claims.
4. **One summary per event** from its final evidence, with the production prompt, schema and citation
   checks (`summary/validation.py`). A summary that fails them is not published, like production.

Expect about 52 events: 25 multi-source events (the interesting ones) and about 27 one-article events.
With the local 7B model the summaries take roughly 30-75 minutes.

## Read the result

```bash
uv run datasources-eval report <run>                  # every event: sources, summary, quotes, checks
uv run datasources-eval report <run> --event E3 --passages   # one event with the full text the model read
uv run datasources-eval report <run> --grouping       # how each article was grouped
uv run datasources-eval compare summarization         # leaderboard
```

`<run>` is the file name printed at the end (e.g. `2026-10-15_2130_local-qwen2.5-7b`), or just the model
name for its newest complete run. Tip: `... report <run> --passages > review.txt` and read it in an editor.

For each event the report shows:

* **sources** `S1..`: the test files in the event and their relation (origin, repeated, update, conflict)
* **answer key**: the event's group, and `MIXED` / `missing` when grouping went wrong
* **review / note**: the conflict decision and, when found, the changed claim (`"$18 billion" vs earlier "$36 billion"`)
* **summary**: each statement with its quotes; `ok` = the quote was found verbatim in the passage it cites
* **NOT PUBLISHED**: the summary failed validation; the reason, and the summary as the model wrote it
* **checks**: how many sources are cited, and whether the update and conflict articles are cited

The checks are hints, not a score: a summary can cite every source with verified quotes and still state
something the quotes do not say. That is what the human rating is for.

## Rate it (the human score)

The run writes `results/<run>_scores.csv`, one row per event. Each rater makes their own copy,
`<run>_scores_<name>.csv`, opens it in Excel, and fills one row per event while reading the report:

| column | scale | meaning |
| --- | --- | --- |
| faithful | 0-2 | 2 every statement is supported by its quotes; 1 a small overstatement; 0 any wrong or invented fact |
| coverage | 0-2 | 2 the main fact AND every new fact from the update article; 1 main fact only; 0 misses the main fact |
| conflict | 0-2 | events with a conflict article only: 2 both claims, each attributed, no winner; 1 mentioned but unattributed; 0 the false claim stated as fact |
| clarity | 0-2 | 2 clear and concise; 1 clumsy or padded; 0 confusing |
| acceptable | y/n | would you publish it on the site as it is? |

Unpublished events are pre-filled `acceptable = n` (the site would show no summary). `compare summarization`
averages every filled sheet of a run in the right-hand columns. Rank models by those.

## How much rating is enough

* **What to rate.** Every multi-source event (about 25): only these test coverage and conflicts. Of the
  one-article events, a sample of 10 is enough (they only test faithfulness and clarity).
* **Check that raters agree first.** Two people rate the same 10 events without talking. If they agree on
  `acceptable` for at least 8 of 10, split the rest between them. If not, discuss the disagreements, add
  a note to the rubric, and rate those 10 again. Report the agreement with the result.
* **Comparing two models.** Rate both models on the same events. With about 25 events, a difference of
  less than about 20 points in the acceptable rate is noise. Hide the model name while rating.
* **Good enough to use** (proposed; agree as a team before looking at results):
  * no event with `faithful = 0`: an invented number or fact is the failure a finance site cannot have
  * no event with `conflict = 0`: the false claim must never be stated as fact
  * `acceptable` for at least 80% of the rated events
  * at least 80% published: otherwise many events show no summary at all
* **A reference helps.** Run a strong model on the same events (copy `local.py` and plug in Claude
  or Gemini through `OpenAICompatibleSummaryBackend` / `AnthropicSummaryBackend`) and rate it the same way.
  "Local gets 60% acceptable where Claude gets 90%" says more than either number alone.

## Use your own model

Copy `experiments/local.py`, change `NAME`, and return any object with `summarize(request)` from
`build_summarizer` (see `datasources.summary.generation` for the Claude backend and a scripted one).

## Full-detail runs, real data and the filter test (`experiments/local_full.py`)

`local_full.py` runs grouping (step 3) and summaries with validation (step 4) and keeps everything a person needs to
judge them by hand. Each run uses its own throwaway database `stock_news_eval_..._test`, never the main database.
The machinery is `src/datasources/evaluation/recording.py`.

* `--mode real` (default) uses a slice of **real** NVDA news plus the 8-K, from
  `populate/003_nvda_8k_windows_2026-07-08.sql`. The default slice is `aug17`: Aug 15-20, about 58 articles and
  8-K 0001045810-26-000069. These dates do not overlap the 125 test articles.
  * Records arrive one at a time in publication order, as they would live.
  * There is no answer key: rate the summaries in `<run>_scores.csv`, and label which articles are the same event in
    `<run>_grouping_labels.csv`.
* `--filters production,no_filter` (the default) runs the slice twice:
  * `production`: the retrieval filter from `.env`.
  * `no_filter`: every event in the time window goes to the LLM.

  `<...>_filter-comparison.json` lists every link one variant made and the other did not, with the LLM's explanation
  and the cosine score, plus the cost difference. It answers "does the filter only save cost, or does it change the
  result?".
* Conflicts: production holds a summary while a conflict review is open. With `--conflicts assume_confirmed`
  (the default), the held state is recorded and the review is then resolved as an **assumed** confirmed conflict
  (reviewer `eval-ASSUMED-unreviewed`), so the summary must state both claims. `--conflicts hold` keeps
  production behaviour.
* `--mode fixtures --grouping answer_key|model` is the 125-test run above, with the same extra detail.

Outputs in `results/`:

| File | Contents |
|---|---|
| `<run>.json` | Per article: the text it was analysed on, its text units, every retrieval candidate (cosine, lexical, fused score, why not compared), the LLM judgments with explanations and validation notes, memberships and timings. Per event: members, conflict state, summary attempts with validation errors, the passages the model was given, checked citations and the published version. Also label-free metrics and review lists (uncertain, singletons, shortlist cuts, pairs just under the gate, conflicts, the 8-K). |
| `calls/<run>.json` | Every model call: the full prompt text (each system prompt stored once, by hash), the raw response including any reasoning field, the parsed output, tokens, latency and errors. |
| `<run>_scores.csv`, `<run>_grouping_labels.csv` | The sheets to fill in by hand. |

`datasources-eval report/compare` read fixtures runs; real runs (`stage: summarization_real`) are plain JSON to open
directly. Commands, from `SystemCode/`:

```
uv run python evaluation/summarization/experiments/local_full.py --mode real --limit-articles 5 --limit-events 2   # smoke test
uv run python evaluation/summarization/experiments/local_full.py --mode real                                       # both variants
uv run python evaluation/summarization/experiments/local_full.py --mode fixtures --grouping model
uv run python evaluation/summarization/experiments/local_full.py --backend offline --limit-articles 5              # plumbing only, no Ollama
```
