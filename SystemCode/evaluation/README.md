# evaluation/

Scores the event-grouping pipeline on 125 labelled test articles (25 batches of 5) with a
model you can swap. One experiment file = one model setup, so everyone's results can be
compared side by side.

## Set up (once, from zero)

Run everything inside `SystemCode/`. You need Docker Desktop (running) and `uv`.

1. `cp .env.example .env`, then open `.env` and set `PGPASSWORD` (any value). If your model
   needs a key, add it too, for example `GROQ_API_KEY=...`, `GEMINI_API_KEY=...` or
   `ANTHROPIC_API_KEY=...`. The rule-based baseline needs no key. Leave the `SIMILARITY_*`
   values as they are in `.env.example`: they change the scores, so everyone must use the same.
2. `uv sync --locked --extra local-embeddings` installs the local embedding model's packages.
3. `uv run datasources-similarity download-model` downloads the pinned embedding model.
4. `make db` starts Postgres in Docker, builds the tables and loads the answer key.
   It is safe to run again any time.

## Run

```bash
uv run python evaluation/consolidation/experiments/baseline_rule.py   # free, no key
make eval EXP=evaluation/consolidation/experiments/baseline_rule.py   # same thing
```

These lines at the top of every experiment file control the run:

```python
BATCHES = [1]     # 1 = tests 001-005 ... 25 = tests 121-125; [] = all 25
SAVE = True       # write the result file (rewritten after every batch)
KEEP_DB = False   # keep each batch's test database to look at it
RESUME = None     # a saved run to finish, e.g. "2026-10-24_1709_gemini"
VERBOSE = False   # True: log every stage as it happens
```

For a paid model, start with one batch.

* **RESUME.** If a run stops part-way (rate limit, outage, Ctrl-C), its result file already holds every
  finished batch. Set `RESUME` to that file's name and run again: batches whose five articles all completed
  are kept, the others run again from their first article (a batch is never patched article by article).
  The model, prompt, embedding model and `SIMILARITY_*` settings must match the saved run, or it refuses.
* **VERBOSE.** Prints a timestamped line for each step: test database, each article, each judgment with
  its validation result, each HTTP call to an OpenAI-compatible API. Never a key, a prompt or article text.

## Use your own model

* Any model: copy `baseline_rule.py` and fill the block marked `PUT YOUR MODEL HERE`.
* Any OpenAI-compatible API: copy `groq.py`, `gemini.py` or `local.py` and change
  `BASE_URL`, `MODEL` and `API_KEY_ENV`. Free tiers: wrap the backend in `RateLimited` as `groq.py` and
  `gemini.py` do, so calls stay under the per-minute limits.

Keys live in `.env`. The files only name the variable.

| File | Model |
| --- | --- |
| `baseline_rule.py` | Rule-based, same answer every time. Template, free. |
| `claude.py` | Claude (Anthropic API) |
| `opus.py` | Claude Opus 5.5 at low effort (Anthropic API) |
| `gemini.py` | Gemini 3.8 Flash (Google, OpenAI-compatible endpoint) |
| `groq.py` | Llama on Groq, free tier |
| `local.py`, `local_qwen3.py` | Local model through Ollama (Qwen2.5-7B, Qwen3-8B) |

All are in `evaluation/consolidation/experiments/`.

example if you work on claude:

```bash
uv run python evaluation/consolidation/experiments/claude.py  
```

## What a run does

For each batch of five articles:

1. It makes a throwaway test database. `stock_news` is never touched.
2. The five articles go through the real pipeline. Your model makes the "same event or
   not" decision at the compare step.
3. The result is compared with the answer key, and the test database is dropped.

## The result

The run prints two scores and a per-article table (expected vs predicted).

* **Grouping F1 (pairwise):** did the articles end up in the right events? Higher is better.
* **Label macro F1:** did the model pick the right label (repeat, update, conflict,
  different)? `uncertain` counts as wrong. Higher is better.

Reference: the free "always same event" baseline over all 25 batches, with the
`.env.example` retrieval settings (3 / 0), scores grouping F1 0.954 and label macro F1 0.355. A real model
should beat both, especially the label score (see "How to read the scores" below).

## Results so far

All 25 batches (100 scored articles), production prompt `compare-2`. "Retrieval" is the setting each run
used: candidate events shown to the model / `SIMILARITY_LEXICAL_PLAUSIBLE_RANK` (0 = lexical search alone
never qualifies a candidate). Runs with different retrieval settings are not strictly comparable.

| Model | Retrieval | Grouping F1 | Label macro F1 | Accuracy | `uncertain` | Model calls | Result file |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Claude Opus 5.5 (low effort) | 6 / 3 | **0.947** | **0.920** | **0.92** | 0 | 101 | `2026-11-06_1007_claude-opus-5-5-low` |
| Gemini 3.8 Flash | 3 / 0 | 0.940 | 0.889 | 0.88 | 2 | 77 | `gemini_125_completed` |
| Claude Sonnet 5.5 | 3 / 0 | 0.925 | 0.869 | 0.87 | 0 | 79 | `2026-10-23_1818_claude-sonnet-5-5` |
| Claude Haiku 5.5 | 6 / 3 | 0.872 | 0.803 | 0.79 | 4 | 116 | `2026-10-23_1557_claude` |
| Claude Haiku 5.5 | 3 / 0 | 0.859 | 0.806 | 0.80 | 2 | 83 | `2026-10-23_1902_haiku-sonnet-settings` |
| Qwen2.5-7B (local) | 6 / 3 | 0.643 | 0.453 | 0.46 | 17 | 132 | `2026-11-05_1519_local-qwen2.5-7b` |
| Rule baseline (always "repeat") | 6 / 3 | 0.735 | 0.102 | 0.25 | 0 | 0 | `2026-11-05_1354_baseline-rule` |

* No LLM merged two different events (grouping precision 1.0 for every model).
* `update` is the hardest label for every model (F1: Opus 0.83, Gemini 0.78, Sonnet 0.72, Haiku 0.62-0.64).
* **Opus** misses 8 of 100: 5 updates split off as new events (`test_028`, `058`, `068`, `078`, `083`),
  2 repeats read as updates (`037`, `107`) and 1 conflict read as an update (`049`, a reversed plan).
* **Gemini** (Le Duc Nghia's run) misses 12: 4 updates split off as new events (`058`, `068`, `078`, `083`),
  3 repeats read as updates (`027`, `037`, `072`), 2 conflicts read as updates (`029`, `049`), 1 update read
  as a repeat (`118`), and 2 `uncertain` (`009`, `063`). In both `uncertain` cases Gemini gave the right
  label; the evidence check downgraded it because one quote was not found word for word.
* The same articles trip every model, which points at the pipeline rather than the model: `test_078` and
  `test_083` never reach the model (the follow-up scores below the 0.78 similarity filter), next-day
  follow-ups (`058`, `068`) are split because the prompt treats a different trading session as a separate
  event, and cancelled or reversed plans (`049`) are labelled `update` as the prompt asks.
* The Gemini run first finished 68 of 125 articles (`2026-10-24_1709_gemini`: 57 failed on HTTP 429,
  timeouts and 503); the other batches were rerun whole with the same settings and combined into
  `gemini_125_completed`. Batch-by-batch explanations and the error analysis are in
  `Miscellaneous/Report/` (repository root).

## Where the result is

* `evaluation/consolidation/results/<date>_<time>_<NAME>.json`. Commit it.
* `uv run datasources-eval compare` prints the leaderboard of all saved runs. Only compare
  runs over the same batches and the same settings (the `cands` and `lex` columns).

## If it fails

* "Docker is not running": start Docker Desktop, run `make db` again.
* Embedding model "not available locally": run the two once-only commands in steps 2 and 3.
* "`<NAME>` is not set": the error names the variable. Add it to `.env`.
* HTTP 429: you hit the provider's rate limit. Lower `REQUESTS_PER_MINUTE` (files that use
  `RateLimited`), run fewer batches, or finish the run later with `RESUME`.

## Why we evaluate, and how to read the scores

**Purpose.** "Consolidation" is the first half of the pipeline: when a news article arrives,
decide whether it belongs to an event we already track, and how (`repeat`: same facts,
`update`: new facts or a later development, `conflict`: a contradictory claim) or whether
it is a `different` event. We test that decision on articles whose answer we know, so we can
compare models fairly and see which kind of decision each one gets wrong.

**The test set.** 25 batches of 5 articles: an original, a repeat, an update and a conflict
of one event, plus one article about a different event. Originals are real Alpaca/Benzinga
news; the others are real articles from other outlets or edited copies (a conflict usually
changes one number). So it tests the grouping and labelling logic, not the full live news mix.

**Why batches run on their own.** Each answer key only describes the five articles inside
its batch. Two people may have picked overlapping stories from the same week, so putting all
125 into one database would need every cross-batch pair checked by hand first. A real feed
keeps all events in one database, so this setup is easier than production.

**The two scores** (agreed by the team, reported separately):

* **Grouping, pairwise F1.** Every pair of articles in a batch should be "same event" or
  not; we count how many pairs the pipeline got right. It checks *membership*, never event ids.
* **Labels, macro F1.** For each article except the first of its batch: was the label
  right (repeat / update / conflict / different)? Macro means each of the four labels counts
  equally, so being good at the easy ones cannot hide a weak `conflict`. `uncertain` is never correct.

**Not every article reaches the model**, and that shapes the scores:

* The first article of a batch has nothing to compare with: it always opens a new event.
* An article that is not similar enough (cosine below `SIMILARITY_MIN_SEMANTIC_SIMILARITY`,
  0.78) never reaches the model either: it opens a new event. With the default settings
  most "different" articles end here, so they are right without any model. That is why
  the always-"same event" baseline still gets grouping F1 0.954: **grouping F1 mostly
  measures retrieval; the label score is where models differ.**
* Two nearly identical articles (400+ identical characters) are linked as a repeat without
  asking the model.
* The "Model calls" line in the output tells you how many decisions actually reached the model.

**How sure can we be.** About 25 articles per label is small: differences of less than
roughly 15 points between two runs are probably noise. Compare runs over the same batches
and the same settings only. If you tune a prompt, do not report the score on the same
batches you tuned on (split by batch, never by article).

**Edge cases included on purpose.** A few batches feed an older article after a newer one
(it happens for real when a backfill stores SEC filings before the news, and with several sources). The label is judged on content against what already
arrived: an older article that adds facts is still an `update`, one that contradicts is a
`conflict`.

`conflict` is also how the pipeline catches fake or AI slop articles:
an article that states a wrong number, date or fact about a known event must be labelled
`conflict` (and flagged), not merged quietly as a repeat or update. Most conflict articles
in the test set are exactly that: a real article with one fact changed. The pipeline judges
claims, not who wrote them: an accurate AI-written article is a normal repeat or update, and
an invented fact that contradicts nothing cannot be caught this way (it would look like an
`update`).

Not covered yet (ideas, not built): all batches in one database with cross-batch distractors, retrieval recall (was the right event even shortlisted?), repeated runs to measure how stable a model's answers are, and deliberately ambiguous items where uncertain is correct. We also do not evaluate against SEC filings. A company's 8-K is filed under legal obligation and false statements in it carry liability, so it is the closest thing we have to a ground-truth record of an event. A future test could put the filing first and check that news articles agreeing with it are grouped as repeats or updates, and that articles contradicting it (including AI slop) are flagged as conflict. Summarization (stage 2) is evaluated separately, by people: see `summarization/README.md`.
