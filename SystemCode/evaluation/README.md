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
2. `make sync-local-embeddings` installs the local embedding model's packages.
3. `uv run datasources-similarity download-model` downloads the pinned embedding model.
4. `make db` starts Postgres in Docker, builds the tables and loads the answer key.
   It is safe to run again any time.

## Run

```bash
uv run python evaluation/consolidation/experiments/baseline_rule.py   # free, no key
make eval EXP=evaluation/consolidation/experiments/baseline_rule.py   # same thing
```

Three lines at the top of every experiment file control the run:

```python
BATCHES = [1]     # 1 = tests 001-005 ... 25 = tests 121-125; [] = all 25
SAVE = True       # write the result file
KEEP_DB = False   # keep each batch's test database to look at it
```

For a paid model, start with one batch.

## Use your own model

* Any model: copy `baseline_rule.py` and fill the block marked `PUT YOUR MODEL HERE`.
* Any OpenAI-compatible API: copy `groq.py`, `gemini.py` or `ollama_local.py` and change
  `BASE_URL`, `MODEL` and `API_KEY_ENV`.

Keys live in `.env`. The files only name the variable.

| File | Model |
| --- | --- |
| `baseline_rule.py` | Rule-based, same answer every time. Template, free. |
| `claude.py` | Claude (Anthropic API) |
| `gemini.py` | Gemini |
| `groq.py` | Llama on Groq, free tier |
| `ollama_local.py` | Local model through Ollama, LM Studio or vLLM |

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
`.env.example` settings, scores grouping F1 0.954 and label macro F1 0.355. A real model
should beat both, especially the label score (see "How to read the scores" below).

## Where the result is

* `evaluation/consolidation/results/<date>_<time>_<NAME>.json`. Commit it.
* `uv run datasources-eval compare` prints the leaderboard of all saved runs. Only compare
  runs over the same batches and the same settings (the `cands` and `lex` columns).

## If it fails

* "Docker is not running": start Docker Desktop, run `make db` again.
* Embedding model "not available locally": run the two once-only commands in steps 2 and 3.
* "`<NAME>` is not set": the error names the variable. Add it to `.env`.
* HTTP 429: you hit the provider's rate limit. Run fewer batches or wait a minute.

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

Not covered yet (ideas, not built): all batches in one database with cross-batch distractors, retrieval recall (was the right event even shortlisted?), repeated runs to measure how stable a model's answers are, and deliberately ambiguous items where uncertain is correct. We also do not evaluate against SEC filings. A company's 8-K is filed under legal obligation and false statements in it carry liability, so it is the closest thing we have to a ground-truth record of an event. A future test could put the filing first and check that news articles agreeing with it are grouped as repeats or updates, and that articles contradicting it (including AI slop) are flagged as conflict. Summarization (stage 2) is evaluated separately later.
