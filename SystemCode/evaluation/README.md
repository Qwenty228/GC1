# evaluation/

Scores the event-grouping pipeline on 125 labelled test articles (25 batches of 5) with a
model you can swap. One experiment file = one model setup, so everyone's results can be
compared side by side.

## Set up (once, from zero)

Run everything inside `SystemCode/`. You need Docker Desktop (running) and `uv`.

1. `cp .env.example .env`, then open `.env` and set `PGPASSWORD` (any value). If your model
   needs a key, add it too, for example `GROQ_API_KEY=...`, `GEMINI_API_KEY=...` or
   `ANTHROPIC_API_KEY=...`. The rule-based baseline needs no key.
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

Reference: the free "always same event" baseline over all 25 batches scores grouping F1
0.735 and label macro F1 0.102. A real model should beat both.

## Where the result is

* `evaluation/consolidation/results/<date>_<time>_<NAME>.json`. Commit it.
* `uv run datasources-eval compare` prints the leaderboard of all saved runs. Only compare
  runs over the same batches.

## If it fails

* "Docker is not running": start Docker Desktop, run `make db` again.
* Embedding model "not available locally": run the two once-only commands in steps 2 and 3.
* "`<NAME>` is not set": the error names the variable. Add it to `.env`.
* HTTP 429: you hit the provider's rate limit. Run fewer batches or wait a minute.
