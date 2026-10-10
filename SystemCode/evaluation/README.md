# evaluation/

Run the pipeline on the labelled test data and score it, with **your own model plugged
in**. One experiment file = one model setup; everyone can try a different model and the
results sit side by side.

```
evaluation/
  consolidation/                 stage 1: event grouping + relation labels  (ready)
    experiments/
      baseline_rule.py           TEMPLATE: rule-based, always the same answer. Copy this.
      claude.py                  Claude (Anthropic API), the production prompt
      gemini.py                  Gemini (OpenAI-compatible API), the production prompt
      groq.py                    Groq (OpenAI-compatible API), free tier
      ollama_local.py            local model via Ollama / LM Studio / vLLM
    results/                     one JSON per run: commit it
  summarization/                 stage 2: event summaries  (slot, see its README)
```

## Run one

```bash
cd SystemCode
make sync-local-embeddings                       # once: the local embedding model's packages
uv run datasources-similarity download-model     # once: the pinned embedding model
make db-up                                       # Postgres must be running (test databases are made on it)

uv run python evaluation/consolidation/experiments/baseline_rule.py   # every experiment file is runnable
uv run python evaluation/consolidation/experiments/groq.py
uv run datasources-eval compare                                      # leaderboard of every saved run
```

Each file starts with its own run config; edit it, then run the file:

```python
BATCHES = [1]     # batch numbers: 1 = tests 001-005 ... 25 = tests 121-125; [] = all 25
SAVE = True       # write results/<date>_<time>_<NAME>.json
KEEP_DB = False   # keep each batch's test database to inspect it
```

Start with one batch: a paid model costs one call per article after the first. Every
file ends with the same `if __name__ == "__main__":` block calling `run_experiment(...)`,
the one shared runner (`src/datasources/evaluation/consolidation.py`), so the pipeline is
run identically for everyone; only the model and the run config differ.

## Plug in your model

```bash
cp evaluation/consolidation/experiments/baseline_rule.py evaluation/consolidation/experiments/my_model.py
```

Then edit `NAME` and the block marked **PUT YOUR MODEL HERE**. The file explains what you
receive and what to return. Two ways to fill it:

* **Per pair:** fill `decide(segment, event)` and return one of the five labels
  (`same_event_repeated`, `same_event_update`, `same_event_conflict`,
  `different_event`, `uncertain`). Simplest; any classifier or prompt works.
* **Whole request:** replace `compare(request)`; `request.render()` is the exact prompt
  the production model gets. For any OpenAI-compatible API (OpenAI, Gemini, Groq, Ollama,
  LM Studio, vLLM) just copy `groq.py`, `gemini.py` or `ollama_local.py` and change three
  lines (`BASE_URL`, `MODEL`, `API_KEY_ENV`).

Keys go in `SystemCode/.env` (e.g. `GROQ_API_KEY=...`); experiment files only name the
variable. Optionally define `build_embedder(settings)` and pass it to `run_experiment` to
swap the embedding model too.

## What a run does

For each batch of five (`populate/event_grouping/`, answer key in `evaluation.fixture_file`):

1. Creates a throwaway database `stock_news_eval_<name>_bNN_..._test`, migrates it and
   registers the coverage companies. Batches never see each other's events, and
   `stock_news` is never touched. The database is dropped afterwards.
2. Feeds the five articles in test-number order through the **real** ingestion pipeline
   (store, embed, retrieve candidates within ±7 days, compare, group) with your
   comparator plugged in. Only the article goes in, never its file name.
3. Reads back each article's event and relation label and compares with the answer key.

Scores (the team's agreed metrics):

* **Grouping, pairwise F1:** every pair inside a batch is "same event" or not, predicted
  vs expected. Membership only, never event ids.
* **Labels, macro F1:** repeat / update / conflict / different, for every article
  except the first of its batch; each label counts equally; `uncertain` is never correct.

Reference points (batches 1 and 21): the always-"same event" baseline gets grouping F1
0.75 (perfect recall, merges the "different" article) and label macro F1 0.10. A real
model should beat both clearly.

## Results

Every run writes `consolidation/results/<date>_<time>_<NAME>.json`: the experiment,
model, prompt version, embedding model, retrieval settings, per-article predictions,
both scores, model calls and tokens. Commit it. `uv run datasources-eval compare`
ranks all saved runs. Only compare runs over the **same batches**.
