"""The local summary model, run through Ollama: every test event summarized, for people to judge.

What a run does (see src/datasources/evaluation/summarization.py)
-----------------------------------------------------------------
All 125 test articles go into ONE throwaway database, through the real ingestion pipeline. Then every
event gets one summary from MODEL, through the production prompt, schema and citation checks. The result
file keeps each summary next to everything the model was given, plus a score sheet for the human rating.

Grouping (GROUPING below)
    "answer_key"  (default) the compare step is answered from the test file names, so every event holds
                  exactly the articles it should. A bad summary is then the summary model's fault, not
                  grouping's. Fast: no model call for grouping.
    "model"       the same local model also groups (end-to-end, like production). Slow (~2 h for 125
                  articles on the 6 GB laptop) and its grouping mistakes (conflicts labelled as updates...)
                  end up in the summaries.

Setup (PowerShell), once: Ollama installed, `ollama pull qwen2.5:7b-instruct`, and the 16k context the
consolidation runs needed (summary prompts are ~5-8k tokens):
    setx OLLAMA_CONTEXT_LENGTH 16384          # then restart Ollama
    make db                                    # Docker running; the test database server

Run (from SystemCode/)
    uv run python evaluation/summarization/experiments/local.py      # LIMIT_EVENTS = 3 first (~5 min)
    uv run datasources-eval report <the printed run name>             # read it; --passages for the input
    uv run datasources-eval compare summarization                     # leaderboard

Then rate it: open evaluation/summarization/results/<run>_scores.csv (see evaluation/summarization/README.md).
"""

from datasources.llm_openai import OpenAICompatibleComparisonBackend, OpenAICompatibleSummaryBackend

NAME = "local-qwen2.5-7b"
DESCRIPTION = "Local Qwen2.5 7B Instruct via Ollama, the production summary prompt"

# ----------------------------------------------------------------------------- run config: edit, then run this file
LIMIT_EVENTS = 3  # summarize only the first N events (a quick check); None = all (~50 events)
GROUPING = "answer_key"  # "answer_key" (isolates the summary model) or "model" (end-to-end, slow)
ATTEMPTS = 2  # tries per event when a summary fails the citation checks (production retries it later)
SAVE = True  # write evaluation/summarization/results/<date>_<time>_<NAME>.json and <...>_scores.csv
KEEP_DB = False  # keep the test database (stock_news_eval_..._test) to inspect it

BASE_URL = "http://localhost:11434/v1"
MODEL = "qwen2.5:7b-instruct"  # copies quotes verbatim, which the citation check needs (see consolidation findings)
API_KEY_ENV = None  # local servers need no key
JSON_MODE = "json_schema"  # grammar-constrained output; "json_object" if the server rejects the schema
MAX_TOKENS = 4096  # a summary is a few hundred tokens; the cap stops a rambling model early


def build_summarizer(settings):
    return OpenAICompatibleSummaryBackend(base_url=BASE_URL, model=MODEL, api_key_env=API_KEY_ENV,
                                          json_mode=JSON_MODE, max_tokens=MAX_TOKENS,
                                          timeout=max(settings.llm_timeout_seconds, 600.0))


def build_comparator(settings):
    """Only used with GROUPING = "model"."""
    return OpenAICompatibleComparisonBackend(base_url=BASE_URL, model=MODEL, api_key_env=API_KEY_ENV,
                                             json_mode=JSON_MODE, max_tokens=settings.llm_max_output_tokens,
                                             timeout=max(settings.llm_timeout_seconds, 600.0))


# ----------------------------------------------------------------------------- run
if __name__ == "__main__":
    from datasources.evaluation.summarization import run_experiment

    run_experiment(NAME, build_summarizer, build_comparator=build_comparator if GROUPING == "model" else None,
                   description=DESCRIPTION, save=SAVE, keep_db=KEEP_DB, attempts=ATTEMPTS,
                   limit_events=LIMIT_EVENTS, source=__file__)
