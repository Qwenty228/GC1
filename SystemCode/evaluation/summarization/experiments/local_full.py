"""Full-detail evaluation with the local model (Ollama): grouping (step 3) + summaries and validation (step 4).

Everything runs in throwaway databases `stock_news_eval_..._test`; the main database is never touched.
The machinery is src/datasources/evaluation/recording.py; this file is the configuration and the command line.

Modes
-----
real      a slice of REAL NVDA news + the 8-K, from populate/003_nvda_8k_windows_2026-07-08.sql (no answer key).
          Default slice aug17 (Aug 15-20: ~58 articles + 8-K 0001045810-26-000069). Run twice by default:
            production  the retrieval filter from .env (gate, top-k, shortlist), as the system runs
            no_filter   every event in the time window sent to the LLM: answers "does the filter only save cost,
                        or does it change the result?" (a filter-comparison file lists every difference)
          Conflicts: production holds an event's summary while a conflict review is open. Real data has no reviewer,
          so the run records the held state, then (CONFLICTS="assume_confirmed") resolves it as an ASSUMED
          confirmed conflict so the summary is generated and must state both claims. "hold" = production behaviour.
fixtures  the 125 test articles (populate/event_grouping), one database, through the existing summarization runner,
          plus the same full detail. GROUPING "model" = Qwen groups them (end to end, ~2 h); "answer_key" = grouping
          from the file names, so only the summaries are tested.

What is saved (evaluation/summarization/results/)
-------------------------------------------------
<run>.json                    articles (text, text units, retrieval scores, LLM judgments + explanations, memberships,
                              timings), events (members, conflict state, summary attempts, validation errors, passages
                              given to the model, citations checked, published version), label-free metrics, review lists
calls/<run>.json              every LLM call: full prompt text, raw response (+ reasoning if the model returns it),
                              parsed output, tokens, latency, errors
<run>_scores.csv              human rubric for the summaries (evaluation/summarization/README.md)
<run>_grouping_labels.csv     real mode: label which articles are the same event (true_event) by hand
<...>_filter-comparison.json  real mode with both variants: links each made that the other did not, and the cost

Windows (PowerShell, from SystemCode/), once
--------------------------------------------
    git pull
    uv sync --locked --extra local-embeddings          # local bge-small embedder
    uv run datasources-similarity download-model        # the embedding model files
    ollama pull qwen2.5:7b-instruct
    setx OLLAMA_CONTEXT_LENGTH 16384                    # then quit and restart Ollama (prompts are 5-10k tokens)
    make db                                             # Docker running; also loads populate/003 into the main DB (optional)

Run
---
    uv run python evaluation/summarization/experiments/local_full.py --mode real --limit-articles 5 --limit-events 2   # smoke test, ~10-20 min
    uv run python evaluation/summarization/experiments/local_full.py --mode real                                       # aug17, both variants (hours)
    uv run python evaluation/summarization/experiments/local_full.py --mode real --filters production                  # one variant only
    uv run python evaluation/summarization/experiments/local_full.py --mode fixtures --grouping answer_key              # 125 tests, summaries only
    uv run python evaluation/summarization/experiments/local_full.py --mode fixtures --grouping model                  # 125 tests end to end
    uv run datasources-eval report <fixtures run name>                                                                 # read a fixtures run
    ollama ps                                                                                                          # is the model on the GPU?

`--backend offline` replaces both models with deterministic stand-ins (no Ollama): a plumbing check only.
"""

import argparse

from datasources.llm_openai import OpenAICompatibleComparisonBackend, OpenAICompatibleSummaryBackend

NAME = "local-qwen2.5-7b-full"
DESCRIPTION = "Local Qwen2.5 7B Instruct via Ollama: grouping + summaries with every input, output and check kept"

# ----------------------------------------------------------------------------- run config: edit, or use the flags
MODE = "real"  # "real" | "fixtures"
SLICE = "aug17"  # real mode: "jul2" | "aug17" | "aug26" | "all"
FILTERS = ["production", "no_filter"]  # real mode: one or both variants, run in this order
GROUPING = "answer_key"  # fixtures mode: "answer_key" | "model"
CONFLICTS = "assume_confirmed"  # real mode: "assume_confirmed" | "hold"
LIMIT_ARTICLES = None  # real mode: only the first N news articles of the slice (the 8-K is always kept)
LIMIT_EVENTS = None  # summarize only the first N events
ATTEMPTS = 2  # tries per event when a summary fails validation (production retries it later)
SAVE = True  # write the result files to evaluation/summarization/results/
KEEP_DB = False  # keep the test database(s) (stock_news_eval_..._test) to inspect them

BASE_URL = "http://localhost:11434/v1"
MODEL = "qwen2.5:7b-instruct"  # copies quotes verbatim, which both validators need (see consolidation findings)
JSON_MODE = "json_schema"  # grammar-constrained output; "json_object" if the server rejects the schema
SUMMARY_MAX_TOKENS = 4096
TIMEOUT = 600.0


def build_comparator(settings, model=MODEL):
    return OpenAICompatibleComparisonBackend(base_url=BASE_URL, model=model, json_mode=JSON_MODE,
                                             max_tokens=settings.llm_max_output_tokens,
                                             timeout=max(settings.llm_timeout_seconds, TIMEOUT))


def build_summarizer(settings, model=MODEL):
    return OpenAICompatibleSummaryBackend(base_url=BASE_URL, model=model, json_mode=JSON_MODE,
                                          max_tokens=SUMMARY_MAX_TOKENS, timeout=max(settings.llm_timeout_seconds, TIMEOUT))


def offline_backends():
    """Deterministic stand-ins for a plumbing check without Ollama (not an evaluation)."""
    from datasources.similarity.comparison import ComparisonCall, build_output
    from datasources.summary.generation import ScriptedSummaryBackend, extractive_output

    class RuleComparator:
        name, model = "rule", "always-same_event_repeated"

        def compare(self, request):
            return ComparisonCall(build_output(request, lambda s, e: "same_event_repeated"), self.model,
                                  usage={"calls": 0}, stop_reason="rule")

    return RuleComparator(), ScriptedSummaryBackend(extractive_output, model="extractive")


def main() -> None:
    from datasources.evaluation.recording import SLICES, VARIANTS, run_experiment

    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--mode", choices=["real", "fixtures"], default=MODE)
    p.add_argument("--slice", choices=sorted(SLICES), default=SLICE)
    p.add_argument("--filters", default=",".join(FILTERS), help=f"comma-separated: {', '.join(VARIANTS)}")
    p.add_argument("--grouping", choices=["answer_key", "model"], default=GROUPING)
    p.add_argument("--conflicts", choices=["assume_confirmed", "hold"], default=CONFLICTS)
    p.add_argument("--model", default=MODEL)
    p.add_argument("--limit-articles", type=int, default=LIMIT_ARTICLES)
    p.add_argument("--limit-events", type=int, default=LIMIT_EVENTS)
    p.add_argument("--attempts", type=int, default=ATTEMPTS)
    p.add_argument("--keep-db", action="store_true", default=KEEP_DB)
    p.add_argument("--no-save", action="store_true", help="print only, write no result files")
    p.add_argument("--backend", choices=["ollama", "offline"], default="ollama")
    a = p.parse_args()

    if a.backend == "offline":
        comparator, summarizer = offline_backends()
        name, build_s, build_c = f"{NAME}-offline", (lambda s: summarizer), (lambda s: comparator)
    else:
        name = NAME if a.model == MODEL else f"{NAME}-{a.model.replace(':', '-').replace('/', '-')}"
        build_s, build_c = (lambda s: build_summarizer(s, a.model)), (lambda s: build_comparator(s, a.model))
    run_experiment(name, build_s, build_c, mode=a.mode, slice_key=a.slice,
                   variants=[v.strip() for v in a.filters.split(",") if v.strip()], grouping=a.grouping,
                   conflicts=a.conflicts, limit_articles=a.limit_articles, limit_events=a.limit_events,
                   attempts=a.attempts, save=SAVE and not a.no_save, keep_db=a.keep_db, description=DESCRIPTION,
                   source=__file__)


if __name__ == "__main__":
    main()
