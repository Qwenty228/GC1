"""Claude Opus 5.5 at low effort through the Anthropic API: the pipeline's own comparison backend.

Needs ANTHROPIC_API_KEY in SystemCode/.env. The model and effort are pinned here, so this file
ignores SIMILARITY_COMPARISON_MODEL / SIMILARITY_COMPARISON_EFFORT from .env; every other setting
(timeouts, retries, max output tokens, retrieval settings) still comes from .env.

Opus 5.5 always thinks (thinking cannot be disabled); effort is the only depth control and its
default is "medium", so it is set explicitly. Thinking tokens count towards max_tokens: if an
answer is cut off (error kind "max_tokens"), raise SIMILARITY_LLM_MAX_OUTPUT_TOKENS in .env.
Refusals use the server-side fallback (SIMILARITY_COMPARISON_FALLBACKS, default "default").

    uv run python evaluation/consolidation/experiments/opus.py        (from SystemCode/)
"""

from dataclasses import replace

from datasources.similarity.comparison import AnthropicComparisonBackend

NAME = "claude-opus-5-5-low"  # letters, digits, . _ - ; used in the results file name
DESCRIPTION = "Claude Opus 5.5 at low effort (Anthropic API), the production comparison prompt"

# ----------------------------------------------------------------------------- run config: edit, then run this file
BATCHES = [1]  # batch numbers: 1 = tests 001-005 ... 25 = tests 121-125; [] = all 25. Paid model: start small.
SAVE = True  # write evaluation/consolidation/results/<date>_<time>_<NAME>.json
KEEP_DB = False  # keep each batch's test database (stock_news_eval_..._test) to inspect it
RESUME = None  # a saved run to finish, e.g. "2026-10-24_1709_gemini": keeps its completed batches
VERBOSE = False  # True: log every stage as it happens (database, article, judgment, HTTP call)

MODEL = "claude-opus-5-5"
EFFORT = "low"  # low | medium | high | xhigh | max


def build_comparator(settings):
    return AnthropicComparisonBackend(replace(settings, comparison_model=MODEL, comparison_effort=EFFORT))


# ----------------------------------------------------------------------------- run
if __name__ == "__main__":
    from datasources.evaluation import run_experiment

    run_experiment(NAME, build_comparator, description=DESCRIPTION, batches=BATCHES, save=SAVE, keep_db=KEEP_DB,
                   source=__file__, resume=RESUME, verbose=VERBOSE)
