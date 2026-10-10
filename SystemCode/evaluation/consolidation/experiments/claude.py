"""Claude through the Anthropic API: the pipeline's own comparison backend.

Needs ANTHROPIC_API_KEY in SystemCode/.env. MODEL = None uses SIMILARITY_COMPARISON_MODEL
from .env; set it here to try another Claude model without touching .env.

    uv run python evaluation/consolidation/experiments/claude.py        (from SystemCode/)
"""

from dataclasses import replace

from datasources.similarity.comparison import AnthropicComparisonBackend

NAME = "claude"
DESCRIPTION = "Claude (Anthropic API), the production comparison prompt"

# ----------------------------------------------------------------------------- run config: edit, then run this file
BATCHES = [1]  # batch numbers: 1 = tests 001-005 ... 25 = tests 121-125; [] = all 25. Paid model: start small.
SAVE = True  # write evaluation/consolidation/results/<date>_<time>_<NAME>.json
KEEP_DB = False  # keep each batch's test database (stock_news_eval_..._test) to inspect it

MODEL = None  # e.g. "claude-sonnet-5-5"; None = SIMILARITY_COMPARISON_MODEL from .env


def build_comparator(settings):
    return AnthropicComparisonBackend(replace(settings, comparison_model=MODEL) if MODEL else settings)


# ----------------------------------------------------------------------------- run
if __name__ == "__main__":
    from datasources.evaluation import run_experiment

    run_experiment(NAME, build_comparator, description=DESCRIPTION, batches=BATCHES, save=SAVE, keep_db=KEEP_DB,
                   source=__file__)
