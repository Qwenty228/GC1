"""Claude through the Anthropic API: the pipeline's own comparison backend.

Needs ANTHROPIC_API_KEY in SystemCode/.env. MODEL = None uses SIMILARITY_COMPARISON_MODEL
from .env; set it here to try another Claude model without touching .env.

    uv run datasources-eval consolidation evaluation/consolidation/experiments/claude.py --batch 1 --yes
"""

from dataclasses import replace

from datasources.similarity.comparison import AnthropicComparisonBackend

NAME = "claude"
DESCRIPTION = "Claude (Anthropic API), the production comparison prompt"
PAID_CALLS = True

MODEL = None  # e.g. "claude-sonnet-5-5"; None = SIMILARITY_COMPARISON_MODEL from .env


def build_comparator(settings):
    return AnthropicComparisonBackend(replace(settings, comparison_model=MODEL) if MODEL else settings)
