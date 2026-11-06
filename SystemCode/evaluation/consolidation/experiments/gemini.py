"""Gemini through Google's OpenAI-compatible endpoint, with the production comparison prompt.

Needs GEMINI_API_KEY in SystemCode/.env. The same backend talks to any OpenAI-compatible
API: OpenAI itself (BASE_URL "https://api.openai.com/v1", OPENAI_API_KEY) or a local
server (see local.py). Change MODEL to the one you use.

    uv run python evaluation/consolidation/experiments/gemini.py        (from SystemCode/)

Free tiers answer HTTP 429 when a limit is reached: RateLimited (see groq.py) paces the calls and retries a 429.
If a run still stops part-way (quota, outage), set RESUME to its saved file and run again: complete batches are
kept, the others rerun whole.
"""

from datasources.evaluation.rate_limit import RateLimited
from datasources.llm_openai import OpenAICompatibleComparisonBackend

NAME = "gemini"
DESCRIPTION = "Gemini (OpenAI-compatible endpoint), the production comparison prompt"

# ----------------------------------------------------------------------------- run config: edit, then run this file
BATCHES = [1]  # batch numbers: 1 = tests 001-005 ... 25 = tests 121-125; [] = all 25. Paid model: start small.
SAVE = True  # write evaluation/consolidation/results/<date>_<time>_<NAME>.json
KEEP_DB = False  # keep each batch's test database (stock_news_eval_..._test) to inspect it
RESUME = None  # a saved run to finish, e.g. "2026-10-24_1709_gemini": keeps its completed batches
VERBOSE = False  # True: log every stage as it happens (database, article, judgment, HTTP call)

BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
MODEL = "gemini-3.8-flash"  # the 125-article run (results/gemini_125_completed.json)
API_KEY_ENV = "GEMINI_API_KEY"  # the name of the variable in .env, not the key itself
JSON_MODE = "json_schema"  # "json_object" or "prompt" if the server rejects JSON schemas
TIMEOUT_SECONDS = 600.0  # some comparisons took gemini-3.8-flash over 180 s; None = SIMILARITY_LLM_TIMEOUT_SECONDS

REQUESTS_PER_MINUTE = 10  # check your project's limits in Google AI Studio; None = no limit
TOKENS_PER_MINUTE = None  # None = no limit


def build_comparator(settings):
    backend = OpenAICompatibleComparisonBackend(base_url=BASE_URL, model=MODEL, api_key_env=API_KEY_ENV,
                                                json_mode=JSON_MODE, max_tokens=settings.llm_max_output_tokens,
                                                timeout=TIMEOUT_SECONDS or settings.llm_timeout_seconds)
    return RateLimited(backend, requests_per_minute=REQUESTS_PER_MINUTE, tokens_per_minute=TOKENS_PER_MINUTE)


# ----------------------------------------------------------------------------- run
if __name__ == "__main__":
    from datasources.evaluation import run_experiment

    run_experiment(NAME, build_comparator, description=DESCRIPTION, batches=BATCHES, save=SAVE, keep_db=KEEP_DB,
                   source=__file__, resume=RESUME, verbose=VERBOSE)
