"""Gemini through Google's OpenAI-compatible endpoint, with the production comparison prompt.

Needs GEMINI_API_KEY in SystemCode/.env. The same backend talks to any OpenAI-compatible
API: OpenAI itself (BASE_URL "https://api.openai.com/v1", OPENAI_API_KEY) or a local
server (see ollama_local.py). Change MODEL to the one you use.

    uv run python evaluation/consolidation/experiments/gemini.py        (from SystemCode/)
"""

from datasources.llm_openai import OpenAICompatibleComparisonBackend

NAME = "gemini"
DESCRIPTION = "Gemini (OpenAI-compatible endpoint), the production comparison prompt"

# ----------------------------------------------------------------------------- run config: edit, then run this file
BATCHES = [1]  # batch numbers: 1 = tests 001-005 ... 25 = tests 121-125; [] = all 25. Paid model: start small.
SAVE = True  # write evaluation/consolidation/results/<date>_<time>_<NAME>.json
KEEP_DB = False  # keep each batch's test database (stock_news_eval_..._test) to inspect it

BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
MODEL = "gemini-2.5-flash"
API_KEY_ENV = "GEMINI_API_KEY"  # the name of the variable in .env, not the key itself
JSON_MODE = "json_schema"  # "json_object" or "prompt" if the server rejects JSON schemas


def build_comparator(settings):
    return OpenAICompatibleComparisonBackend(base_url=BASE_URL, model=MODEL, api_key_env=API_KEY_ENV,
                                             json_mode=JSON_MODE, max_tokens=settings.llm_max_output_tokens,
                                             timeout=settings.llm_timeout_seconds)


# ----------------------------------------------------------------------------- run
if __name__ == "__main__":
    from datasources.evaluation import run_experiment

    run_experiment(NAME, build_comparator, description=DESCRIPTION, batches=BATCHES, save=SAVE, keep_db=KEEP_DB,
                   source=__file__)
