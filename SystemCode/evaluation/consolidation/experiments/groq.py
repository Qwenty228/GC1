"""Groq (free tier available) through its OpenAI-compatible endpoint, with the production comparison prompt.

Needs GROQ_API_KEY in SystemCode/.env (create one at https://console.groq.com/keys).
Pick any chat model Groq lists at https://console.groq.com/docs/models and put its id in MODEL.

Free-tier notes:
  * Rate limits are per minute (requests and tokens). One comparison prompt is several
    thousand tokens, so run ONE batch first; a 429 "rate limit" error means wait a minute
    or use a model with a higher tokens-per-minute limit.
  * JSON_MODE "json_object" works on every Groq chat model. "json_schema" (stricter) only on
    the models Groq lists as supporting structured outputs; switch to it if yours does.

    uv run python evaluation/consolidation/experiments/groq.py        (from SystemCode/)
"""

from datasources.llm_openai import OpenAICompatibleComparisonBackend

NAME = "groq-llama-3.3-70b"
DESCRIPTION = "Llama 3.3 70B on Groq (OpenAI-compatible API), the production comparison prompt"

# ----------------------------------------------------------------------------- run config: edit, then run this file
BATCHES = [1]  # batch numbers: 1 = tests 001-005 ... 25 = tests 121-125; [] = all 25. Paid model: start small.
SAVE = True  # write evaluation/consolidation/results/<date>_<time>_<NAME>.json
KEEP_DB = False  # keep each batch's test database (stock_news_eval_..._test) to inspect it

BASE_URL = "https://api.groq.com/openai/v1"
MODEL = "llama-3.3-70b-versatile"
API_KEY_ENV = "GROQ_API_KEY"  # the name of the variable in .env, not the key itself
JSON_MODE = "json_object"  # schema goes in the prompt; "json_schema" for models with structured outputs
MAX_OUTPUT_TOKENS = 8000  # the answer is short; stays under Groq's per-model output caps


def build_comparator(settings):
    return OpenAICompatibleComparisonBackend(base_url=BASE_URL, model=MODEL, api_key_env=API_KEY_ENV,
                                             json_mode=JSON_MODE, max_tokens=MAX_OUTPUT_TOKENS,
                                             timeout=settings.llm_timeout_seconds)


# ----------------------------------------------------------------------------- run
if __name__ == "__main__":
    from datasources.evaluation import run_experiment

    run_experiment(NAME, build_comparator, description=DESCRIPTION, batches=BATCHES, save=SAVE, keep_db=KEEP_DB,
                   source=__file__)
