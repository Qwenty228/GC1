"""Gemini through Google's OpenAI-compatible endpoint, with the production comparison prompt.

Needs GEMINI_API_KEY in SystemCode/.env. The same backend talks to any OpenAI-compatible
API: OpenAI itself (BASE_URL "https://api.openai.com/v1", OPENAI_API_KEY) or a local
server (see ollama_local.py). Change MODEL to the one you use.

    uv run datasources-eval consolidation evaluation/consolidation/experiments/gemini.py --batch 1 --yes
"""

from datasources.llm_openai import OpenAICompatibleComparisonBackend

NAME = "gemini"
DESCRIPTION = "Gemini (OpenAI-compatible endpoint), the production comparison prompt"
PAID_CALLS = True

BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
MODEL = "gemini-2.5-flash"
API_KEY_ENV = "GEMINI_API_KEY"  # the name of the variable in .env, not the key itself
JSON_MODE = "json_schema"  # "json_object" or "prompt" if the server rejects JSON schemas


def build_comparator(settings):
    return OpenAICompatibleComparisonBackend(base_url=BASE_URL, model=MODEL, api_key_env=API_KEY_ENV,
                                             json_mode=JSON_MODE, max_tokens=settings.llm_max_output_tokens,
                                             timeout=settings.llm_timeout_seconds)
