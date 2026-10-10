"""A local model through Ollama (or LM Studio / vLLM), with the production comparison prompt.

Free and offline: start the server and pull the model first, e.g.
    ollama pull qwen3:14b && ollama serve
LM Studio: BASE_URL "http://localhost:1234/v1". vLLM: "http://localhost:8000/v1".

Small local models often struggle with the long structured answer; JSON_MODE
"json_object" (schema in the prompt) is the most widely supported.

    uv run python evaluation/consolidation/experiments/ollama_local.py        (from SystemCode/)
"""

from datasources.llm_openai import OpenAICompatibleComparisonBackend

NAME = "ollama-qwen3-14b"
DESCRIPTION = "Local Qwen3 14B via Ollama, the production comparison prompt"

# ----------------------------------------------------------------------------- run config: edit, then run this file
BATCHES = [1]  # batch numbers: 1 = tests 001-005 ... 25 = tests 121-125; [] = all 25. Paid model: start small.
SAVE = True  # write evaluation/consolidation/results/<date>_<time>_<NAME>.json
KEEP_DB = False  # keep each batch's test database (stock_news_eval_..._test) to inspect it

BASE_URL = "http://localhost:11434/v1"
MODEL = "qwen3:14b"
API_KEY_ENV = None  # local servers need no key
JSON_MODE = "json_object"


def build_comparator(settings):
    return OpenAICompatibleComparisonBackend(base_url=BASE_URL, model=MODEL, api_key_env=API_KEY_ENV,
                                             json_mode=JSON_MODE, max_tokens=settings.llm_max_output_tokens,
                                             timeout=max(settings.llm_timeout_seconds, 600.0))


# ----------------------------------------------------------------------------- run
if __name__ == "__main__":
    from datasources.evaluation import run_experiment

    run_experiment(NAME, build_comparator, description=DESCRIPTION, batches=BATCHES, save=SAVE, keep_db=KEEP_DB,
                   source=__file__)
