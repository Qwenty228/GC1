"""The best *local* model for the consolidation comparison, run through Ollama, as a head-to-head
against the cloud models (claude.py, gemini.py, groq.py) on the exact same prompt and schema.

Chosen model: Qwen3 8B (MODEL = "qwen3:8b").

Why this model, for THIS task, on THIS machine
-----------------------------------------------
What the task demands (see src/datasources/similarity/comparison.py):
  * A ~2K-token rules prompt plus a deeply nested JSON schema (segments -> judgments ->
    evidence / differences / figures). The backend rejects anything that does not validate,
    so weak instruction-following or messy JSON simply scores zero.
  * Verbatim quote extraction: every evidence quote is checked to occur in the cited source
    text, and a same-event label whose evidence cannot be verified on BOTH sides is downgraded
    to "uncertain". This rewards faithful, low-hallucination models and punishes paraphrasers.
  * A nuanced 5-way decision (repeated / update / conflict / different / uncertain) plus a
    written `explanation` field -- i.e. the model has to reason and justify, not just classify.

What this laptop allows (RTX 3060 Laptop, 6 GB VRAM; 16 GB system RAM):
  * The sweet spot is a 7-8B model at ~Q4 (~5 GB): it fits almost entirely in 6 GB of VRAM
    and runs at a usable speed. 4B is faster but weaker on the nuanced labels; 12-14B spills
    out of VRAM into the (scarce) system RAM and crawls -- fine for a one-off overnight ceiling
    run, wrong as the default.
  * NOTE: Postgres runs in Docker (stock-news-postgres-1), so keep Docker running. Close VS Code,
    Brave and any stray Jupyter kernel before a run to free VRAM/RAM for the model.

Why Qwen3 8B specifically:
  * Best-in-class open model at this size for structured JSON + instruction-following, which is
    exactly what the strict schema and quote-verification need.
  * Reasoning ("thinking") capable, which maps directly onto the task's "explainable" requirement
    (the `explanation` / `matching_facts` / `differences` fields).
  * Fits the 6 GB VRAM budget at Q4, so a full 25-batch run finishes in ~1.5-2 h -- well inside a
    2-day window, leaving time to also try a heavier ceiling model.

Alternatives considered (swap MODEL to benchmark them too):
  * qwen2.5:7b-instruct  -- rock-solid NON-thinking JSON baseline; the clean "no reasoning" control.
  * gemma3:4b            -- fast, fully in VRAM, strong for its size; the open counterpart to Gemini.
  * qwen3:14b / phi4     -- highest open quality but spills to RAM here -> slow; overnight ceiling only.
  * (Rejected) DeepSeek-R1 distills: verbose reasoners that fight strict JSON schemas and wrap
    output in prose -- a poor fit for this quote-validated, schema-strict pipeline.

Thinking vs JSON: Qwen3 defaults to "thinking" on. Recent Ollama keeps the reasoning out of the
returned content, so json_object parsing still works; the chosen explanation lives in the JSON.
If you ever see "response did not match the schema" errors from reasoning leaking into the body,
switch MODEL to "qwen2.5:7b-instruct" (no thinking) or pull a Qwen3 instruct variant.

Setup (PowerShell), then run this file:
    winget install Ollama.Ollama          # once
    ollama pull qwen3:8b                   # ~5 GB download
    ollama serve                           # leave running in its own window
    uv run python evaluation/consolidation/experiments/local.py     (from SystemCode/)
    
Start Check Stop
    ollama ps                            # which model is loaded + GPU/CPU split (shows if it spilled to RAM)
    ollama list                          # downloaded models
    curl.exe http://localhost:11434/api/tags   # is the server up? (winget starts it automatically)
    Get-Process ollama* | Stop-Process   # stop Ollama entirely when done

Run benchmark
    uv run python evaluation/consolidation/experiments/local.py       # BATCHES=[1] sanity check first (~2-3 min)

Watch progress 
    nvidia-smi --query-gpu=memory.used,memory.free,utilization.gpu --format=csv -l 5   # live VRAM + GPU use
    Get-ChildItem evaluation\\consolidation\\results\\*local* | Sort LastWriteTime | Select -Last 1   # newest result file

        

LM Studio / vLLM instead of Ollama: set BASE_URL to "http://localhost:1234/v1" or
"http://localhost:8000/v1". Small local models do best with JSON_MODE "json_object"
(schema in the prompt), which every server supports.
"""

from datasources.llm_openai import OpenAICompatibleComparisonBackend

NAME = "local-qwen2.5-7b"
DESCRIPTION = "Local Qwen2.5 7B Instruct via Ollama, the production comparison prompt"

# ----------------------------------------------------------------------------- run config: edit, then run this file
BATCHES = []  # batch numbers: 1 = tests 001-005 ... 25 = tests 121-125; [] = all 25. Start with [1] to sanity-check.
SAVE = True  # write evaluation/consolidation/results/<date>_<time>_<NAME>.json
KEEP_DB = False  # keep each batch's test database (stock_news_eval_..._test) to inspect it
RESUME = None  # a saved run to finish, e.g. "2026-10-24_1709_gemini": keeps its completed batches
VERBOSE = False  # True: log every stage as it happens (database, article, judgment, HTTP call)

# qwen3:8b reasons well but paraphrases its evidence quotes, so the verbatim-quote gate downgrades them to
# "uncertain"; qwen2.5:7b-instruct is non-thinking and copies spans more literally -> a better fit for the gate.
BASE_URL = "http://localhost:11434/v1"
MODEL = "qwen2.5:7b-instruct"
API_KEY_ENV = None  # local servers need no key
JSON_MODE = "json_schema"  # grammar-constrains the output so small models can't drift off the schema (e.g. a
# status value outside the allowed list); needs Ollama's structured-outputs support. Fall back to "json_object" if
# the server rejects the schema.


def build_comparator(settings):
    return OpenAICompatibleComparisonBackend(base_url=BASE_URL, model=MODEL, api_key_env=API_KEY_ENV,
                                             json_mode=JSON_MODE, max_tokens=settings.llm_max_output_tokens,
                                             timeout=max(settings.llm_timeout_seconds, 1800.0))  # partly on CPU (6 GB VRAM): 600 s timed out now and then


# ----------------------------------------------------------------------------- run
if __name__ == "__main__":
    from datasources.evaluation import run_experiment

    run_experiment(NAME, build_comparator, description=DESCRIPTION, batches=BATCHES, save=SAVE, keep_db=KEEP_DB,
                   source=__file__, resume=RESUME, verbose=VERBOSE)
