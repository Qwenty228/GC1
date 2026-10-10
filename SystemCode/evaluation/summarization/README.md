# summarization/ (stage 2, not built yet)

Will evaluate the event-summary LLM the same way `consolidation/` evaluates grouping:
experiment files in `experiments/`, results in `results/`, `datasources-eval summarization`.

Already in place:

* The summary step takes a backend object with `summarize(request)`, so models are
  swappable: `datasources.summary.generation.AnthropicSummaryBackend` (Claude) and
  `datasources.llm_openai.OpenAICompatibleSummaryBackend` (OpenAI, Gemini, Ollama, LM
  Studio, vLLM), with the same prompt and output schema.

Still needed before a runner makes sense:

* Labelled data: for each test event, what a correct summary must contain (key facts,
  both sides of a conflict, citations) or a reference summary to compare against.
* Agreed metrics, e.g. fact coverage, unsupported-claim rate, citation validity
  (the pipeline already validates citations), conflict handling.
