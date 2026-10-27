# Local model findings — consolidation step

Goal: pick the best model we can run **on this laptop** (no API cost) to stand next to Claude and
Gemini when we compare how well each one decides whether two news items are about the same event.

All tests below are batch 1 only (5 articles) — a quick sanity check, not the full score.

## The machine is the real limit

This laptop has a small graphics card (6 GB) and 16 GB of memory. That means we can only run
small models (around 7–8 billion parameters). Bigger models technically load, but they spill over
onto the CPU and become very slow. So "pick the best local model" really means "pick the best
*small* model that still behaves."

## What we tried

**Qwen3 8B** — the smarter reasoner.
- It understands the task well. When it explained its answers, it was usually right: it correctly
  spotted repeats, updates, and unrelated stories.
- The problem: our pipeline only trusts a match if the model quotes the article **word for word**.
  Qwen3 tends to reword quotes in its own words, so the system can't verify them and throws the
  answer away (it becomes "uncertain"). So it was right in spirit but kept failing on a technicality.

**Qwen2.5 7B** — the more literal one.
- It copies quotes much more faithfully, which is exactly what the pipeline wants.
- But its judgement is weaker: in one case it saw two conflicting versions of the same story and
  called them "different stories" instead of "a conflict."
- It was also slower here, and one answer took so long it hit the time limit and was dropped.

Bottom line so far: neither small model is close to Claude/Gemini yet. Qwen3 thinks better but
won't quote cleanly; Qwen2.5 quotes cleanly but thinks worse.

## Two settings that mattered a lot

These tripped us up and are worth remembering for any local model:

1. **How much text the model reads at once.** Out of the box the local server only let the model
   read a small amount of text, so our long prompts got cut off and the answers came back broken.
   Raising that limit (we set it to 16k) fixed the broken answers.

2. **Forcing the answer format.** There are two ways to ask for the structured answer. The loose
   way just *describes* the format and hopes the model follows it — small models don't, and the
   answer gets rejected. The strict way *forces* the model to stay in the format. Switching to the
   strict way removed most of those failures. Use the strict format for any local model.

## Recommendation

- Keep **Qwen2.5 7B with the strict answer format and the larger reading limit** as the main local
  entry for now — it at least produces answers the pipeline accepts.
- **Qwen3 8B** is the more promising brain; it's worth a second attempt once we find a way to make
  it copy quotes exactly (e.g. turning off its "thinking" step, which seems to be when it rewords).
- Don't expect a small local model to beat the cloud models on this strict task. The honest value
  of running them is to show *how far* an offline, free option gets — and right now that's "it can
  follow the format, but it's slower and less reliable on the harder calls."

## How to reproduce

From `SystemCode/`, with the local server running:

```
uv run python evaluation/consolidation/experiments/local.py       # runs the model set in local.py
uv run datasources-eval report local-qwen2.5-7b --reasons         # see the answers + why
uv run datasources-eval compare                                   # leaderboard (failed runs are hidden)
```

Runs where the model failed on some articles are tagged "incomplete" and kept as logs, but left out
of the leaderboard so they don't skew the comparison.
