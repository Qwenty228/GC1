"""TEMPLATE experiment: a rule-based baseline. Copy this file to plug in your own model.

    cp evaluation/consolidation/experiments/baseline_rule.py evaluation/consolidation/experiments/my_model.py
    # edit NAME, the run config, and the "PUT YOUR MODEL HERE" part
    uv run python evaluation/consolidation/experiments/my_model.py        (from SystemCode/)

How it fits together
--------------------
Running this file calls `run_experiment(...)` (bottom), the one shared runner. For each
batch it creates a throwaway test database and pushes the five articles through the REAL
pipeline (ingest/graph.py: store, embed, retrieve candidate events within ±7 days, compare,
group). At the compare step the pipeline calls YOUR object: `comparator.compare(request)`.
Everything else is unchanged production code, so the score reflects your model.

Your answer per (incoming article segment, candidate event) pair decides grouping:

    same_event_repeated   joins the event: same facts, nothing new
    same_event_update     joins the event: new facts or a later development
    same_event_conflict   joins the event, flagged: a contradictory claim
    different_event       opens a new event
    uncertain             opens a new event, linked as "pending"

An article with no candidates (the first of a batch, or nothing similar retrieved) never
reaches `compare`: it opens a new event. The file name, which holds the answer key, is
never shown to the model.

This baseline gives the same label for every pair: it measures "always say yes, same
event" (or, with ALWAYS = "different_event", "always say no"). Any real model should beat it.
"""

from datasources.similarity.comparison import ComparisonCall, ComparisonRequest, build_output

NAME = "baseline-rule"  # letters, digits, . _ - ; used in the results file name
DESCRIPTION = "Rule baseline: every candidate pair gets the same constant label"

# ----------------------------------------------------------------------------- run config: edit, then run this file
BATCHES = [1]  # batch numbers: 1 = tests 001-005 ... 25 = tests 121-125; [] = all 25
SAVE = True  # write evaluation/consolidation/results/<date>_<time>_<NAME>.json
KEEP_DB = False  # keep each batch's test database (stock_news_eval_..._test) to inspect it

ALWAYS = "same_event_repeated"  # the constant answer; try "different_event" for the opposite baseline


# ----------------------------------------------------------------------------- the model
def decide(segment, event) -> str:
    """One decision: does the incoming `segment` belong to candidate `event`? Return a label above.

    ----------------------------------------------------------------------------------------
    PUT YOUR MODEL HERE (simplest way: one call per pair).

    What you get:
        segment.units          incoming text, a list of units; unit.text is a paragraph or chunk
        event.title            the candidate event's title
        event.passages         its evidence; passage.text, passage.source
        event.first_reported   datetime of its earliest article
        event.facts            dict: participants, action, object, figures, ... (may be empty)

    Example (pseudo-code):
        prompt = f"Incoming: {segment.units[0].text}\\nEvent: {event.title}\\n{event.passages[0].text}"
        return my_llm(prompt)          # must return one of the five labels
    ----------------------------------------------------------------------------------------
    """
    return ALWAYS


class RuleComparator:
    """The object the pipeline talks to: needs `name`, `model` and `compare(request)`."""

    name = "rule"
    model = f"always-{ALWAYS}"

    def compare(self, request: ComparisonRequest) -> ComparisonCall:
        # Want the whole request at once instead of one pair at a time (one LLM call per
        # article, like the Claude backend)? `request.render()` is the full prompt text the
        # production model gets; `request.pairs` maps each segment to its candidate events.
        # For any OpenAI-compatible API, copy groq.py / gemini.py instead.
        #
        # build_output turns your per-pair labels into the structured answer the pipeline
        # validates, including short verbatim evidence quotes from both sides (a "same
        # event" label without verifiable quotes is downgraded to uncertain).
        output = build_output(request, decide)
        return ComparisonCall(output, self.model, usage={"calls": 0}, stop_reason="rule")


def build_comparator(settings):
    """Called once per run. `settings` holds the project's SIMILARITY_* values (.env); optional to use."""
    return RuleComparator()


# OPTIONAL: swap the embedding model too (default: SIMILARITY_EMBEDDING_* from .env). Then pass
# build_embedder=build_embedder to run_experiment below.
# def build_embedder(settings):
#     from dataclasses import replace
#     from datasources.similarity.embeddings import build_embedding_backend
#     return build_embedding_backend(replace(settings, embedding_model="BAAI/bge-base-en-v1.5"))


# ----------------------------------------------------------------------------- run
if __name__ == "__main__":
    from datasources.evaluation import run_experiment

    run_experiment(NAME, build_comparator, description=DESCRIPTION, batches=BATCHES, save=SAVE, keep_db=KEEP_DB,
                   source=__file__)
