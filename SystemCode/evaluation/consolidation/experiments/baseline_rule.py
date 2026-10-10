"""TEMPLATE experiment: a rule-based baseline. Copy this file to plug in your own model.

    cp evaluation/consolidation/experiments/baseline_rule.py evaluation/consolidation/experiments/my_model.py
    # edit NAME, then the marked "PUT YOUR MODEL HERE" part
    uv run datasources-eval consolidation evaluation/consolidation/experiments/my_model.py --batch 1

What the evaluation does with this file
---------------------------------------
The real pipeline runs unchanged: it stores each article, embeds it, and retrieves the
events it might belong to (published within ±7 days, same company). Only the step that
DECIDES the relation is yours: `compare(request)` below. For every incoming article that
has candidate events, the pipeline calls it once, and your answer decides grouping:

    same_event_repeated   joins the event: same facts, nothing new
    same_event_update     joins the event: new facts or a later development
    same_event_conflict   joins the event, flagged: a contradictory claim
    different_event       opens a new event
    uncertain             opens a new event, linked as "pending"

An article with no candidates (the first of a batch, or nothing similar retrieved) never
reaches `compare`: it opens a new event. The file name, which holds the answer key, is
never shown to you.

This baseline answers the same label for every pair, so it measures what "always say
yes, same event" (or, with ALWAYS = "different_event", "always say no") would score.
Any real model should beat both.
"""

from datasources.similarity.comparison import ComparisonCall, ComparisonRequest, build_output

NAME = "baseline-rule"  # letters, digits, . _ - ; used in the results file name
DESCRIPTION = "Rule baseline: every candidate pair is labelled the same, constant answer"
PAID_CALLS = False  # True if your model costs money per call (then the runner asks for --yes)

ALWAYS = "same_event_repeated"  # the constant answer; try "different_event" for the opposite baseline


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
        # pipeline's own model gets, and `request.pairs` maps each segment to its candidate
        # events. Replace the next line with your call; see claude.py / gemini.py for that.
        #
        # build_output turns your per-pair labels into the structured answer the pipeline
        # validates, including short verbatim evidence quotes from both sides (a "same
        # event" label without verifiable quotes is downgraded to uncertain).
        output = build_output(request, decide)
        return ComparisonCall(output, self.model, usage={"calls": 0}, stop_reason="rule")


def build_comparator(settings):
    """Called once per run. `settings` holds the project's SIMILARITY_* values (.env); optional."""
    return RuleComparator()


# OPTIONAL: swap the embedding model too (default: SIMILARITY_EMBEDDING_* from .env).
# def build_embedder(settings):
#     from datasources.similarity.embeddings import build_embedding_backend
#     from dataclasses import replace
#     return build_embedding_backend(replace(settings, embedding_model="BAAI/bge-base-en-v1.5"))
