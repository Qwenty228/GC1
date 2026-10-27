# Gemini Consolidation: Two Uncertain Outcomes

*StockEvent Lens \| BGE-small embeddings, Python thresholds, Gemini comparison \| Evidence-validation downgrades (companion to “Ten Misclassifications”)*

> **Colour key:** **<mark style="background-color:#00FF00;color:#000000">GREEN</mark>** = facts Gemini identified correctly; **<mark style="background-color:#FFFF00;color:#000000">YELLOW</mark>** = where the uncertainty came from; **<mark style="background-color:#00FFFF;color:#000000">CYAN</mark>** = wording in Gemini's notes that does not appear in the cited source passage.

# Executive evaluation

Besides the 10 misclassifications, the completed run (25 batches, 125 articles) produced **2 uncertain outcomes** among the 100 scored relationships: test_009 (expected conflict) and test_063 (expected update). In both cases **Gemini returned the correct label**. The pipeline's evidence validator then downgraded it to *uncertain* because one of the two supporting quotes was not found word for word in its source passage. The uncertainty is therefore **a verification failure, not a reasoning failure**.

Had both labels been accepted, Gemini's label accuracy would rise from 88.0% to 90.0% and macro-F1 from 88.9% to 90.0% (see “Impact on the scores”).

# How the pipeline produces “uncertain”

1\. Retrieval finds the candidate event. In both cases the original's event (E1) was found and compared.

2\. Gemini returns a label, an explanation, matching facts, differences and **one verbatim evidence quote per side**: the new article's passage (U1) and the original's passage (C1).

3\. The validator (similarity/comparison.py, lines 367–384) normalizes each quote and its source (case, spacing, curly quotes, dashes, HTML entities) and keeps the quote only if it appears **inside the cited passage**.

4\. If a same-event label lacks a verified quote on **either** side, the label is replaced by *uncertain* and the note “evidence could not be verified on both sides” is recorded.

5\. In the evaluation, an uncertain article is not joined to the event: it counts as wrong for labels and splits the event for grouping.

*The validator does not accept paraphrases, sentences stitched together, abbreviations the source does not use, or wording borrowed from the other article.*

# Verified configuration and evidence boundaries

- Saved run: gemini_125_completed.json; comparator gemini-3.8-flash; prompt_version compare-2; semantic minimum similarity 0.78; max_candidate_events 3.

- Both judgments have method = llm, uncertainty = null and insufficient_context = false: **Gemini itself did not express doubt**.

- The rejected quote text is not stored, only the validator note, so the exact character mismatch cannot be shown. Causes marked “most likely” are inferred from Gemini's recorded notes and the source text.

- The similarity figures in the batch HTML dashboards (89% for Batch 2, 82% for Batch 13, τ = 0.60) are illustrative, not measured.

# Case-by-case findings

## Batch 2 \| Test 009

**Article:** test_009_conflict_group_003.json (compared with test_006_original_group_003.json)

**Expected (ground truth):** **<span style="color:#176B38">same_event_conflict</span>**

**Gemini's own label:** **<span style="color:#176B38">same_event_conflict</span>** (correct)

**Final recorded label:** **<span style="color:#9C5700">uncertain</span>** (downgraded by the validator)

**Underlying article comparison:** test_006 reports Nvidia weighing an investment of up to **<mark style="background-color:#00FF00;color:#000000">\$10 billion</mark>** in Anthropic's IPO; test_009 reports a competing account that the ceiling was **<mark style="background-color:#00FF00;color:#000000">\$5 billion</mark>**, describing “a disagreement about the same talks, not a later revision”.

**Why the expected label applies:** Same talks, same metric, same time reference, incompatible values: a genuine conflict.

**Recorded Gemini explanation:** “The incoming report disputes the candidate's reported \$10 billion ceiling for the same talks, asserting the maximum was \$5 billion.”

**Gemini difference record:** conflict: Nvidia investment ceiling (**<mark style="background-color:#00FF00;color:#000000">\$5 billion</mark>** vs **<mark style="background-color:#00FF00;color:#000000">\$10 billion</mark>**)

**Validator record:** **<mark style="background-color:#FFFF00;color:#000000">candidate evidence quote not found in C1</mark>** → downgraded same_event_conflict to uncertain

**Which side failed:** The **original's side** (test_006, passage C1). The incoming quote passed: “A competing account says Nvidia's investment ceiling in the talks reported on September 12 was \$5 billion, not \$10 billion”.

**Was a verbatim quote available?** Yes. C1 contains “Nvidia may invest up to \$10 billion in Anthropic's IPO, which could raise \$100 billion at a \$2 trillion valuation.” Claude Haiku quoted exactly this sentence for the same pair and passed, and Gemini quoted the same passage successfully for test_007 in this batch.

**Most likely cause:** Wording borrowed from the other article. Gemini's notes say “**<mark style="background-color:#00FFFF;color:#000000">anchor investment</mark>**” and “**<mark style="background-color:#00FFFF;color:#000000">investment ceiling</mark>**”; both phrases occur only in test_009, while test_006 says “anchor investor” and never uses “ceiling”. A quote from C1 phrased this way cannot match.

**Error concept:** Evidence-verification false negative: correct conflict, unverifiable quote from the original.

> **WHAT MADE IT UNCERTAIN \|** Gemini detected the conflict (**<mark style="background-color:#00FF00;color:#000000">\$10B vs \$5B</mark>**). The label was removed because its quote from the original article **<mark style="background-color:#FFFF00;color:#000000">did not appear verbatim in passage C1</mark>**.

> **CONTEXT CLUES TO TRACK \|** Trigger words: “\$10 billion”, “\$5 billion”, “**<mark style="background-color:#00FFFF;color:#000000">ceiling</mark>**”, “**<mark style="background-color:#00FFFF;color:#000000">anchor investment</mark>**” vs “anchor investor”. Numbers and headline-style phrasing invite a summarized quote instead of a copied one.

## Batch 13 \| Test 063

**Article:** test_063_update_group_025.json (compared with test_061_original_group_025.json)

**Expected (ground truth):** **<span style="color:#176B38">same_event_update</span>**

**Gemini's own label:** **<span style="color:#176B38">same_event_update</span>** (correct)

**Final recorded label:** **<span style="color:#9C5700">uncertain</span>** (downgraded by the validator)

**Underlying article comparison:** test_061: SpaceX is “reportedly preparing to borrow about **<mark style="background-color:#00FF00;color:#000000">\$40 billion</mark>**” through a financing led by Apollo; Dan Ives' target is \$225. test_063: SpaceX “has **<mark style="background-color:#00FF00;color:#000000">finalized</mark>** its \$40 billion Apollo-led chip financing”, Pacific Investment Management “formally committed”, closing moved to the first quarter of 2027, and Ives raised his target to **<mark style="background-color:#00FF00;color:#000000">\$240</mark>** from \$225.

**Why the expected label applies:** Same financing, later status (planned → finalized) and new facts (a new participant, a new price target): an update.

**Recorded Gemini explanation:** “The incoming segment reports the completion of the \$40 billion financing package previously reported as in early discussions.”

**Gemini difference record:** temporal_development: deal status (**<mark style="background-color:#00FF00;color:#000000">finalized</mark>** and **<mark style="background-color:#00FFFF;color:#000000">PIMCO</mark>** committed vs in early talks / preparing to borrow); new_information: analyst price target (**<mark style="background-color:#00FF00;color:#000000">raised to \$240</mark>** vs \$225)

**Validator record:** **<mark style="background-color:#FFFF00;color:#000000">incoming evidence quote not found in U1</mark>** → downgraded same_event_update to uncertain

**Which side failed:** The **new article's side** (test_063, passage U1). The candidate quote passed: “SpaceX (NASDAQ:SPCX) is reportedly preparing to borrow about \$40 billion to buy Nvidia Corp. (NASDAQ:NVDA) chips.”

**Was a verbatim quote available?** Yes. U1 contains “SpaceX has finalized its \$40 billion Apollo-led chip financing”, which Claude Haiku quoted for the same pair and passed.

**Most likely cause:** An abbreviation the source never uses. Gemini's notes say “**<mark style="background-color:#00FFFF;color:#000000">PIMCO</mark>**”, but neither test_063 nor test_061 contains that word; the text says “Pacific Investment Management”. A quote written with “PIMCO”, or one merging the finalization, the commitment and the \$240 target into one sentence, cannot match U1.

**Run note:** The first attempt of Batch 13 failed on test_063 with an API rate limit (HTTP 429). The recorded uncertain comes from the successful re-run, not from that failure.

**Error concept:** Evidence-verification false negative: correct update, unverifiable quote from the new article.

> **WHAT MADE IT UNCERTAIN \|** Gemini detected the update (**<mark style="background-color:#00FF00;color:#000000">planned → finalized; \$225 → \$240</mark>**). The label was removed because its quote from the new article **<mark style="background-color:#FFFF00;color:#000000">did not appear verbatim in passage U1</mark>**.

> **CONTEXT CLUES TO TRACK \|** Trigger words: “finalized”, “**<mark style="background-color:#00FFFF;color:#000000">PIMCO</mark>**” vs “Pacific Investment Management”, “\$240”, “first quarter of 2027”. Abbreviations and multi-fact sentences invite a rewritten quote.

# Cross-case synthesis

1\. **Correct reasoning, failed verification.** Both raw labels match the answer key, and neither judgment expressed uncertainty.

2\. **One side each.** test_009 failed on the original's passage (C1), test_063 on the new article's passage (U1); the other quote passed in both cases.

3\. **The sources were quotable.** Verbatim sentences carrying the key fact existed in both passages, and Claude Haiku quoted them successfully.

4\. **Likely trigger.** Gemini's notes use wording that is not in the cited passage (“anchor investment”, “ceiling”, “PIMCO”), the signature of summarizing rather than copying.

5\. **Different from the ten misclassifications.** Those are wrong labels; these are right labels that the pipeline could not confirm.

# Hypotheses checked against the record

| **<span style="color:#FFFFFF">Hypothesis</span>** | **<span style="color:#FFFFFF">Verdict</span>** | **<span style="color:#FFFFFF">Evidence</span>** |
|----|----|----|
| Gemini mistook the conflict (009) or the update (063) | **<span style="color:#A51D2D">Rejected</span>** | Raw labels were same_event_conflict and same_event_update |
| High similarity hid the changed facts | **<span style="color:#A51D2D">Rejected</span>** | Both difference records name the changed facts |
| Retrieval missed the original | **<span style="color:#A51D2D">Rejected</span>** | Candidate event E1 was compared in both cases |
| Gemini was unsure | **<span style="color:#A51D2D">Rejected</span>** | uncertainty = null; insufficient_context = false |
| Curly quotes or dashes broke the match | **<span style="color:#9C5700">Unlikely</span>** | The validator already normalizes them |
| API failure | **<span style="color:#A51D2D">Rejected</span>** | Both come from successful calls (063's HTTP 429 was a superseded attempt) |
| Quote reworded, abbreviated or borrowed from the other article | **<span style="color:#176B38">Most likely</span>** | Gemini's notes use “anchor investment”, “ceiling”, “PIMCO”, absent from the cited passages; rejected quotes were not saved |

# Impact on the scores

| **<span style="color:#FFFFFF">Metric</span>** | **<span style="color:#FFFFFF">As recorded</span>** | **<span style="color:#FFFFFF">If both labels were accepted</span>** |
|----|----|----|
| Label accuracy | 88.0% | **90.0%** |
| Macro-F1 | 88.9% | **90.0%** |
| Update F1 | 77.6% | **80.0%** |
| Conflict F1 | 93.6% | **95.8%** |
| Grouping recall | 88.7% | **92.0%** |
| Grouping F1 | 94.0% | **95.8%** |

*Hypothetical recalculation from gemini_125_completed.json: the two articles would join event E1, recovering 5 of the 17 missed same-event pairs. Grouping F1 would then exceed the rule baseline (95.4%).*

# Recommendations and final evaluation

1\. Save the raw model output and every rejected quote, so each downgrade can be audited.

2\. Accept near-verbatim matches (for example, at least 90% token overlap within the cited passage) before downgrading.

3\. When a quote fails, ask the model once to copy an exact sentence from the cited passage before downgrading.

4\. Report validator downgrades separately from model doubt: “uncertain (verification)” vs “uncertain (model)”.

5\. Instruct the judge to quote names exactly as written (no abbreviations such as PIMCO) and to quote each side only from its own passage.

**Final conclusion:** Both uncertain outcomes are correct Gemini judgments that the evidence validator could not confirm. The safety rule worked as designed, since no unverified claim entered an event, but it cost 2 points of accuracy and 5 grouping pairs. The fix lies in verification and logging, not in the model.

# Evidence used

gemini_125_completed.json (Batch 2 and 13 rows: labels, explanations, matching facts, differences, evidence, validation notes); gemini_recovery/batch_13.json (superseded attempt); 2026-10-23_1557_claude.json (Claude Haiku's verified quotes for the same pairs); test_006, test_009, test_061 and test_063 JSON files; SystemCode/src/datasources/similarity/comparison.py lines 367–384; batch_002 and batch_013 HTML reports (illustrative figures only).
