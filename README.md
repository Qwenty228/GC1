# CG1 · StockEvent Lens: Gemini Event Consolidation Results

**Branch `GC1_nghia_mike_gemini_results`** holds the completed evaluation of **Gemini 3.8 Flash** on the event-consolidation stage of StockEvent Lens: all **125 labelled NVIDIA news articles**, **25 batches**, **no processing errors**.

📄 **Reports (in [`Report/`](Report/))**

| File | What it shows |
| :-- | :-- |
| [`CG1_Gemini_125_Visual_Evaluation_Report.pdf`](Report/CG1_Gemini_125_Visual_Evaluation_Report.pdf) | One-page summary: headline scores, per-class chart and table, key findings, method |
| [`Gemini_125_Articles_REDOWNLOAD.pdf`](Report/Gemini_125_Articles_REDOWNLOAD.pdf) | All 125 article outcomes: expected vs predicted label, event, result |

📦 **Raw result:** [`SystemCode/evaluation/consolidation/results/gemini_125_completed.json`](SystemCode/evaluation/consolidation/results/gemini_125_completed.json) holds every score below, plus the model's explanation, evidence quotes and matching facts for each judgment.

---

## 1. Headline results

| Articles processed | Grouping F1 | Label macro-F1 | Label accuracy | Processing errors |
| :-: | :-: | :-: | :-: | :-: |
| **125 / 125** | **94.0 %** | **88.9 %** | **88.0 %** (88 / 100) | **0** |

- **Grouping**: Gemini never merged articles that belong to different events (**precision 100 %**). It missed some pairs that belong together (**recall 88.7 %**).
- **Labels**: three of the four relation types score above 91 % F1. **Updates are the weak spot (77.5 % F1).**
- **Outcomes**: 88 correct, 10 incorrect, 2 `uncertain`. The other 25 articles are batch references and are not scored.

## 2. Per-class performance

| Relation label | Precision | Recall | **F1** | TP / FP / FN |
| :-- | :-: | :-: | :-: | :-: |
| Repeated event (`same_event_repeated`) | 95.7 % | 88.0 % | **91.7 %** | 22 / 1 / 3 |
| Updated event (`same_event_update`) | 79.2 % | 76.0 % | **77.5 %** | 19 / 5 / 6 |
| Conflicting event (`same_event_conflict`) | 100.0 % | 88.0 % | **93.6 %** | 22 / 0 / 3 |
| Different event (`different_event`) | 86.2 % | 100.0 % | **92.6 %** | 25 / 4 / 0 |
| **Macro average** | | | **88.9 %** | |

Grouping (pairwise, within each batch): precision 100.0 %, recall 88.7 %, F1 94.0 % (TP 133, FP 0, FN 17).

## 3. Where Gemini went wrong (12 of 100 scored articles)

| Expected → predicted | Count | Articles (batch) |
| :-- | :-: | :-- |
| update → different event | 4 | `test_058` (12), `test_068` (14), `test_078` (16)\*, `test_083` (17)\* |
| repeated → update | 3 | `test_027` (6), `test_037` (8), `test_072` (15) |
| conflict → update | 2 | `test_029` (6), `test_049` (10) |
| update → uncertain | 1 | `test_063` (13) |
| update → repeated | 1 | `test_118` (24) |
| conflict → uncertain | 1 | `test_009` (2) |

\* **Not a Gemini decision.** For `test_078` and `test_083`, embedding retrieval found no candidate event, so the article never reached Gemini and opened a new event by default. Gemini judged the other ten itself; its reasoning for each is in the result JSON (`judgments`).

**Takeaways**
1. **Grouping is conservative.** Zero false merges, but 17 same-event pairs were left apart.
2. **Updates are the main weakness.** Gemini blurs update and repeat in both directions (repeat→update ×3, update→repeat ×1), and labels some conflicts as updates (×2).
3. **Part of the update problem is retrieval, not the LLM.** 2 of the 4 update→different misses never reached Gemini, so tuning retrieval (similarity threshold, candidate count) could help as much as changing the prompt.

## 4. Detailed Batch Documentation and Gemini Error Specialization

This section provides supporting documentation for the completed Gemini 3.8 Flash event-consolidation evaluation. All 25 batches and 125 articles are linked to the final machine-readable evaluation output:

**Primary evaluation result:** [gemini_125_completed.json](SystemCode/evaluation/consolidation/results/gemini_125_completed.json)

The following documents provide batch-level explanations, evidence comparisons, and specialized analysis of incorrect or uncertain predictions.

### 4.1 Batch-Level Evaluation Reports (`Report/batch_doc/`)

**Folder:** [View all 25 batch reports](Report/batch_doc/)

The `batch_doc` folder contains 25 Word documents, from `batch_001.docx` to `batch_025.docx`, corresponding to the 25 batches evaluated by Gemini.

Each document examines five articles, including their expected event relationships and supporting textual evidence.

| Article position | Category | Purpose |
|---|---|---|
| 1 | Original | Establishes the reference event and its baseline facts. |
| 2 | Repeat | Reports the same event without meaningful new information. |
| 3 | Update | Adds new information or developments to the existing event. |
| 4 | Conflict | Contradicts an existing factual claim about the same event. |
| 5 | Different | Represents a separate event that should not be consolidated with the reference event. |

The batch reports contain:

- Batch metadata and the five associated article filenames.
- Event grouping and consolidation explanations.
- Textual evidence supporting each article's expected relationship.
- Classification rationale for original, repeated, updated, conflicting, and different events.
- Context for interpreting Gemini's predicted classifications against the expected labels.

**Example — Batch 001:**

- `test_001`: Original NVIDIA–Groq DOJ investigation.
- `test_002`: Repeated coverage of the same investigation.
- `test_003`: Update introducing a 30-day response deadline.
- `test_004`: Conflict concerning exclusive versus nonexclusive licensing rights.
- `test_005`: Different event involving an NVIDIA developer training center.

These documents explain the expected semantic relationships. Gemini's actual predicted labels, event assignments, and judgment evidence are recorded separately in `gemini_125_completed.json`.

### 4.2 Gemini Error Detection and Specialization (`Report/gemini error detect/`)

**Document:** [Gemini Evaluation Error Specialization v2](Report/gemini%20error%20detect/gemini_evaluation_error_specialization_v2.docx)

This report focuses on discrepancies between Gemini's predicted labels and the expected classifications.

The completed evaluation contains:

| Evaluation outcome | Count |
|---|---:|
| Correct classifications | 88 |
| Incorrect classifications | 10 |
| Uncertain classifications | 2 |
| Reference articles (not scored) | 25 |
| **Total articles** | **125** |

The diagnostic report examines four principal error patterns:

1. **Multi-day market and timeline disconnections:** An update is incorrectly classified as a different event.
2. **Over-interpretation of minor details:** A repeated article is incorrectly classified as an update.
3. **Conflict rationalization as a temporal update:** Contradictory information is treated as a normal event development.
4. **Under-sensitivity to contextual updates:** An article containing meaningful new information is incorrectly classified as a repeat.

It also discusses uncertain predictions, evidence-quote verification, technical edge cases, and possible improvements to the evaluation pipeline.

**Important distinction:** Not every incorrect classification originates from Gemini's language-model judgment. Some errors occur during candidate retrieval before Gemini receives an event for comparison. Other uncertain results arise during evidence validation.

### 4.3 Relationship Between the Documents and Final JSON

The three resources serve different purposes:

| Resource | Role |
|---|---|
| [`gemini_125_completed.json`](SystemCode/evaluation/consolidation/results/gemini_125_completed.json) | Primary evaluation output containing expected and predicted labels, event assignments, model judgments, evidence, and metrics. |
| [`Report/batch_doc/`](Report/batch_doc/) | Human-readable explanations of each batch's article relationships and supporting evidence. |
| [`Report/gemini error detect/`](Report/gemini%20error%20detect/) | Diagnostic interpretation of classification discrepancies, uncertain predictions, and technical limitations. |

**Recommended evaluation review workflow:**

1. Identify the article and batch in `gemini_125_completed.json`.
2. Open the corresponding `batch_XXX.docx` to understand the original event, expected relationship, and textual evidence.
3. Compare the expected label against Gemini's predicted label and judgment explanation in the JSON.
4. For incorrect or uncertain outcomes, consult `gemini_evaluation_error_specialization_v2.docx` to examine the relevant error pattern.
5. Distinguish language-model classification errors from retrieval failures and post-processing validation issues.

This structure connects the quantitative evaluation results with qualitative, evidence-based explanations. It supports examination of why a classification was expected, what Gemini actually predicted, and which pipeline component may have contributed to a discrepancy.

**Evaluation scope:** These documents describe the team's 125-article benchmark. They are supporting analyses of the completed evaluation, not independent evaluation runs or evidence of performance on live financial news.

## 5. What was evaluated

| | |
| :-- | :-- |
| **Stage** | Event consolidation: does a new article report the same event as an earlier one, and how? |
| **Dataset** | 125 labelled fixture articles (`SystemCode/populate/event_grouping/`), 25 independent batches of 5 |
| **Batch design** | 1 reference article, then a repeat, an update, a conflict and a different event (order varies slightly) |
| **Pipeline** | BGE embeddings (`BAAI/bge-small-en-v1.5`) retrieve candidate events, then Gemini labels each candidate pair |
| **Model / prompt** | `gemini-3.8-flash` via the OpenAI-compatible endpoint, JSON-schema output, production prompt `compare-2` |
| **Retrieval settings** | 7-day window, min. semantic similarity 0.78, at most 3 candidate events |
| **Isolation** | Each batch runs in its own fresh PostgreSQL database, so batches never see each other |
| **Answer key** | Never shown to the model: only the article text goes in |

**How to read the metrics**
- **Grouping F1**: for every pair of articles in a batch, was "same event or not" predicted correctly? Only membership counts, never event IDs.
- **Label macro-F1**: F1 for each of the 4 labels, averaged with equal weight, so the weak update class is not hidden by the easy ones.
- **Accuracy**: over the 100 non-reference articles; `uncertain` always counts as wrong.
- **Event IDs** (`E1`, `E2` …) are local to each batch.

## 6. How the full 125-article run was completed

The first run (`2026-10-24_1709_gemini.json`, kept unchanged) finished only 68 of 125 articles. 57 failed on provider limits and outages: **HTTP 429 ×51, timeout ×4, HTTP 503 ×2**. A dedicated recovery runner finished the job:

1. **Kept** the 4 batches that had fully completed (1, 2, 3, 6), exactly as saved.
2. **Reran the 21 other batches whole**, each from its first article in a fresh database. Batches are stateful, so a partial batch can't be patched.
3. **Same configuration as the original run**: model, prompt, JSON schema, embedder and retrieval settings were all checked against the saved run before every start.
4. **Saved a checkpoint after every batch**, so an interruption or quota stop lost at most the batch in flight.
5. **Combined and re-scored** the 4 kept and 21 recovered batches with the project's own `metrics.py`, and verified 25 unique batches, 125 unique articles and 0 processing errors.

Only processing failures were rerun. Wrong labels and `uncertain` answers are model results; they were kept and scored, never retried.

| Usage | Comparison calls | Input tokens | Output tokens |
| :-- | :-: | :-: | :-: |
| Final 25 batches | 77 | 179,767 | 50,380 |
| Discarded (failed original batches + 5 failed recovery attempts) | 5 | 9,379 | 3,171 |

## 7. All 125 article outcomes

<details>
<summary><b>Show the full table</b> (same content as <code>Gemini_125_Articles_REDOWNLOAD.pdf</code>, generated from the result JSON)</summary>

| Batch | Article | Expected | Predicted | Event | Result |
|:-:|:--|:--|:--|:-:|:-:|
| 1 | `test_001_original_group_001` | *(reference)* | different_event | E1 | reference |
| 1 | `test_002_repeat_group_001` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 1 | `test_003_update_group_001` | same_event_update | same_event_update | E1 | ✅ correct |
| 1 | `test_004_conflict_group_001` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 1 | `test_005_different_group_002` | different_event | different_event | E2 | ✅ correct |
| 2 | `test_006_original_group_003` | *(reference)* | different_event | E1 | reference |
| 2 | `test_007_repeat_group_003` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 2 | `test_008_update_group_003` | same_event_update | same_event_update | E1 | ✅ correct |
| 2 | `test_009_conflict_group_003` | same_event_conflict | uncertain | E2 | ⚠️ uncertain |
| 2 | `test_010_different_group_004` | different_event | different_event | E3 | ✅ correct |
| 3 | `test_011_original_group_005` | *(reference)* | different_event | E1 | reference |
| 3 | `test_012_repeat_group_005` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 3 | `test_013_update_group_005` | same_event_update | same_event_update | E1 | ✅ correct |
| 3 | `test_014_conflict_group_005` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 3 | `test_015_different_group_006` | different_event | different_event | E2 | ✅ correct |
| 4 | `test_016_original_group_007` | *(reference)* | different_event | E1 | reference |
| 4 | `test_017_repeat_group_007` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 4 | `test_018_update_group_007` | same_event_update | same_event_update | E1 | ✅ correct |
| 4 | `test_019_conflict_group_007` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 4 | `test_020_different_group_008` | different_event | different_event | E2 | ✅ correct |
| 5 | `test_021_original_group_009` | *(reference)* | different_event | E1 | reference |
| 5 | `test_022_repeat_group_009` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 5 | `test_023_update_group_009` | same_event_update | same_event_update | E1 | ✅ correct |
| 5 | `test_024_conflict_group_009` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 5 | `test_025_different_group_010` | different_event | different_event | E2 | ✅ correct |
| 6 | `test_026_original_group_011` | *(reference)* | different_event | E1 | reference |
| 6 | `test_027_repeat_group_011` | same_event_repeated | same_event_update | E1 | ❌ incorrect |
| 6 | `test_028_update_group_011` | same_event_update | same_event_update | E1 | ✅ correct |
| 6 | `test_029_conflict_group_011` | same_event_conflict | same_event_update | E1 | ❌ incorrect |
| 6 | `test_030_different_group_012` | different_event | different_event | E2 | ✅ correct |
| 7 | `test_031_original_group_013` | *(reference)* | different_event | E1 | reference |
| 7 | `test_032_repeat_group_013` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 7 | `test_033_update_group_013` | same_event_update | same_event_update | E1 | ✅ correct |
| 7 | `test_034_conflict_group_013` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 7 | `test_035_different_group_014` | different_event | different_event | E2 | ✅ correct |
| 8 | `test_036_original_group_015` | *(reference)* | different_event | E1 | reference |
| 8 | `test_037_repeat_group_015` | same_event_repeated | same_event_update | E1 | ❌ incorrect |
| 8 | `test_038_update_group_015` | same_event_update | same_event_update | E1 | ✅ correct |
| 8 | `test_039_conflict_group_015` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 8 | `test_040_different_group_016` | different_event | different_event | E2 | ✅ correct |
| 9 | `test_041_original_group_017` | *(reference)* | different_event | E1 | reference |
| 9 | `test_042_repeat_group_017` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 9 | `test_043_update_group_017` | same_event_update | same_event_update | E1 | ✅ correct |
| 9 | `test_044_conflict_group_017` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 9 | `test_045_different_group_018` | different_event | different_event | E2 | ✅ correct |
| 10 | `test_046_original_group_019` | *(reference)* | different_event | E1 | reference |
| 10 | `test_047_repeat_group_019` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 10 | `test_048_update_group_019` | same_event_update | same_event_update | E1 | ✅ correct |
| 10 | `test_049_conflict_group_019` | same_event_conflict | same_event_update | E1 | ❌ incorrect |
| 10 | `test_050_different_group_020` | different_event | different_event | E2 | ✅ correct |
| 11 | `test_051_original_group_021` | *(reference)* | different_event | E1 | reference |
| 11 | `test_052_repeat_group_021` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 11 | `test_053_update_group_021` | same_event_update | same_event_update | E1 | ✅ correct |
| 11 | `test_054_conflict_group_021` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 11 | `test_055_different_group_022` | different_event | different_event | E2 | ✅ correct |
| 12 | `test_056_original_group_023` | *(reference)* | different_event | E1 | reference |
| 12 | `test_057_repeat_group_023` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 12 | `test_058_update_group_023` | same_event_update | different_event | E2 | ❌ incorrect |
| 12 | `test_059_conflict_group_023` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 12 | `test_060_different_group_024` | different_event | different_event | E3 | ✅ correct |
| 13 | `test_061_original_group_025` | *(reference)* | different_event | E1 | reference |
| 13 | `test_062_repeat_group_025` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 13 | `test_063_update_group_025` | same_event_update | uncertain | E2 | ⚠️ uncertain |
| 13 | `test_064_conflict_group_025` | same_event_conflict | same_event_conflict | E2, E1 | ✅ correct |
| 13 | `test_065_different_group_026` | different_event | different_event | E3 | ✅ correct |
| 14 | `test_066_original_group_027` | *(reference)* | different_event | E1 | reference |
| 14 | `test_067_repeat_group_027` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 14 | `test_068_update_group_027` | same_event_update | different_event | E2 | ❌ incorrect |
| 14 | `test_069_conflict_group_027` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 14 | `test_070_different_group_028` | different_event | different_event | E3 | ✅ correct |
| 15 | `test_071_original_group_029` | *(reference)* | different_event | E1 | reference |
| 15 | `test_072_repeat_group_029` | same_event_repeated | same_event_update | E1 | ❌ incorrect |
| 15 | `test_073_update_group_029` | same_event_update | same_event_update | E1 | ✅ correct |
| 15 | `test_074_conflict_group_029` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 15 | `test_075_different_group_030` | different_event | different_event | E2 | ✅ correct |
| 16 | `test_076_original_group_031` | *(reference)* | different_event | E1 | reference |
| 16 | `test_077_repeat_group_031` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 16 | `test_078_update_group_031` | same_event_update | different_event | E2 | ❌ incorrect |
| 16 | `test_079_conflict_group_031` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 16 | `test_080_different_group_032` | different_event | different_event | E3 | ✅ correct |
| 17 | `test_081_original_group_033` | *(reference)* | different_event | E1 | reference |
| 17 | `test_082_repeat_group_033` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 17 | `test_083_update_group_033` | same_event_update | different_event | E2 | ❌ incorrect |
| 17 | `test_084_conflict_group_033` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 17 | `test_085_different_group_034` | different_event | different_event | E3 | ✅ correct |
| 18 | `test_086_original_group_035` | *(reference)* | different_event | E1 | reference |
| 18 | `test_087_repeat_group_035` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 18 | `test_088_update_group_035` | same_event_update | same_event_update | E1 | ✅ correct |
| 18 | `test_089_conflict_group_035` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 18 | `test_090_different_group_036` | different_event | different_event | E2 | ✅ correct |
| 19 | `test_091_original_group_037` | *(reference)* | different_event | E1 | reference |
| 19 | `test_092_repeat_group_037` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 19 | `test_093_update_group_037` | same_event_update | same_event_update | E1 | ✅ correct |
| 19 | `test_094_conflict_group_037` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 19 | `test_095_different_group_038` | different_event | different_event | E2 | ✅ correct |
| 20 | `test_096_original_group_039` | *(reference)* | different_event | E1 | reference |
| 20 | `test_097_repeat_group_039` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 20 | `test_098_update_group_039` | same_event_update | same_event_update | E1 | ✅ correct |
| 20 | `test_099_conflict_group_039` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 20 | `test_100_different_group_040` | different_event | different_event | E2 | ✅ correct |
| 21 | `test_101_original_group_041` | *(reference)* | different_event | E1 | reference |
| 21 | `test_102_repeat_group_041` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 21 | `test_103_update_group_041` | same_event_update | same_event_update | E1 | ✅ correct |
| 21 | `test_104_conflict_group_041` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 21 | `test_105_different_group_042` | different_event | different_event | E2 | ✅ correct |
| 22 | `test_106_original_group_043` | *(reference)* | different_event | E1 | reference |
| 22 | `test_107_repeat_group_043` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 22 | `test_108_update_group_043` | same_event_update | same_event_update | E1 | ✅ correct |
| 22 | `test_109_conflict_group_043` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 22 | `test_110_different_group_044` | different_event | different_event | E2 | ✅ correct |
| 23 | `test_111_original_group_045` | *(reference)* | different_event | E1 | reference |
| 23 | `test_112_update_group_045` | same_event_update | same_event_update | E1 | ✅ correct |
| 23 | `test_113_repeat_group_045` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 23 | `test_114_conflict_group_045` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 23 | `test_115_different_group_046` | different_event | different_event | E2 | ✅ correct |
| 24 | `test_116_original_group_047` | *(reference)* | different_event | E1 | reference |
| 24 | `test_117_repeat_group_047` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 24 | `test_118_update_group_047` | same_event_update | same_event_repeated | E1 | ❌ incorrect |
| 24 | `test_119_conflict_group_047` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 24 | `test_120_different_group_048` | different_event | different_event | E2 | ✅ correct |
| 25 | `test_121_original_group_049` | *(reference)* | different_event | E1 | reference |
| 25 | `test_122_update_group_049` | same_event_update | same_event_update | E1 | ✅ correct |
| 25 | `test_123_repeat_group_049` | same_event_repeated | same_event_repeated | E1 | ✅ correct |
| 25 | `test_124_conflict_group_049` | same_event_conflict | same_event_conflict | E1 | ✅ correct |
| 25 | `test_125_different_group_050` | different_event | different_event | E2 | ✅ correct |

</details>

## 8. Files on this branch

```
Report/
├── CG1_Gemini_125_Visual_Evaluation_Report.pdf   one-page visual summary
└── Gemini_125_Articles_REDOWNLOAD.pdf             all 125 outcomes
SystemCode/evaluation/consolidation/
├── resume_gemini.py                  recovery runner (dry run, checkpoints, final report)
├── experiments/gemini.py             the Gemini experiment (model, endpoint, JSON mode)
└── results/
    ├── gemini_125_completed.json     FINAL: 25 batches, 125 articles, scores, reasons
    ├── 2026-10-24_1709_gemini.json   the original, incomplete run (unchanged)
    └── gemini_recovery/              per-batch checkpoints + manifest (no API keys)
SystemCode/src/datasources/evaluation/
├── resume.py                         resume plan, checkpoints, final combine and re-score
├── gemini_projects.py                retries, Retry-After, key failover, quota handling
└── verbose.py                        --verbose real-time stage logs
SystemCode/tests/
├── test_evaluation_resume.py         offline tests: resume, quota, checkpoints, final scores
└── test_evaluation_verbose.py        offline tests: logging, never leaks a key or prompt
```

## 9. Reproduce or inspect

From `SystemCode/` (Windows CMD shown; API keys go in `SystemCode/.env`, which git ignores, so no key is in this repository):

```bat
rem view the final run in the terminal, with Gemini's reasoning per article
uv run datasources-eval report gemini_125_completed --reasons

rem leaderboard of all saved consolidation runs
uv run datasources-eval compare

rem what a recovery would do now (no API call, nothing written)
uv run python evaluation\consolidation\resume_gemini.py --dry-run

rem the offline tests for the recovery and logging code
uv run python -m pytest tests\test_evaluation_resume.py tests\test_evaluation_verbose.py
```

## 10. Limitations

- **Team-built dataset.** These scores come from the team's 125 labelled fixtures, not live news traffic. Treat them as a benchmark, not a real-world reliability claim.
- **One run.** LLM output can vary between runs, even at temperature 0.
- **Strict scoring.** `uncertain` counts as wrong, and a retrieval miss counts against the pipeline even when Gemini never saw the article.
