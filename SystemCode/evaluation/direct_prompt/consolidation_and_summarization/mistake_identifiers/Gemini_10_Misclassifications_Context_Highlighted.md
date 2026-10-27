# Gemini Consolidation: Ten Misclassifications

StockEvent Lens \| BGE-small embeddings, Python thresholds, Gemini comparison \| Evaluation evidence and final interpretation

# Executive evaluation

The completed experiment processed 25 five-article batches (125 articles). The 25 originals are event starting points; the remaining 100 relationships yielded 88 correct, 10 incorrect and 2 uncertain results. Reported label accuracy was 0.88 and macro-F1 was 0.8886. **<mark style="background-color:#DCEBFA;color:#1558A6">Update</mark>** had the weakest F1 (0.7755).

Four recurring error patterns occur in these ten cases: **<mark style="background-color:#DDF3E2;color:#176B38">repeat</mark>**→**<mark style="background-color:#DCEBFA;color:#1558A6">update</mark>** (3), **<mark style="background-color:#FBE0E1;color:#A51D2D">conflict</mark>**→**<mark style="background-color:#DCEBFA;color:#1558A6">update</mark>** (2), **<mark style="background-color:#DCEBFA;color:#1558A6">update</mark>**→different (4), and **<mark style="background-color:#DCEBFA;color:#1558A6">update</mark>**→**<mark style="background-color:#DDF3E2;color:#176B38">repeat</mark>** (1). These are mismatches against the supplied synthetic labels, not automatically proof that every label is unambiguous.

# Verified configuration and evidence boundaries

- Embedder: BAAI/bge-small-en-v1.5; 384-dimensional normalized cosine embeddings.

- Semantic minimum similarity: 0.78 (the 0.60 threshold displayed in several HTML dashboards is illustrative and not the saved run setting).

- semantic_top_k = 20; max_candidate_events = 3; lexical_plausible_rank = 0; comparator = gemini-3.8-flash; prompt_version = compare-2.

- Eight of the ten cases have recorded method=llm judgements; tests 078 and 083 have no recorded judgements. Thus the exact reason for those two different-event outcomes cannot be proven from the per-article result alone.

- The HTML dashboards contain explanatory or illustrative overlap/similarity percentages. Do not treat those as measured per-pair BGE similarity unless confirmed by execution traces.

# Case-by-case findings

## Batch 6 \| Test 027

**Article:** test_027_repeat_group_011.json

Expected (ground truth): **<mark style="background-color:#DDF3E2;color:#176B38">same_event_repeated</mark>**

Predicted: **<mark style="background-color:#DCEBFA;color:#1558A6">same_event_update</mark>**

**Underlying article comparison:** SpaceX Starmind **<mark style="background-color:#00FF00;color:#000000">250 kW</mark>** solar-power description and NVIDIA VR72 operating-power remarks.

**Why the expected label applies:** The incoming article paraphrases the original Starmind announcement rather than documenting a **<mark style="background-color:#00FFFF;color:#000000">new</mark>** operational milestone.

**Observed judgement / plausible error:** Gemini marked the **<mark style="background-color:#00FFFF;color:#000000">VR72 average/peak power</mark>** discussion as **<mark style="background-color:#00FFFF;color:#000000">new</mark>**. That information is already in the supplied original fixture; compare claims across the full text rather than comparing excerpts.

**Recorded Gemini explanation:** Both report Musk's statements regarding SpaceX Starmind satellites' power, with the incoming segment adding detailed remarks on VR72 operating power.

**Gemini difference record:** **<mark style="background-color:#00FFFF;color:#000000">new</mark>**\_information: VR72 power details (not reported in text → operate VR72 at ~250kW average power with peak ~10% higher)

**Error concept:** Novelty false positive; candidate-text coverage / comparison granularity.

**CONTEXT CLUES TO TRACK \|** Trigger words: **<mark style="background-color:#00FF00;color:#000000">"adding detailed remarks"</mark>**, **<mark style="background-color:#00FF00;color:#000000">"new_information"</mark>**, **<mark style="background-color:#00FF00;color:#000000">"VR72"</mark>**. These suggest novelty even though the power remarks are already present. GREEN = repeated underlying fact; BLUE = mistaken update cue.

## Batch 6 \| Test 029

**Article:** test_029_conflict_group_011.json

Expected (ground truth): **<mark style="background-color:#FBE0E1;color:#A51D2D">same_event_conflict</mark>**

Predicted: **<mark style="background-color:#DCEBFA;color:#1558A6">same_event_update</mark>**

**Underlying article comparison:** SpaceX Starmind **<mark style="background-color:#FF00FF;color:#FFFFFF">250 kW</mark>** projection versus a claimed **<mark style="background-color:#FF00FF;color:#FFFFFF">45 kW</mark>** **<mark style="background-color:#FF00FF;color:#FFFFFF">thermal</mark>** cap.

**Why the expected label applies:** The incoming fixture explicitly challenges the earlier power claim and changes the claimed engineering feasibility.

Observed judgement / plausible error: Gemini called the cap a **<mark style="background-color:#DCEBFA;color:#1558A6">update</mark>**.**<mark style="background-color:#00FFFF;color:#000000">later disclosure</mark>** and a temporal development. Under the fixture labels it is a competing, incompatible claim, not a straightforward incremental fact. A real confirmed revision could instead be an

**Recorded Gemini explanation:** Musk later disclosed technical roadblocks capping Starmind satellites at **<mark style="background-color:#FF00FF;color:#FFFFFF">45 kW</mark>**, updating earlier **<mark style="background-color:#FF00FF;color:#FFFFFF">250 kW</mark>** projections.

**Gemini difference record:** temporal_development: satellite power capacity (projected at over **<mark style="background-color:#FF00FF;color:#FFFFFF">250 kW</mark>** → capped at **<mark style="background-color:#FF00FF;color:#FFFFFF">45 kW</mark>** due to engineering roadblocks); new_information: announcement setting (posts on X → unexpected press briefing)

Error concept: **<mark style="background-color:#FBE0E1;color:#A51D2D">Conflict</mark>**-versus-**<mark style="background-color:#DCEBFA;color:#1558A6">update</mark>** ambiguity; numerical attribute contradiction.

**CONTEXT CLUES TO TRACK \|** Trigger words: **<mark style="background-color:#FF00FF;color:#FFFFFF">"later disclosed"</mark>**, **<mark style="background-color:#FF00FF;color:#FFFFFF">"capping"</mark>**, **<mark style="background-color:#FF00FF;color:#FFFFFF">"45 kW"</mark>**, **<mark style="background-color:#FF00FF;color:#FFFFFF">"250 kW"</mark>**. The temporal language encourages UPDATE; incompatible capacity claims require a CONFLICT check.

## Batch 8 \| Test 037

**Article:** test_037_repeat_group_015.json

Expected (ground truth): **<mark style="background-color:#DDF3E2;color:#176B38">same_event_repeated</mark>**

Predicted: **<mark style="background-color:#DCEBFA;color:#1558A6">same_event_update</mark>**

**Underlying article comparison:** Morgan Stanley reinstates NVIDIA as **<mark style="background-color:#00FF00;color:#000000">top semiconductor pick</mark>**.

Why the expected label applies: The **<mark style="background-color:#DDF3E2;color:#176B38">repeated</mark>** article restates the recommendation and valuation rationale, with no new analyst action.

**Observed judgement / plausible error:** Gemini regarded a **<mark style="background-color:#00FFFF;color:#000000">Friday closing price</mark>** (+2.46%, **<mark style="background-color:#00FFFF;color:#000000">\$236.53</mark>**) as a new fact; the original uploaded fixture already reports that price.

**Recorded Gemini explanation:** Both articles report Morgan Stanley naming Nvidia its top chip pick, with the incoming text adding Friday closing share price data.

**Gemini difference record:** new_information: closing share price (not reported → closed up 2.46% at **<mark style="background-color:#00FFFF;color:#000000">\$236.53</mark>**)

**Error concept:** Novelty false positive; full-article fact matching.

**CONTEXT CLUES TO TRACK \|** Trigger words: **<mark style="background-color:#00FF00;color:#000000">"adding Friday closing share price data"</mark>**, **<mark style="background-color:#00FF00;color:#000000">"2.46%"</mark>**, **<mark style="background-color:#00FF00;color:#000000">"\$236.53"</mark>**. Gemini regarded an existing market detail as new.

## Batch 10 \| Test 049

**Article:** test_049_conflict_group_019.json

Expected (ground truth): **<mark style="background-color:#FBE0E1;color:#A51D2D">same_event_conflict</mark>**

Predicted: **<mark style="background-color:#DCEBFA;color:#1558A6">same_event_update</mark>**

**Underlying article comparison:** Amazon explores an **<mark style="background-color:#FF00FF;color:#FFFFFF">\$8B</mark>** NVIDIA-chip special-purpose financing vehicle; incoming report says it was **<mark style="background-color:#FF00FF;color:#FFFFFF">abandoned</mark>**.

Why the expected label applies: The synthetic test is labeled **<mark style="background-color:#FBE0E1;color:#A51D2D">conflict</mark>** because the account disputes the original proposed financing outcome.

**Observed judgement / plausible error:** Gemini interpreted abandonment as a later **<mark style="background-color:#00FFFF;color:#000000">status change</mark>**. This is semantically defensible if chronologically confirmed; the fixture treats it as an incompatible competing account.

**Recorded Gemini explanation:** Amazon reportedly **<mark style="background-color:#FF00FF;color:#FFFFFF">abandoned</mark>** the planned \$8 billion Nvidia chip financing vehicle that was previously being explored.

**Gemini difference record:** temporal_development: plan status (exploring and holding **<mark style="background-color:#FF00FF;color:#FFFFFF">preliminary talks</mark>** → completely **<mark style="background-color:#FF00FF;color:#FFFFFF">abandoned</mark>**)

**Error concept:** Event-status and temporal provenance ambiguity.

**CONTEXT CLUES TO TRACK \|** Trigger words: **<mark style="background-color:#FF00FF;color:#FFFFFF">"abandoned"</mark>**, **<mark style="background-color:#FF00FF;color:#FFFFFF">"previously being explored"</mark>**, **<mark style="background-color:#FF00FF;color:#FFFFFF">"temporal_development"</mark>**. These can signal a genuine later update, but the synthetic fixture treats the assertions as competing.

## Batch 12 \| Test 058

**Article:** test_058_update_group_023.json

Expected (ground truth): **<mark style="background-color:#DCEBFA;color:#1558A6">same_event_update</mark>**

**Predicted:** different_event

**Underlying article comparison:** AST SpaceMobile **<mark style="background-color:#00FFFF;color:#000000">Wednesday</mark>** drop of 6% to \$59.24; **<mark style="background-color:#00FFFF;color:#000000">Thursday</mark>** **<mark style="background-color:#00FFFF;color:#000000">rebound</mark>** of 2% to \$60.45 and **<mark style="background-color:#00FFFF;color:#000000">Rakuten</mark>** H1 2027 launch confirmation.

**Why the expected label applies:** The new report explicitly follows the earlier selloff and adds a partnership milestone within the constructed event narrative.

**Observed judgement / plausible error:** Gemini distinguished the trading sessions and selected different_event. This is an event-boundary error; a different trading day alone does not prove a different event.

**Recorded Gemini explanation:** The incoming segment reports **<mark style="background-color:#00FFFF;color:#000000">Thursday</mark>**'s stock **<mark style="background-color:#00FFFF;color:#000000">rebound</mark>**, whereas the candidate event reports **<mark style="background-color:#00FFFF;color:#000000">Wednesday</mark>**'s trading decline during a different trading session.

**Error concept:** Over-segmentation by time/session; possible event-boundary ambiguity.

**CONTEXT CLUES TO TRACK \|** Trigger words: **<mark style="background-color:#00FFFF;color:#000000">"Wednesday"</mark>**, **<mark style="background-color:#00FFFF;color:#000000">"Thursday"</mark>**, **<mark style="background-color:#00FFFF;color:#000000">"different trading session"</mark>**, **<mark style="background-color:#00FFFF;color:#000000">"rebound"</mark>**. Session boundaries overshadow the explicit continuation.

## Batch 14 \| Test 068

**Article:** test_068_update_group_027.json

Expected (ground truth): **<mark style="background-color:#DCEBFA;color:#1558A6">same_event_update</mark>**

**Predicted:** different_event

**Underlying article comparison:** Intuitive Machines LUNR drops 7.10% to \$14.00; later **<mark style="background-color:#00FFFF;color:#000000">rebound</mark>**s 4% to \$14.58 and announces a **<mark style="background-color:#00FFFF;color:#000000">2028</mark>** lunar-delivery contract.

**Why the expected label applies:** The incoming article explicitly references recovery from the prior **<mark style="background-color:#00FFFF;color:#000000">selloff</mark>** and retains the SpaceX linkage.

**Observed judgement / plausible error:** Gemini emphasized the **<mark style="background-color:#00FFFF;color:#000000">new contract</mark>** and later session as a separate occurrence rather than following the connected event storyline.

**Recorded Gemini explanation:** The incoming segment covers Thursday's **<mark style="background-color:#00FFFF;color:#000000">rebound</mark>** and a new lunar contract, whereas the candidate event covers Wednesday's sell-off.

**Error concept:** Over-segmentation by event angle and trading session.

**CONTEXT CLUES TO TRACK \|** Trigger words: **<mark style="background-color:#00FFFF;color:#000000">"new lunar contract"</mark>**, **<mark style="background-color:#00FFFF;color:#000000">"Thursday rebound"</mark>**, **<mark style="background-color:#00FFFF;color:#000000">"Wednesday sell-off"</mark>**. A new angle and trading date overshadow the linked market storyline.

## Batch 15 \| Test 072

**Article:** test_072_repeat_group_029.json

Expected (ground truth): **<mark style="background-color:#DDF3E2;color:#176B38">same_event_repeated</mark>**

Predicted: **<mark style="background-color:#DCEBFA;color:#1558A6">same_event_update</mark>**

**Underlying article comparison:** Bond selloff, 30-year yield **<mark style="background-color:#00FF00;color:#000000">5.719%</mark>**, **<mark style="background-color:#00FF00;color:#000000">\$39B</mark>** Treasury auction and FOMC minutes.

**Why the expected label applies:** The incoming article reproduces the same macroeconomic narrative and existing numerical context.

Observed judgement / plausible error: Gemini treated a **<mark style="background-color:#DDF3E2;color:#176B38">repeated</mark>** fixture itself already contains that fact. The comparator may have seen an incomplete candidate excerpt.**<mark style="background-color:#00FFFF;color:#000000">2.090</mark>**-million-barrel **<mark style="background-color:#00FFFF;color:#000000">crude inventory</mark>** draw as novel, although the supplied

**Recorded Gemini explanation:** Both articles report on the market conditions on October 7, 2026, with the incoming text adding **<mark style="background-color:#00FFFF;color:#000000">crude inventory</mark>** data.

**Gemini difference record:** new_information: crude inventories (not mentioned → **<mark style="background-color:#00FFFF;color:#000000">2.090</mark>** million barrel draw)

**Error concept:** Novelty false positive; background fact omission in comparison.

**CONTEXT CLUES TO TRACK \|** Trigger words: **<mark style="background-color:#00FF00;color:#000000">"adding crude inventory data"</mark>**, **<mark style="background-color:#00FF00;color:#000000">"2.090 million barrel draw"</mark>**. Existing background was incorrectly promoted to novel information.

## Batch 16 \| Test 078

**Article:** test_078_update_group_031.json

Expected (ground truth): **<mark style="background-color:#DCEBFA;color:#1558A6">same_event_update</mark>**

**Predicted:** different_event

**Underlying article comparison:** Super Micro **<mark style="background-color:#00FFFF;color:#000000">ships</mark>** Vera Rubin NVL72 racks (72 Rubin GPUs, 36 Vera CPUs); customer later accepts first installed rack after cooling/network tests.

**Why the expected label applies:** Customer **<mark style="background-color:#00FFFF;color:#000000">acceptance testing</mark>** is a subsequent milestone of the same shipment program, not a separate product launch.

**Observed judgement / plausible error:** The result is different_event with **<mark style="background-color:#FF00FF;color:#FFFFFF">no recorded</mark>** per-candidate judgement. A retrieval/threshold or candidate-generation failure is possible but not established. Headline and summary also claim **<mark style="background-color:#FF00FF;color:#FFFFFF">gigawatt-scale</mark>** deployment while body denies a fully operational **<mark style="background-color:#FF00FF;color:#FFFFFF">gigawatt-scale</mark>** facility.

**Judgement trace:** None recorded; predicted_events = \['E2'\]

**Error concept:** No recorded LLM judgement; candidate availability / metadata inconsistency.

**CONTEXT CLUES TO TRACK \|** Trigger words: **<mark style="background-color:#00FFFF;color:#000000">"shipment"</mark>**, **<mark style="background-color:#00FFFF;color:#000000">"acceptance testing"</mark>**, **<mark style="background-color:#00FFFF;color:#000000">"gigawatt-scale"</mark>**. Milestone wording differs, while headline/body inconsistency complicates comparison. No Gemini judgement was recorded.

## Batch 17 \| Test 083

**Article:** test_083_update_group_033.json

Expected (ground truth): **<mark style="background-color:#DCEBFA;color:#1558A6">same_event_update</mark>**

**Predicted:** different_event

**Underlying article comparison:** SpaceXAI Colossus expansion plans first batch of 220,000 NVIDIA GB300 GPUs; later batch **<mark style="background-color:#00FFFF;color:#000000">installed</mark>** and **<mark style="background-color:#00FFFF;color:#000000">online</mark>**.

**Why the expected label applies:** Operational activation fulfills the previously announced expansion phase, explicitly identified as the same program.

**Observed judgement / plausible error:** The result is different_event with **<mark style="background-color:#FF00FF;color:#FFFFFF">no recorded</mark>** per-candidate judgement. The saved outcome does not prove whether similarity filtering, candidate generation, or another fallback caused the new event.

**Judgement trace:** None recorded; predicted_events = \['E2'\]

**Error concept:** No recorded LLM judgement; missed continuity / retrieval path.

**CONTEXT CLUES TO TRACK \|** Trigger words: **<mark style="background-color:#00FFFF;color:#000000">"scheduled"</mark>**, **<mark style="background-color:#00FFFF;color:#000000">"completed installation"</mark>**, **<mark style="background-color:#00FFFF;color:#000000">"operational"</mark>**. A planned milestone becoming real should remain connected to the same expansion. No Gemini judgement was recorded.

## Batch 24 \| Test 118

**Article:** test_118_update_group_047.json

Expected (ground truth): **<mark style="background-color:#DCEBFA;color:#1558A6">same_event_update</mark>**

Predicted: **<mark style="background-color:#DDF3E2;color:#176B38">same_event_repeated</mark>**

**Underlying article comparison:** IonQ Superion 256 deployment at NVIDIA NVAQC; **<mark style="background-color:#00FF00;color:#000000">SkyWater</mark>** **<mark style="background-color:#00FF00;color:#000000">\$1.8B</mark>** acquisition, Heights-related **<mark style="background-color:#00FF00;color:#000000">\$2B</mark>** offering, Amazon **<mark style="background-color:#00FF00;color:#000000">\$36.7M</mark>** stake.

Why the expected label applies: The batch report labels these financial details as an **<mark style="background-color:#DCEBFA;color:#1558A6">update</mark>**, but the supplied original article already includes all three.

Observed judgement / plausible error: Gemini judged **<mark style="background-color:#DDF3E2;color:#176B38">repeated</mark>** and recorded **<mark style="background-color:#DCEBFA;color:#1558A6">update</mark>** label is questionable unless the incoming fixture provides some additional, verifiably novel claim beyond these already-known facts.**<mark style="background-color:#00FF00;color:#000000">no new differences</mark>**. The ground-truth

**Recorded Gemini explanation:** Both articles report the announcement that IonQ will deploy its Superion 256 system at Nvidia's NVAQC facility in 2027.

**Error concept:** Possible ground-truth weakness; do not automatically count as a semantic model error.

**CONTEXT CLUES TO TRACK \|** Trigger words: **<mark style="background-color:#00FF00;color:#000000">"SkyWater"</mark>**, **<mark style="background-color:#00FF00;color:#000000">"\$1.8B"</mark>**, **<mark style="background-color:#00FF00;color:#000000">"\$2B"</mark>**, **<mark style="background-color:#00FF00;color:#000000">"\$36.7M"</mark>**. These financial facts already appear in the original, making the expected UPDATE label questionable.

# Cross-case synthesis

1\. False novelty: Tests 027, 037 and 072 show an **<mark style="background-color:#DCEBFA;color:#1558A6">update</mark>** label triggered by background or price facts that were already in the source narrative. New wording, later timestamps, and re-emphasized facts should not count as event updates.

**2. Contradiction versus chronological revision:** Tests 029 and 049 show that a later incompatible claim may be interpreted as a normal development. Reliable classification requires the exact attribute, value, effective time, source and whether the earlier assertion is being disputed or legitimately superseded.

**3. Excessive event splitting:** Tests 058, 068, 078 and 083 were labeled different events even though their synthetic descriptions link the later movement or milestone to the earlier event. The last two have no saved LLM judgement, so isolate candidate retrieval from classification before assigning blame.

4\. Questionable synthetic label: Test 118 is marked as an **<mark style="background-color:#DCEBFA;color:#1558A6">update</mark>** despite its highlighted financial details appearing in the original IonQ article. Ground-truth validation is essential before interpreting this as a model defect.

# Recommendations and final evaluation

1.  Instrument candidate retrieval: log candidate event IDs, cosine similarities, filters, timestamps and whether Gemini was called.

2.  Compare full normalized claims (entity, attribute, value, unit, effective time, source) before allowing a new_information label.

3.  Apply a contradiction check to same-attribute claims; distinguish a competing assertion from a verified subsequent revision.

4.  Model event continuity explicitly: shipment→acceptance, planned deployment→operational activation, selloff→rebound may belong to one evolving story when source text links them.

5.  Audit synthetic ground truth and headline/body consistency, especially Test 118 and Test 078; keep ambiguous cases in an adjudication set.

6.  Evaluate separately: retrieval Recall@k, classification macro-F1 on successfully retrieved candidates, and end-to-end label accuracy. Avoid interpreting BGE cosine similarity as factual agreement.

**Final conclusion: The experiment demonstrates strong overall relationship classification** but weaker handling of updates and event continuity. **The observed eight explicit Gemini errors** primarily involve novelty detection, status interpretation and event segmentation. **Two different-event results lack judgement traces** and require retrieval-stage diagnostics. *The apparent Test 118 mistake may partly reflect ground-truth design.* Improvements should target ***retrieval observability, structured fact comparisons, temporal reasoning and dataset adjudication*—not threshold adjustment alone.**

# Evidence used

Saved run: evaluation/consolidation/results/gemini_125_completed.json in nghia_mike_gemini(2).zip; uploaded test_027, 029, 037, 049, 058, 068, 072, 078, 083, 118 JSON files; uploaded batch_006, 008, 010, 012, 014, 015, 016, 017, 024 HTML reports. All factual examples concern the evaluation fixtures, not independently verified real-world events.
