# ProjectReport

Expected files:

- `Project Report <ProjectName>.pdf`: the full project report
- `User Guide <ProjectName>.pdf`: installation and user guide (report appendix)

## To explain in the report

- [ ] **Why the rule baseline beats the local LLM on grouping F1.** Same 125 articles, same settings
  (6 candidates, lexical rank 3):

  | Run | Grouping F1 | Precision | Recall | Label macro-F1 |
  |---|---|---|---|---|
  | `2026-11-05_1519_local-qwen2.5-7b` | 0.642 | 1.000 | 0.473 | 0.453 |
  | `2026-11-05_1354_baseline-rule` | 0.735 | 0.595 | 0.960 | 0.102 |

  State the trade-off, not just the scores. The rule is greedy: it links almost every real match (recall 0.960)
  but also merges unrelated articles (precision 0.595), and gives every link the same label. The LLM is
  cautious: it never merges unrelated articles (precision 1.000) and labels far better (0.453 vs 0.102), but
  misses about half the real matches (recall 0.473). Explain why: same-event labels need evidence quotes that are
  verified on both sides, otherwise they become `uncertain`, and qwen2.5 sometimes returns no judgment for a pair
  (4 articles, scored as misses). Argue which error costs more here: a false merge puts
  one event's facts in another event's summary, while a missed link only leaves a story ungrouped. Run-to-run noise for qwen2.5 is about ±0.01 F1 (0.649, 0.636, 0.642).
