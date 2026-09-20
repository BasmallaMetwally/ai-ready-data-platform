# DQ Evaluation Task Generator (mini example)

Generates a realistic "messy" claims CSV with **known, tracked errors**, the exact **correct final state**, an **answer key**, and a **rubric** — the same structure used to evaluate AI agents on data-validation work.

```bash
python generate_task.py --rows 120 --seed 42 --out out/
```

| File | Purpose |
|------|---------|
| `messy_claims.csv` | Input given to the agent (mixed date formats, stripped leading zeros, scientific notation, mojibake, VARCHAR(30) truncation, duplicates, blank amounts) |
| `ground_truth.csv` | Correct final state |
| `error_manifest.json` | Every injected error: type, claim_id, field, correct vs messy value |
| `rubric.md` | Short grading rubric with counts derived from the manifest |

**Spec assumption stated to the agent:** slash dates are DD/MM/YYYY. Deterministic via `--seed`.
