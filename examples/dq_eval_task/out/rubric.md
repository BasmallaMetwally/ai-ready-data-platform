# Grading rubric (short form)

Task size: 120 source rows. Answer key: `error_manifest.json`.

Score each item 0/1. Pass threshold: 13/15, and all Correctness items must be 1.

| # | Category | Criterion |
|---|----------|-----------|
| 1 | Detection | Identifies all 95 non-ISO dates and states the DD/MM/YYYY assumption for slash dates. |
| 2 | Detection | Flags all 87 account_number values that lost leading zeros (must be 10 chars). |
| 3 | Detection | Flags all 13 member_id values in scientific notation as unrecoverable-by-arithmetic (precision lost) and does NOT 'fix' them by expanding the float. |
| 4 | Detection | Flags all 55 mojibake names and restores the original characters (latin-1 -> utf-8 round trip). |
| 5 | Detection | Flags all 76 descriptions cut at exactly 30 chars and marks them as truncated rather than complete. |
| 6 | Detection | Finds all 3 exact and 1 near duplicates (whitespace/case drift), keeping one canonical row. |
| 7 | Detection | Reports all 5 blank amounts as missing; does not impute or invent a value. |
| 8 | Correctness | Output dates are all ISO 8601 (YYYY-MM-DD) and match ground_truth.csv. |
| 9 | Correctness | Account numbers restored to 10 digits with zero-padding where the source pattern proves it. |
| 10 | Correctness | Final row count equals the ground-truth row count after de-duplication. |
| 11 | Correctness | No correct row was altered (zero false-positive corrections). |
| 12 | Audit trail | Every change is logged with claim_id, field, old value, new value, and rule applied. |
| 13 | Audit trail | Unfixable items are listed separately as 'needs source re-pull' with reason. |
| 14 | Reconciliation | Provides input rows, removed duplicates, output rows, and the arithmetic that ties them together. |
| 15 | Reconciliation | Provides per-field error counts that sum to the total issues found. |

> A production version of this task would expand to 35+ criteria (per-field, per-format, per-severity).