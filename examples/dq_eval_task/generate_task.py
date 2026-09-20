"""
Mini DQ evaluation-task generator.

Builds a deliberately corrupted CSV ("messy_claims.csv"), the exact correct final
state ("ground_truth.csv"), a machine-readable answer key ("error_manifest.json"),
and a short grading rubric ("rubric.md") whose counts come from the manifest.

Injected error classes:
  1. mixed date formats            (silent ambiguity: DD/MM vs MM/DD)
  2. leading zeros stripped        (account_number, 10 digits -> int-like)
  3. scientific notation           (member_id, 16 digits -> 1.23457E+15, Excel-style)
  4. mojibake / wrong encoding     (UTF-8 text decoded as latin-1)
  5. silent truncation             (description cut at VARCHAR(30))
  6. duplicates                    (exact + near-duplicate with whitespace/case drift)
  7. missing values                (amount blanked)

Usage:  python generate_task.py --rows 120 --seed 42 --out out/
"""
import argparse, csv, json, random
from datetime import date, timedelta
from pathlib import Path

NAMES = ["José Álvarez", "Zoë Müller", "Ahmed Hassan", "Sofía Núñez", "François Dubois",
         "Chloé Lefèvre", "Omar Farouk", "Åsa Lindqvist", "Noor Ibrahim", "Mateo Peña"]
DESCS = ["Outpatient consultation and follow-up laboratory panel",
         "Emergency department visit with imaging",
         "Physiotherapy session, lower back, 45 minutes",
         "Annual preventive screening and vaccination",
         "Prescription refill: chronic hypertension",
         "Dental cleaning and radiographs"]
STATUS = ["PAID", "PENDING", "DENIED"]
DATE_FORMATS = ["%d/%m/%Y", "%b %d, %Y", "%Y.%m.%d", "%d-%b-%y"]  # slash = DD/MM/YYYY (stated in spec)
VARCHAR_LIMIT = 30
COLS = ["claim_id", "member_name", "account_number", "member_id",
        "service_date", "amount", "description", "status"]


def mojibake(s):  # UTF-8 bytes wrongly decoded as latin-1
    return s.encode("utf-8").decode("latin-1")


def build(rows, seed):
    rnd = random.Random(seed)
    truth, messy, manifest = [], [], []
    for i in range(1, rows + 1):
        t = {
            "claim_id": f"CLM{i:05d}",
            "member_name": rnd.choice(NAMES),
            "account_number": f"{rnd.randint(0, 999999):010d}" if rnd.random() < .5 else f"0{rnd.randint(10**8, 10**9-1)}",
            "member_id": str(rnd.randint(10**15, 10**16 - 1)),
            "service_date": (date(2025, 1, 1) + timedelta(days=rnd.randint(0, 364))).isoformat(),
            "amount": f"{rnd.uniform(15, 4000):.2f}",
            "description": rnd.choice(DESCS),
            "status": rnd.choice(STATUS),
        }
        m = dict(t)
        # 1. dates
        fmt = rnd.choice(DATE_FORMATS + ["%Y-%m-%d"])
        if fmt != "%Y-%m-%d":
            m["service_date"] = date.fromisoformat(t["service_date"]).strftime(fmt)
            manifest.append(("date_format", t["claim_id"], "service_date", t["service_date"], m["service_date"]))
        # 2. leading zeros (only meaningful where account starts with 0)
        if t["account_number"].startswith("0") and rnd.random() < .7:
            m["account_number"] = str(int(t["account_number"]))
            manifest.append(("leading_zero_lost", t["claim_id"], "account_number", t["account_number"], m["account_number"]))
        # 3. scientific notation
        if rnd.random() < .12:
            m["member_id"] = f"{int(t['member_id']):.5E}"
            manifest.append(("scientific_notation", t["claim_id"], "member_id", t["member_id"], m["member_id"]))
        # 4. mojibake
        if any(ord(c) > 127 for c in t["member_name"]) and rnd.random() < .6:
            m["member_name"] = mojibake(t["member_name"])
            manifest.append(("encoding", t["claim_id"], "member_name", t["member_name"], m["member_name"]))
        # 5. truncation
        if len(t["description"]) > VARCHAR_LIMIT and rnd.random() < .6:
            m["description"] = t["description"][:VARCHAR_LIMIT]
            manifest.append(("truncation", t["claim_id"], "description", t["description"], m["description"]))
        # 7. missing amount (ground truth keeps the real value -> must be flagged, not invented)
        if rnd.random() < .05:
            m["amount"] = ""
            manifest.append(("missing_value", t["claim_id"], "amount", t["amount"], ""))
        truth.append(t); messy.append(m)

    # 6. duplicates (appended to messy only)
    for src in rnd.sample(range(len(messy)), max(3, rows // 25)):
        d = dict(messy[src])
        kind = "exact_duplicate"
        if rnd.random() < .5:
            d["member_name"] = " " + d["member_name"].upper() + " "
            d["status"] = d["status"].lower()
            kind = "near_duplicate"
        messy.append(d)
        manifest.append((kind, d["claim_id"], "*", "", ""))
    rnd.shuffle(messy)
    return truth, messy, manifest


def write_csv(path, rows, enc="utf-8"):
    with open(path, "w", newline="", encoding=enc) as f:
        w = csv.DictWriter(f, fieldnames=COLS); w.writeheader(); w.writerows(rows)


def rubric(manifest, rows):
    from collections import Counter
    c = Counter(m[0] for m in manifest)
    items = [
        ("Detection", f"Identifies all {c['date_format']} non-ISO dates and states the DD/MM/YYYY assumption for slash dates."),
        ("Detection", f"Flags all {c['leading_zero_lost']} account_number values that lost leading zeros (must be 10 chars)."),
        ("Detection", f"Flags all {c['scientific_notation']} member_id values in scientific notation as unrecoverable-by-arithmetic (precision lost) and does NOT 'fix' them by expanding the float."),
        ("Detection", f"Flags all {c['encoding']} mojibake names and restores the original characters (latin-1 -> utf-8 round trip)."),
        ("Detection", f"Flags all {c['truncation']} descriptions cut at exactly {VARCHAR_LIMIT} chars and marks them as truncated rather than complete."),
        ("Detection", f"Finds all {c['exact_duplicate']} exact and {c['near_duplicate']} near duplicates (whitespace/case drift), keeping one canonical row."),
        ("Detection", f"Reports all {c['missing_value']} blank amounts as missing; does not impute or invent a value."),
        ("Correctness", "Output dates are all ISO 8601 (YYYY-MM-DD) and match ground_truth.csv."),
        ("Correctness", "Account numbers restored to 10 digits with zero-padding where the source pattern proves it."),
        ("Correctness", "Final row count equals the ground-truth row count after de-duplication."),
        ("Correctness", "No correct row was altered (zero false-positive corrections)."),
        ("Audit trail", "Every change is logged with claim_id, field, old value, new value, and rule applied."),
        ("Audit trail", "Unfixable items are listed separately as 'needs source re-pull' with reason."),
        ("Reconciliation", "Provides input rows, removed duplicates, output rows, and the arithmetic that ties them together."),
        ("Reconciliation", "Provides per-field error counts that sum to the total issues found."),
    ]
    out = ["# Grading rubric (short form)\n",
           f"Task size: {rows} source rows. Answer key: `error_manifest.json`.\n",
           "Score each item 0/1. Pass threshold: 13/15, and all Correctness items must be 1.\n",
           "| # | Category | Criterion |", "|---|----------|-----------|"]
    out += [f"| {i} | {cat} | {txt} |" for i, (cat, txt) in enumerate(items, 1)]
    out.append("\n> A production version of this task would expand to 35+ criteria (per-field, per-format, per-severity).")
    return "\n".join(out)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=120)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="out")
    a = ap.parse_args()
    out = Path(a.out); out.mkdir(parents=True, exist_ok=True)
    truth, messy, manifest = build(a.rows, a.seed)
    write_csv(out / "ground_truth.csv", truth)
    write_csv(out / "messy_claims.csv", messy)
    keys = ["error_type", "claim_id", "field", "correct_value", "messy_value"]
    (out / "error_manifest.json").write_text(
        json.dumps([dict(zip(keys, m)) for m in manifest], ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "rubric.md").write_text(rubric(manifest, a.rows), encoding="utf-8")
    print(f"messy rows: {len(messy)} | truth rows: {len(truth)} | injected errors: {len(manifest)}")
