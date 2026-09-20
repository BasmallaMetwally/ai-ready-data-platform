# Data Quality Engine

Reusable data-quality validation for CSV datasets and pandas DataFrames.
It scores quality, optionally remediates safe issues, records every run, and
produces an audit trail and HTML report.

## Capabilities

- Missing-value, duplicate, schema, range, date-consistency, and outlier checks
- Weighted score from 0–100 with clear deductions
- Optional remediation for missing values, duplicates, and outliers
- SQLite quality history and CSV audit trail
- Reusable quality gate that blocks downstream loads below a threshold

## Run it

```bash
cd dq
python3 main.py --file customers_orders.csv --name customers_orders --remediate
python3 -m unittest discover -s tests -v
```

The ETL pipeline imports `quality_gate.py` directly, so the same quality rules
protect both ad-hoc datasets and warehouse loads.
