# Unified API

FastAPI service exposing data-quality operations, pipeline execution, quality
history, and analytics generated from the warehouse and ML artifacts.

## Start locally

```bash
cd ..
python3 run_all.py
cd api
uvicorn main:app --reload --port 8000
```

Open `http://localhost:8000/docs` for interactive OpenAPI documentation.

## Endpoint groups

- **System:** `/health`
- **Data quality:** `/dq/validate`, `/quality/latest`, `/quality/history/{dataset_name}`
- **Pipeline:** `/pipeline/run`
- **Analytics:** `/forecast`, `/sales/anomalies`, `/segments/summary`
- **Recommendations:** `/products/{product_id}/similar`, `/customers/{customer_id}/recommendations`

The service loads generated models from `../models/`; run `python3 run_all.py`
before starting it on a fresh clone.
