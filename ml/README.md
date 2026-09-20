# Machine-Learning Workflows

ML workflows trained from the e-commerce warehouse to demonstrate how reliable
datasets can support analytics and AI use cases.

| Workflow | Output |
| --- | --- |
| Forecasting | Daily revenue forecast |
| Customer segmentation | Customer clusters and summaries |
| Anomaly detection | Unusual sales days |
| Recommendations | Item-item similarity recommendations |

Models are trained and saved through:

```bash
python3 run_all.py
```

Generated model artifacts are deliberately gitignored: they can be recreated
from the synthetic input data, keeping the repository reproducible and small.
