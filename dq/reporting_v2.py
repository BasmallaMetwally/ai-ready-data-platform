"""
Reporting Module v2 — Interactive Dashboard
---------------------------------------------
An interactive HTML report (not static like v1):
- Tabs for the different sections
- Searchable, sortable tables (vanilla JS)
- A real trend chart of the Data Quality Score over time (Chart.js)
- An Auto-Remediation section when it was run (before/after + Audit Trail)
- Dark mode aware, responsive, self-contained
"""
import base64
import io
import json
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from jinja2 import Template


def _fig_to_base64(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def _missing_heatmap(missing_report):
    cols = [m["column"] for m in missing_report]
    pcts = [m["missing_pct"] for m in missing_report]
    fig, ax = plt.subplots(figsize=(8, max(2, len(cols) * 0.4)))
    colors = ["#C4472B" if p >= 15 else "#E0A639" if p >= 5 else "#2F6F5E" for p in pcts]
    ax.barh(cols, pcts, color=colors)
    ax.set_xlabel("Missing values (%)")
    ax.set_title("Missing Data by Column")
    ax.invert_yaxis()
    for i, p in enumerate(pcts):
        ax.text(p + 0.3, i, f"{p}%", va="center", fontsize=9)
    fig.tight_layout()
    return _fig_to_base64(fig)


def _score_gauge(overall_score):
    fig, ax = plt.subplots(figsize=(4, 4), subplot_kw={"aspect": "equal"})
    color = "#2F6F5E" if overall_score >= 80 else "#E0A639" if overall_score >= 60 else "#C4472B"
    ax.pie(
        [overall_score, 100 - overall_score],
        colors=[color, "#E7E2D6"],
        startangle=90,
        counterclock=False,
        wedgeprops={"width": 0.35},
    )
    ax.text(0, 0, f"{overall_score}", ha="center", va="center", fontsize=28, fontweight="bold", color="#26241F")
    ax.text(0, -0.28, "/ 100", ha="center", va="center", fontsize=11, color="#6B6656")
    fig.tight_layout()
    return _fig_to_base64(fig)


def _column_scores_chart(column_scores):
    if not column_scores:
        return None
    cols = list(column_scores.keys())
    vals = list(column_scores.values())
    order = np.argsort(vals)
    cols = [cols[i] for i in order]
    vals = [vals[i] for i in order]
    fig, ax = plt.subplots(figsize=(8, max(2, len(cols) * 0.4)))
    colors = ["#2F6F5E" if v >= 80 else "#E0A639" if v >= 60 else "#C4472B" for v in vals]
    ax.barh(cols, vals, color=colors)
    ax.set_xlim(0, 100)
    ax.set_xlabel("Column Quality Score")
    ax.set_title("Score by Column")
    for i, v in enumerate(vals):
        ax.text(v + 1, i, f"{v}", va="center", fontsize=9)
    fig.tight_layout()
    return _fig_to_base64(fig)


def _outlier_boxplots(df, outlier_report):
    cols = [o["column"] for o in outlier_report if o["outlier_count"] > 0]
    if not cols:
        return None
    fig, axes = plt.subplots(1, len(cols), figsize=(3.2 * len(cols), 4))
    if len(cols) == 1:
        axes = [axes]
    for ax, col in zip(axes, cols):
        data = pd.to_numeric(df[col], errors="coerce").dropna()
        bp = ax.boxplot(data, patch_artist=True, widths=0.5)
        bp["boxes"][0].set_facecolor("#E0A639")
        bp["boxes"][0].set_alpha(0.7)
        ax.set_title(col, fontsize=10)
        ax.set_xticks([])
    fig.suptitle("Outlier Distribution (IQR method)")
    fig.tight_layout()
    return _fig_to_base64(fig)


REPORT_TEMPLATE = """
<!DOCTYPE html>
<html lang="en" dir="ltr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Data Quality Report — {{ dataset_name }}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  :root {
    --bg: #FAF8F3; --panel: #FFFFFF; --ink: #26241F; --muted: #6B6656;
    --line: #E7E2D6; --good: #2F6F5E; --warn: #E0A639; --bad: #C4472B; --accent: #2F6F5E;
  }
  * { box-sizing: border-box; }
  @media (prefers-color-scheme: dark) {
    :root:not([data-theme="light"]) { --bg:#1C1B17; --panel:#26241F; --ink:#F0ECE0; --muted:#A69F8C; --line:#3A382F; }
  }
  :root[data-theme="dark"] { --bg:#1C1B17; --panel:#26241F; --ink:#F0ECE0; --muted:#A69F8C; --line:#3A382F; }
  body { margin:0; background:var(--bg); color:var(--ink); font-family:'Inter','Segoe UI',Tahoma,sans-serif; line-height:1.6; }
  header { padding:32px 6vw 24px; border-bottom:1px solid var(--line); background:var(--panel); }
  header .eyebrow { color:var(--muted); font-size:.85rem; margin-bottom:6px; }
  header h1 { margin:0 0 8px; font-size:1.7rem; }
  header .meta { color:var(--muted); font-size:.9rem; }

  nav.tabs { display:flex; gap:4px; padding:0 6vw; background:var(--panel); border-bottom:1px solid var(--line); overflow-x:auto; }
  nav.tabs button {
    background:none; border:none; padding:14px 16px; font-size:.92rem; color:var(--muted);
    cursor:pointer; border-bottom:3px solid transparent; white-space:nowrap; font-family:inherit;
  }
  nav.tabs button.active { color:var(--accent); border-bottom-color:var(--accent); font-weight:600; }

  main { padding:28px 6vw 60px; max-width:1150px; margin:0 auto; }
  .tab-panel { display:none; }
  .tab-panel.active { display:block; }
  section { margin-bottom:36px; }
  h2 { font-size:1.15rem; margin-bottom:16px; }

  .summary-grid { display:flex; gap:24px; align-items:center; flex-wrap:wrap; }
  .gauge img { width:200px; }
  .summary-stats { display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:14px; flex:1; min-width:280px; }
  .stat-card { background:var(--panel); border:1px solid var(--line); border-radius:6px; padding:14px; text-align:center; }
  .stat-card .num { font-size:1.5rem; font-weight:700; }
  .stat-card .label { color:var(--muted); font-size:.82rem; margin-top:4px; }

  .table-toolbar { display:flex; justify-content:space-between; align-items:center; margin-bottom:10px; gap:10px; flex-wrap:wrap; }
  .table-toolbar input {
    padding:7px 12px; border:1px solid var(--line); border-radius:5px; background:var(--panel);
    color:var(--ink); font-family:inherit; font-size:.88rem; min-width:200px;
  }
  .table-wrap { overflow-x:auto; background:var(--panel); border:1px solid var(--line); border-radius:6px; }
  table { width:100%; border-collapse:collapse; font-size:.88rem; }
  th, td { padding:9px 12px; text-align:left; border-bottom:1px solid var(--line); }
  th { background:rgba(0,0,0,0.03); font-weight:600; cursor:pointer; user-select:none; white-space:nowrap; }
  th:hover { color:var(--accent); }
  th .arrow { font-size:.7rem; opacity:.5; margin-left:4px; }
  tr:last-child td { border-bottom:none; }

  .badge { display:inline-block; padding:2px 10px; border-radius:20px; font-size:.75rem; font-weight:600; color:#fff; }
  .badge.ok { background:var(--good); } .badge.warning { background:var(--warn); } .badge.critical { background:var(--bad); }

  .chart-wrap { background:var(--panel); border:1px solid var(--line); border-radius:6px; padding:16px; }
  .chart-wrap img { max-width:100%; }
  .trend-canvas-wrap { background:var(--panel); border:1px solid var(--line); border-radius:6px; padding:20px; height:320px; }
  .empty-note { color:var(--muted); font-style:italic; }
  footer { padding:20px 6vw; color:var(--muted); font-size:.82rem; text-align:center; }

  .remediation-grid { display:flex; gap:24px; flex-wrap:wrap; margin-bottom:20px; }
  .remediation-grid .stat-card { min-width:130px; }
</style>
</head>
<body>

<header>
  <div class="eyebrow">Data Quality Report — Interactive</div>
  <h1>Data Quality Report — {{ dataset_name }}</h1>
  <div class="meta">Run date: {{ run_time }} &nbsp;|&nbsp; Rows: {{ total_rows }} &nbsp;|&nbsp; Columns: {{ total_cols }}</div>
</header>

<nav class="tabs" id="tabNav">
  <button data-tab="overview" class="active">Overview</button>
  <button data-tab="missing">Missing</button>
  <button data-tab="duplicates">Duplicates</button>
  <button data-tab="outliers">Outliers</button>
  {% if schema_report %}<button data-tab="schema">Schema</button>{% endif %}
  {% if consistency_report %}<button data-tab="consistency">Consistency</button>{% endif %}
  {% if history_rows %}<button data-tab="history">History</button>{% endif %}
  {% if remediation %}<button data-tab="remediation">Auto-Remediation</button>{% endif %}
</nav>

<main>

<div class="tab-panel active" id="tab-overview">
  <section>
    <h2>Executive Summary</h2>
    <div class="summary-grid">
      <div class="gauge"><img src="data:image/png;base64,{{ gauge_img }}"></div>
      <div class="summary-stats">
        <div class="stat-card"><div class="num">{{ critical_missing_cols }}</div><div class="label">Columns with critical missing data</div></div>
        <div class="stat-card"><div class="num">{{ full_dup_count }}</div><div class="label">Duplicate rows</div></div>
        <div class="stat-card"><div class="num">{{ pk_dup_count }}</div><div class="label">Primary key duplicates</div></div>
        <div class="stat-card"><div class="num">{{ total_outliers }}</div><div class="label">Outlier values</div></div>
        <div class="stat-card"><div class="num">{{ total_schema_violations }}</div><div class="label">Schema violations</div></div>
        <div class="stat-card"><div class="num">{{ total_consistency_violations }}</div><div class="label">Consistency violations</div></div>
      </div>
    </div>
  </section>

  <section>
    <h2>Top Reasons for Point Deductions</h2>
    {% if deductions %}
    <div class="table-toolbar"><input type="text" class="table-search" data-table="tbl-deductions" placeholder="Search..."></div>
    <div class="table-wrap"><table class="sortable" id="tbl-deductions">
      <thead><tr><th>Reason</th><th>Column</th><th data-type="num">Points deducted</th></tr></thead>
      <tbody>
        {% for d in deductions %}
        <tr><td>{{ d.reason }}</td><td>{{ d.column or '—' }}</td><td>-{{ d.points }}</td></tr>
        {% endfor %}
      </tbody>
    </table></div>
    {% else %}
    <p class="empty-note">No significant issues found — the data is relatively clean 👍</p>
    {% endif %}
  </section>

  <section>
    <h2>Score by Column</h2>
    {% if column_scores_img %}
    <div class="chart-wrap"><img src="data:image/png;base64,{{ column_scores_img }}"></div>
    {% else %}
    <p class="empty-note">No columns have enough penalties to display here.</p>
    {% endif %}
  </section>
</div>

<div class="tab-panel" id="tab-missing">
  <section>
    <h2>Missing Values Detail</h2>
    <div class="chart-wrap"><img src="data:image/png;base64,{{ missing_img }}"></div>
    <div class="table-toolbar" style="margin-top:16px;"><input type="text" class="table-search" data-table="tbl-missing" placeholder="Search by column name..."></div>
    <div class="table-wrap"><table class="sortable" id="tbl-missing">
      <thead><tr><th>Column</th><th data-type="num">Missing count</th><th data-type="num">Percent</th><th>Status</th></tr></thead>
      <tbody>
        {% for m in missing_report %}
        <tr>
          <td>{{ m.column }}</td><td>{{ m.missing_count }}</td><td>{{ m.missing_pct }}%</td>
          <td><span class="badge {{ m.severity }}">{{ m.severity }}</span></td>
        </tr>
        {% endfor %}
      </tbody>
    </table></div>
  </section>
</div>

<div class="tab-panel" id="tab-duplicates">
  <section>
    <h2>Duplicates</h2>
    <div class="table-wrap"><table>
      <tr><th>Type</th><th>Count</th></tr>
      <tr><td>Fully duplicated rows</td><td>{{ full_dup_count }}</td></tr>
      <tr><td>Primary key duplicates ({{ primary_key or '—' }})</td><td>{{ pk_dup_count }}</td></tr>
    </table></div>
  </section>
</div>

<div class="tab-panel" id="tab-outliers">
  <section>
    <h2>Outliers</h2>
    {% if outlier_img %}<div class="chart-wrap"><img src="data:image/png;base64,{{ outlier_img }}"></div>{% endif %}
    <div class="table-toolbar" style="margin-top:16px;"><input type="text" class="table-search" data-table="tbl-outliers" placeholder="Search by column name..."></div>
    <div class="table-wrap"><table class="sortable" id="tbl-outliers">
      <thead><tr><th>Column</th><th>Normal range</th><th data-type="num">Outlier count</th><th data-type="num">Percent</th></tr></thead>
      <tbody>
        {% for o in outlier_report %}
        <tr><td>{{ o.column }}</td><td>[{{ o.lower_bound }}, {{ o.upper_bound }}]</td><td>{{ o.outlier_count }}</td><td>{{ o.outlier_pct }}%</td></tr>
        {% endfor %}
      </tbody>
    </table></div>
  </section>
</div>

{% if schema_report %}
<div class="tab-panel" id="tab-schema">
  <section>
    <h2>Schema Violations</h2>
    <div class="table-wrap"><table class="sortable" id="tbl-schema">
      <thead><tr><th>Column</th><th>Rule</th><th data-type="num">Violation count</th></tr></thead>
      <tbody>
        {% for s in schema_report %}
        <tr><td>{{ s.column }}</td><td>{{ s.rule }}</td><td>{{ s.violation_count }}</td></tr>
        {% endfor %}
      </tbody>
    </table></div>
  </section>
</div>
{% endif %}

{% if consistency_report %}
<div class="tab-panel" id="tab-consistency">
  <section>
    <h2>Consistency Violations</h2>
    <div class="table-wrap"><table>
      <tr><th>Check</th><th>Violation count</th></tr>
      {% for c in consistency_report %}
      <tr><td>{{ c.check }}</td><td>{{ c.violation_count }}</td></tr>
      {% endfor %}
    </table></div>
  </section>
</div>
{% endif %}

{% if history_rows %}
<div class="tab-panel" id="tab-history">
  <section>
    <h2>Data Quality Over Time</h2>
    <div class="trend-canvas-wrap"><canvas id="trendChart"></canvas></div>
    <div class="table-toolbar" style="margin-top:16px;"><input type="text" class="table-search" data-table="tbl-history" placeholder="Search..."></div>
    <div class="table-wrap"><table class="sortable" id="tbl-history">
      <thead><tr><th data-type="num">#</th><th>Date</th><th data-type="num">Rows</th><th data-type="num">Score</th></tr></thead>
      <tbody>
        {% for h in history_rows %}
        <tr><td>{{ h.run_id }}</td><td>{{ h.run_timestamp }}</td><td>{{ h.total_rows }}</td><td>{{ h.overall_score }}</td></tr>
        {% endfor %}
      </tbody>
    </table></div>
  </section>
</div>
{% endif %}

{% if remediation %}
<div class="tab-panel" id="tab-remediation">
  <section>
    <h2>Auto-Remediation Result</h2>
    <div class="remediation-grid">
      <div class="stat-card"><div class="num">{{ remediation.score_before }}</div><div class="label">Score before</div></div>
      <div class="stat-card"><div class="num">{{ remediation.score_after }}</div><div class="label">Score after</div></div>
      <div class="stat-card"><div class="num">{{ remediation.total_actions }}</div><div class="label">Total changes</div></div>
      <div class="stat-card"><div class="num">{{ remediation.rows_removed }}</div><div class="label">Rows removed</div></div>
    </div>

    {% if remediation.column_comparison %}
    <h2>Per-Column Score Comparison (Before/After)</h2>
    <div class="table-toolbar"><input type="text" class="table-search" data-table="tbl-comparison" placeholder="Search..."></div>
    <div class="table-wrap"><table class="sortable" id="tbl-comparison">
      <thead><tr><th>Column</th><th data-type="num">Before</th><th data-type="num">After</th><th data-type="num">Delta</th></tr></thead>
      <tbody>
        {% for c in remediation.column_comparison %}
        <tr>
          <td>{{ c.column }}</td><td>{{ c.before }}</td><td>{{ c.after }}</td>
          <td style="color: {{ 'var(--good)' if c.delta > 0 else ('var(--bad)' if c.delta < 0 else 'var(--muted)') }};">
            {{ '+' if c.delta > 0 else '' }}{{ c.delta }}
          </td>
        </tr>
        {% endfor %}
      </tbody>
    </table></div>
    {% endif %}

    {% if remediation.skipped_columns %}
    <h2>Skipped Columns (No Changes Applied)</h2>
    <p class="empty-note">These columns had no suitable cleaning strategy applied to them — they aren't missing from the check, they were deliberately left unchanged.</p>
    <div class="table-wrap"><table>
      <thead><tr><th>Column</th><th>Reason</th></tr></thead>
      <tbody>
        {% for s in remediation.skipped_columns %}
        <tr><td>{{ s.column }}</td><td>{{ s.reason }}</td></tr>
        {% endfor %}
      </tbody>
    </table></div>
    {% endif %}

    <h2>Audit Trail (first 100 changes)</h2>
    <div class="table-toolbar"><input type="text" class="table-search" data-table="tbl-audit" placeholder="Search..."></div>
    <div class="table-wrap"><table class="sortable" id="tbl-audit">
      <thead><tr><th data-type="num">Row</th><th>Column</th><th>Old value</th><th>New value</th><th>Action</th><th>Reason</th></tr></thead>
      <tbody>
        {% for a in remediation.audit_sample %}
        <tr><td>{{ a.row_index }}</td><td>{{ a.column or '—' }}</td><td>{{ a.old_value }}</td><td>{{ a.new_value }}</td><td>{{ a.action }}</td><td>{{ a.reason }}</td></tr>
        {% endfor %}
      </tbody>
    </table></div>
  </section>
</div>
{% endif %}

</main>

<footer>Data Quality Validation System &mdash; auto-generated interactive report</footer>

<script>
(function() {
  // ---- Tabs ----
  var buttons = document.querySelectorAll('#tabNav button');
  buttons.forEach(function(btn) {
    btn.addEventListener('click', function() {
      buttons.forEach(function(b) { b.classList.remove('active'); });
      document.querySelectorAll('.tab-panel').forEach(function(p) { p.classList.remove('active'); });
      btn.classList.add('active');
      document.getElementById('tab-' + btn.dataset.tab).classList.add('active');
    });
  });

  // ---- Table search ----
  document.querySelectorAll('.table-search').forEach(function(input) {
    input.addEventListener('input', function() {
      var table = document.getElementById(input.dataset.table);
      var q = input.value.trim().toLowerCase();
      table.querySelectorAll('tbody tr').forEach(function(row) {
        row.style.display = row.textContent.toLowerCase().indexOf(q) !== -1 ? '' : 'none';
      });
    });
  });

  // ---- Sortable tables ----
  document.querySelectorAll('table.sortable').forEach(function(table) {
    var headers = table.querySelectorAll('thead th');
    headers.forEach(function(th, colIndex) {
      var state = 0; // 0 none, 1 asc, 2 desc
      th.addEventListener('click', function() {
        var tbody = table.querySelector('tbody');
        var rows = Array.prototype.slice.call(tbody.querySelectorAll('tr'));
        var isNum = th.dataset.type === 'num';
        state = state === 1 ? 2 : 1;
        headers.forEach(function(h) { if (h !== th) h.querySelector('.arrow') && (h.querySelector('.arrow').remove()); });
        var oldArrow = th.querySelector('.arrow'); if (oldArrow) oldArrow.remove();
        var arrow = document.createElement('span');
        arrow.className = 'arrow';
        arrow.textContent = state === 1 ? '▲' : '▼';
        th.appendChild(arrow);

        rows.sort(function(a, b) {
          var av = a.children[colIndex].textContent.trim();
          var bv = b.children[colIndex].textContent.trim();
          if (isNum) {
            av = parseFloat(av.replace('%', '').replace('-', '')) || 0;
            bv = parseFloat(bv.replace('%', '').replace('-', '')) || 0;
            return state === 1 ? av - bv : bv - av;
          }
          return state === 1 ? av.localeCompare(bv) : bv.localeCompare(av);
        });
        rows.forEach(function(r) { tbody.appendChild(r); });
      });
    });
  });

  // ---- Trend chart ----
  var trendCanvas = document.getElementById('trendChart');
  if (trendCanvas && window.Chart) {
    var labels = {{ history_labels_json | safe }};
    var scores = {{ history_scores_json | safe }};
    new Chart(trendCanvas.getContext('2d'), {
      type: 'line',
      data: {
        labels: labels,
        datasets: [{
          label: 'Data Quality Score',
          data: scores,
          borderColor: '#2F6F5E',
          backgroundColor: 'rgba(47,111,94,0.12)',
          tension: 0.3,
          fill: true,
          pointRadius: 4,
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: { y: { min: 0, max: 100 } },
        plugins: { legend: { display: false } }
      }
    });
  }
})();
</script>

</body>
</html>
"""


def build_report(df, results, score_result, dataset_name="dataset",
                  primary_key=None, history_rows=None, output_path="dq_report.html",
                  remediation_info=None):
    missing_report = results.get("missing", [])
    duplicates = results.get("duplicates", {})
    outlier_report = results.get("outliers", [])
    schema_report = results.get("schema", [])
    consistency_report = results.get("consistency", [])

    gauge_img = _score_gauge(score_result["overall_score"])
    missing_img = _missing_heatmap(missing_report)
    column_scores_img = _column_scores_chart(score_result["column_scores"])
    outlier_img = _outlier_boxplots(df, outlier_report)

    history_rows = history_rows or []
    # History comes back newest-first, reverse it so the chart is in chronological order
    chrono = list(reversed(history_rows))
    history_labels_json = json.dumps([f"#{h['run_id']}" for h in chrono], ensure_ascii=False)
    history_scores_json = json.dumps([h["overall_score"] for h in chrono])

    remediation = None
    if remediation_info:
        remediation = dict(remediation_info)
        trail = remediation.get("audit_trail")
        if trail is not None and not trail.empty:
            remediation["audit_sample"] = trail.head(100).to_dict("records")
        else:
            remediation["audit_sample"] = []

    context = dict(
        dataset_name=dataset_name,
        run_time=datetime.now().strftime("%Y-%m-%d %H:%M"),
        total_rows=len(df),
        total_cols=len(df.columns),
        gauge_img=gauge_img,
        missing_img=missing_img,
        column_scores_img=column_scores_img,
        outlier_img=outlier_img,
        missing_report=missing_report,
        critical_missing_cols=sum(1 for m in missing_report if m["severity"] == "critical"),
        full_dup_count=duplicates.get("full_row_duplicates", 0),
        pk_dup_count=duplicates.get("primary_key_duplicates", 0),
        primary_key=primary_key,
        total_outliers=sum(o["outlier_count"] for o in outlier_report),
        outlier_report=outlier_report,
        total_schema_violations=sum(s["violation_count"] for s in schema_report),
        schema_report=schema_report,
        total_consistency_violations=sum(c["violation_count"] for c in consistency_report),
        consistency_report=consistency_report,
        deductions=score_result["deductions"],
        history_rows=history_rows,
        history_labels_json=history_labels_json,
        history_scores_json=history_scores_json,
        remediation=remediation,
    )

    html = Template(REPORT_TEMPLATE).render(**context)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    return output_path
