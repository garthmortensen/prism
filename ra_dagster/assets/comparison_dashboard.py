"""
Asset: comparison_dashboard.py
Description:
    Aggregates comparison metrics for a specific comparison batch.
    - Calculates score differences and match statistics.
    - Generates a summary HTML dashboard using Plotly.

Usage:
    Executed via the `comparison_dashboard_job` in Dagster.
"""
from pathlib import Path
from datetime import datetime

import plotly.graph_objects as go
import plotly.io as pio
from dagster import asset, ResourceParam
from sqlalchemy import text

from ra_dagster.resources.sqlalchemy_resource import SqlAlchemyResource
from ra_dagster.db.run_registry import resolve_run_id
from ra_dagster.config_schemas import ComparisonDashboardConfig

VISUALIZATIONS_DIR = Path(__file__).resolve().parents[1] / "output" / "visualizations"
VISUALIZATIONS_DIR.mkdir(parents=True, exist_ok=True)


@asset(deps=["compare_runs"])
def comparison_dashboard_metrics(context, config: ComparisonDashboardConfig, database: ResourceParam[SqlAlchemyResource]) -> dict:
    """
    Calculate comparison metrics for a specific batch using the run_comparison table.
    """
    engine = database.get_engine()
    con = engine.connect()
    
    # Resolve batch_ref if it's a human code
    batch_id = resolve_run_id(con, config.batch_ref)

    try:
        # 1. Get Summary Metrics
        summary = con.execute(text("""
            SELECT 
                COUNT(*) as total_members,
                AVG(score_a) as avg_score_a,
                AVG(score_b) as avg_score_b,
                AVG(score_diff) as avg_diff,
                SUM(CASE WHEN match_status = 'matched' THEN 1 ELSE 0 END) as matched_count,
                SUM(CASE WHEN match_status = 'a_only' THEN 1 ELSE 0 END) as a_only_count,
                SUM(CASE WHEN match_status = 'b_only' THEN 1 ELSE 0 END) as b_only_count
            FROM main_analytics.run_comparison
            WHERE batch_id = :batch_id
        """), {"batch_id": batch_id}).fetchone()

        if not summary or summary[0] == 0:
            raise ValueError(f"No comparison data found for batch_id: {batch_id}")

        (total_members, avg_score_a, avg_score_b, avg_diff, 
         matched_count, a_only_count, b_only_count) = summary

        # 2. Match Status Distribution
        match_dist = {
            "Matched": matched_count,
            "Run A Only": a_only_count,
            "Run B Only": b_only_count
        }

        # 3. Score Difference Distribution (Histogram)
        diff_rows = con.execute(text("""
            SELECT score_diff
            FROM main_analytics.run_comparison
            WHERE batch_id = :batch_id AND match_status = 'matched'
        """), {"batch_id": batch_id}).fetchall()
        score_diffs = [row[0] for row in diff_rows]

        # 4. Scatter Plot Data (Sampled if too large)
        scatter_rows = con.execute(text("""
            SELECT score_a, score_b
            FROM main_analytics.run_comparison
            WHERE batch_id = :batch_id AND match_status = 'matched'
            LIMIT 5000
        """), {"batch_id": batch_id}).fetchall()
        scatter_data = [{"a": row[0], "b": row[1]} for row in scatter_rows]

        # 5. Dimension Breakdowns
        dim_rows = con.execute(text("""
            SELECT 
                dimension_name,
                dimension_value,
                total_members,
                matched_count,
                added_count,
                removed_count,
                avg_score_diff,
                avg_score_added,
                avg_score_removed
            FROM main.run_comparison_by_dim
            WHERE batch_id = :batch_id
            ORDER BY dimension_name, dimension_value
        """), {"batch_id": batch_id}).fetchall()
        
        dimension_data = {}
        for row in dim_rows:
            dim_name = row[0]
            if dim_name not in dimension_data:
                dimension_data[dim_name] = []
            
            dimension_data[dim_name].append({
                "value": row[1],
                "total": row[2],
                "matched": row[3],
                "added": row[4],
                "removed": row[5],
                "avg_diff": row[6] or 0.0,
                "avg_added": row[7] or 0.0,
                "avg_removed": row[8] or 0.0
            })

        results = {
            "batch_id": batch_id,
            "description": config.description,
            "total_members": total_members,
            "avg_score_a": avg_score_a or 0.0,
            "avg_score_b": avg_score_b or 0.0,
            "avg_diff": avg_diff or 0.0,
            "match_dist": match_dist,
            "score_diffs": score_diffs,
            "scatter_data": scatter_data,
            "dimension_data": dimension_data,
        }

        context.log.info(f"Calculated comparison metrics for batch {batch_id}")
        return results

    finally:
        con.close()


@asset
def comparison_dashboard_html(context, comparison_dashboard_metrics: dict) -> None:
    """
    Generate an HTML comparison dashboard from the calculated metrics using Plotly.
    """
    batch_id = comparison_dashboard_metrics["batch_id"]
    desc = comparison_dashboard_metrics["description"]
    
    # Format numbers
    total = f"{comparison_dashboard_metrics['total_members']:,}"
    avg_a = f"{comparison_dashboard_metrics['avg_score_a']:.3f}"
    avg_b = f"{comparison_dashboard_metrics['avg_score_b']:.3f}"
    avg_diff = f"{comparison_dashboard_metrics['avg_diff']:.3f}"
    diff_color = "green" if comparison_dashboard_metrics['avg_diff'] <= 0 else "red"

    # 1. Match Status (Pie)
    match_data = comparison_dashboard_metrics["match_dist"]
    fig_match = go.Figure(data=[go.Pie(labels=list(match_data.keys()), values=list(match_data.values()), hole=.3)])
    fig_match.update_layout(title_text="Population Overlap", autosize=True)
    html_match = pio.to_html(fig_match, full_html=False, include_plotlyjs=False, config={'responsive': True})

    # 2. Score Difference Histogram
    diffs = comparison_dashboard_metrics["score_diffs"]
    fig_hist = go.Figure(data=[go.Histogram(x=diffs, nbinsx=50, marker_color='#6f42c1')])
    fig_hist.update_layout(
        title_text="Score Difference Distribution (Matched Members)",
        xaxis_title="Score Difference (B - A)",
        yaxis_title="Count",
        autosize=True
    )
    html_hist = pio.to_html(fig_hist, full_html=False, include_plotlyjs=False, config={'responsive': True})

    # 3. Scatter Plot (A vs B)
    scatter_data = comparison_dashboard_metrics["scatter_data"]
    x_vals = [d["a"] for d in scatter_data]
    y_vals = [d["b"] for d in scatter_data]
    
    fig_scatter = go.Figure(data=go.Scatter(
        x=x_vals, 
        y=y_vals, 
        mode='markers',
        marker=dict(size=5, color='#17a2b8', opacity=0.5)
    ))
    # Add 1:1 line
    max_val = max(max(x_vals, default=0), max(y_vals, default=0))
    fig_scatter.add_shape(type="line", x0=0, y0=0, x1=max_val, y1=max_val, line=dict(color="Gray", width=1, dash="dash"))
    
    fig_scatter.update_layout(
        title_text="Risk Score Correlation (Run A vs Run B)",
        xaxis_title="Score Run A",
        yaxis_title="Score Run B",
        autosize=True
    )
    html_scatter = pio.to_html(fig_scatter, full_html=False, include_plotlyjs=False, config={'responsive': True})

    # 4. Dimension Analysis
    dimension_charts_html = ""
    dimension_data = comparison_dashboard_metrics.get("dimension_data", {})
    
    for dim_name, data in dimension_data.items():
        labels = [d["value"] for d in data]
        avg_diffs = [d["avg_diff"] for d in data]
        added = [d["added"] for d in data]
        removed = [d["removed"] for d in data]

        # Chart A: Score Impact
        fig_impact = go.Figure(data=[
            go.Bar(x=labels, y=avg_diffs, name="Avg Score Impact", marker_color='#28a745')
        ])
        fig_impact.update_layout(
            title_text=f"Model Impact by {dim_name.replace('_', ' ').title()}",
            xaxis_title=dim_name,
            yaxis_title="Avg Score Difference",
            autosize=True,
            margin=dict(l=20, r=20, t=40, b=20)
        )
        html_impact = pio.to_html(fig_impact, full_html=False, include_plotlyjs=False, config={'responsive': True})

        # Chart B: Churn
        fig_churn = go.Figure(data=[
            go.Bar(x=labels, y=added, name="Added", marker_color='#17a2b8'),
            go.Bar(x=labels, y=removed, name="Removed", marker_color='#dc3545')
        ])
        fig_churn.update_layout(
            title_text=f"Population Shift by {dim_name.replace('_', ' ').title()}",
            xaxis_title=dim_name,
            yaxis_title="Member Count",
            barmode='group',
            autosize=True,
            margin=dict(l=20, r=20, t=40, b=20)
        )
        html_churn = pio.to_html(fig_churn, full_html=False, include_plotlyjs=False, config={'responsive': True})

        dimension_charts_html += f"""
        <div class="chart-container">
            {html_impact}
            <hr style="border: 0; border-top: 1px solid #eee; margin: 20px 0;">
            {html_churn}
        </div>
        """

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Comparison Dashboard: {desc}</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            * {{ box-sizing: border-box; }}
            body {{ font-family: sans-serif; margin: 20px; background-color: #f4f4f9; }}
            h1 {{ color: #333; text-align: center; }}
            h3 {{ color: #555; text-align: center; }}
            .metric-container {{ 
                display: flex; 
                justify-content: center; 
                gap: 20px; 
                margin-bottom: 40px; 
                flex-wrap: wrap;
            }}
            .metric-card {{ 
                background: white;
                padding: 20px; 
                border-radius: 8px; 
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                text-align: center;
                min-width: 150px;
                flex: 1;
                max-width: 300px;
            }}
            .metric-value {{ font-size: 28px; font-weight: bold; color: #007bff; }}
            .metric-label {{ color: #666; margin-top: 5px; }}
            .chart-container {{ 
                width: 95%;
                max-width: 1000px;
                margin: 0 auto 30px auto;
                background: white; 
                padding: 20px; 
                border-radius: 8px; 
                box-shadow: 0 2px 5px rgba(0,0,0,0.1); 
            }}
        </style>
    </head>
    <body>
        <h1>Comparison Dashboard</h1>
        <h3>{desc} <span style="font-weight:normal; font-size:0.8em; color:#888">
            ({batch_id})</span></h3>
        
        <div class="metric-container">
            <div class="metric-card">
                <div class="metric-value">{total}</div>
                <div class="metric-label">Total Members</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{avg_a}</div>
                <div class="metric-label">Avg Score A</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{avg_b}</div>
                <div class="metric-label">Avg Score B</div>
            </div>
            <div class="metric-card">
                <div class="metric-value" style="color:{diff_color}">{avg_diff}</div>
                <div class="metric-label">Avg Difference</div>
            </div>
        </div>

        <div class="chart-container">{html_match}</div>
        <div class="chart-container">{html_hist}</div>
        <div class="chart-container">{html_scatter}</div>
        
        <h2 style="text-align:center; color:#555; margin-top:50px;">Dimension Analysis</h2>
        {dimension_charts_html}
    </body>
    </html>
    """

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    job_name = context.run.job_name if context.run and context.run.job_name else "comparison_dashboard"
    filename = f"{job_name}_{timestamp}_{batch_id}.html"

    output_path = VISUALIZATIONS_DIR / filename
    with open(output_path, "w") as f:
        f.write(html_content)

    context.log.info(f"Comparison dashboard saved to {output_path}")
