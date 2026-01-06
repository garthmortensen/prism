"""
Asset: dashboard.py
Description:
    Aggregates population-level metrics for a specific run.
    - Calculates demographics (Age, Gender).
    - Summarizes risk scores by Metal Level.
    - Generates a summary HTML dashboard.

Usage:
    Executed via the `dashboard_job` in Dagster.
"""
from pathlib import Path
from datetime import datetime

import plotly.graph_objects as go
import plotly.io as pio
from dagster import asset, ResourceParam
from sqlalchemy import text

from ra_dagster.resources.sqlalchemy_resource import SqlAlchemyResource
from ra_dagster.db.run_registry import resolve_run_id
from ra_dagster.config_schemas import DashboardConfig

VISUALIZATIONS_DIR = Path(__file__).resolve().parents[1] / "output" / "visualizations"
VISUALIZATIONS_DIR.mkdir(parents=True, exist_ok=True)


@asset(deps=["run_score_summary", "run_score_by_dim", "run_score_distribution", "run_hcc_summary", "run_rxc_summary"])
def dashboard_metrics(context, config: DashboardConfig, database: ResourceParam[SqlAlchemyResource]) -> dict:
    """
    Calculate population metrics for a specific run using materialized analytics views.
    """
    engine = database.get_engine()
    con = engine.connect()
    
    # Resolve run_ref from potential human code
    run_id = resolve_run_id(con, config.run_ref)

    try:
        # 1. Get Summary Metrics
        summary = con.execute(text("""
            SELECT member_count, avg_score, avg_age
            FROM main.run_score_summary
            WHERE run_id = :run_id
        """), {"run_id": run_id}).fetchone()

        if not summary:
            raise ValueError(f"No summary data found for run_id: {run_id}")

        total_members, avg_risk_score, avg_age = summary

        # 2. Get Gender Counts
        gender_rows = con.execute(text("""
            SELECT dimension_value, member_count
            FROM main.run_score_by_dim
            WHERE run_id = :run_id AND dimension_name = 'gender'
        """), {"run_id": run_id}).fetchall()
        
        gender_map = {row[0]: row[1] for row in gender_rows}

        # 3. Get Metal Level Counts
        metal_rows = con.execute(text("""
            SELECT lower(dimension_value), member_count
            FROM main.run_score_by_dim
            WHERE run_id = :run_id AND dimension_name = 'metal_level'
        """), {"run_id": run_id}).fetchall()
        
        metal_map = {row[0]: row[1] for row in metal_rows}

        # 4. Score Distribution
        dist_rows = con.execute(text("""
            SELECT score_bucket, member_count
            FROM main.run_score_distribution
            WHERE run_id = :run_id
            ORDER BY score_bucket
        """), {"run_id": run_id}).fetchall()
        score_dist = [{"bucket": row[0], "count": row[1]} for row in dist_rows]

        # 5. Top HCCs
        hcc_rows = con.execute(text("""
            SELECT hcc_code, prevalence, avg_score_contribution
            FROM main.run_hcc_summary
            WHERE run_id = :run_id
            ORDER BY prevalence DESC
            LIMIT 10
        """), {"run_id": run_id}).fetchall()
        top_hccs = [{"code": row[0], "prevalence": row[1], "contribution": row[2]} for row in hcc_rows]

        # 6. Top RxCs
        rxc_rows = con.execute(text("""
            SELECT rxc_code, prevalence, avg_score_contribution
            FROM main.run_rxc_summary
            WHERE run_id = :run_id
            ORDER BY prevalence DESC
            LIMIT 10
        """), {"run_id": run_id}).fetchall()
        top_rxcs = [{"code": row[0], "prevalence": row[1], "contribution": row[2]} for row in rxc_rows]
        
        # 7. Age Band
        age_rows = con.execute(text("""
            SELECT dimension_value, member_count, avg_score
            FROM main.run_score_by_dim
            WHERE run_id = :run_id AND dimension_name = 'age_band'
            ORDER BY dimension_value
        """), {"run_id": run_id}).fetchall()
        age_dist = [{"band": row[0], "count": row[1], "avg_score": row[2]} for row in age_rows]

        results = {
            "run_id": run_id,
            "description": config.run_description,
            "total_members": total_members,
            "avg_risk_score": avg_risk_score,
            "avg_age": avg_age,
            "gender_dist": gender_map,
            "metal_dist": metal_map,
            "score_dist": score_dist,
            "top_hccs": top_hccs,
            "top_rxcs": top_rxcs,
            "age_dist": age_dist,
        }

        context.log.info(f"Calculated metrics for run {run_id}: {results}")
        return results

    finally:
        con.close()


@asset
def dashboard_html(context, dashboard_metrics: dict) -> None:
    """
    Generate an HTML dashboard from the calculated metrics using Plotly.
    """
    run_id = dashboard_metrics["run_id"]
    desc = dashboard_metrics["description"]
    total_members = dashboard_metrics["total_members"]

    # Format numbers
    total = f"{total_members:,}"
    avg_score = f"{dashboard_metrics['avg_risk_score']:.3f}"
    avg_age = f"{dashboard_metrics['avg_age']:.1f}"

    # 1. Gender Distribution (Pie)
    gender_data = dashboard_metrics["gender_dist"]
    fig_gender = go.Figure(data=[go.Pie(labels=list(gender_data.keys()), values=list(gender_data.values()), hole=.3)])
    fig_gender.update_layout(title_text="Gender Distribution", autosize=True)
    html_gender = pio.to_html(fig_gender, full_html=False, include_plotlyjs=False, config={'responsive': True})

    # 2. Metal Level Distribution (Pie)
    metal_data = dashboard_metrics["metal_dist"]
    fig_metal = go.Figure(data=[go.Pie(labels=list(metal_data.keys()), values=list(metal_data.values()), hole=.3)])
    fig_metal.update_layout(title_text="Metal Level Distribution", autosize=True)
    html_metal = pio.to_html(fig_metal, full_html=False, include_plotlyjs=False, config={'responsive': True})

    # 3. Age Band Analysis (Bar + Line)
    age_data = dashboard_metrics["age_dist"]
    bands = [d["band"] for d in age_data]
    counts = [d["count"] for d in age_data]
    scores = [d["avg_score"] for d in age_data]

    fig_age = go.Figure()
    fig_age.add_trace(go.Bar(x=bands, y=counts, name="Member Count", yaxis='y1', marker_color='#17a2b8'))
    fig_age.add_trace(go.Scatter(x=bands, y=scores, name="Avg Risk Score", yaxis='y2', mode='lines+markers', line=dict(color='#dc3545')))
    
    fig_age.update_layout(
        title_text="Age Band Analysis",
        yaxis=dict(title="Member Count"),
        yaxis2=dict(title="Avg Risk Score", overlaying='y', side='right'),
        legend=dict(x=0, y=1.1, orientation='h'),
        autosize=True
    )
    html_age = pio.to_html(fig_age, full_html=False, include_plotlyjs=False, config={'responsive': True})

    # 4. Top HCC Drivers (Horizontal Bar)
    hcc_data = dashboard_metrics["top_hccs"][:]
    hcc_data.reverse() # Sort for horizontal bar chart (top at top)
    hcc_codes = [d["code"] for d in hcc_data]
    hcc_prev = [d["prevalence"] * 100 for d in hcc_data]
    
    fig_hcc = go.Figure(go.Bar(
        x=hcc_prev,
        y=hcc_codes,
        orientation='h',
        text=[f"{p:.1f}%" for p in hcc_prev],
        textposition='auto',
        marker_color='#007bff'
    ))
    fig_hcc.update_layout(title_text="Top 10 HCCs by Prevalence", xaxis_title="Prevalence (%)", autosize=True)
    html_hcc = pio.to_html(fig_hcc, full_html=False, include_plotlyjs=False, config={'responsive': True})

    # 5. Top RxC Drivers (Horizontal Bar)
    rxc_data = dashboard_metrics["top_rxcs"][:]
    rxc_data.reverse()
    rxc_codes = [d["code"] for d in rxc_data]
    rxc_prev = [d["prevalence"] * 100 for d in rxc_data]

    fig_rxc = go.Figure(go.Bar(
        x=rxc_prev,
        y=rxc_codes,
        orientation='h',
        text=[f"{p:.1f}%" for p in rxc_prev],
        textposition='auto',
        marker_color='#ffc107'
    ))
    fig_rxc.update_layout(title_text="Top 10 RxCs by Prevalence", xaxis_title="Prevalence (%)", autosize=True)
    html_rxc = pio.to_html(fig_rxc, full_html=False, include_plotlyjs=False, config={'responsive': True})

    # 6. Risk Score Distribution (Histogram)
    score_data = dashboard_metrics["score_dist"]
    buckets = [d["bucket"] for d in score_data]
    bucket_counts = [d["count"] for d in score_data]

    fig_score = go.Figure(go.Bar(x=buckets, y=bucket_counts, marker_color='#28a745'))
    fig_score.update_layout(title_text="Risk Score Distribution", xaxis_title="Risk Score", yaxis_title="Member Count", autosize=True)
    html_score = pio.to_html(fig_score, full_html=False, include_plotlyjs=False, config={'responsive': True})

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Population Dashboard: {desc}</title>
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
        <h1>Population Dashboard</h1>
        <h3>Run: {desc} <span style="font-weight:normal; font-size:0.8em; color:#888">
            ({run_id})</span></h3>
        
        <div class="metric-container">
            <div class="metric-card">
                <div class="metric-value">{total}</div>
                <div class="metric-label">Total Members</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{avg_score}</div>
                <div class="metric-label">Avg Risk Score</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{avg_age}</div>
                <div class="metric-label">Avg Age</div>
            </div>
        </div>

        <div class="chart-container">{html_gender}</div>
        <div class="chart-container">{html_metal}</div>
        <div class="chart-container">{html_age}</div>
        <div class="chart-container">{html_hcc}</div>
        <div class="chart-container">{html_rxc}</div>
        <div class="chart-container">{html_score}</div>
    </body>
    </html>
    """

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    job_name = context.run.job_name if context.run and context.run.job_name else "dashboard"
    filename = f"{job_name}_{timestamp}_{run_id}.html"

    output_path = VISUALIZATIONS_DIR / filename
    with open(output_path, "w") as f:
        f.write(html_content)

    context.log.info(f"Dashboard saved to {output_path}")
