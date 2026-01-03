from pathlib import Path

from dagster import Config, asset

from ra_dagster.resources.duckdb_resource import DuckDBResource

VISUALIZATIONS_DIR = Path(__file__).resolve().parents[1] / "output" / "visualizations"
VISUALIZATIONS_DIR.mkdir(parents=True, exist_ok=True)


class DashboardConfig(Config):
    run_id: str
    run_description: str = "Dashboard Analysis"


@asset
def dashboard_metrics(context, config: DashboardConfig, duckdb: DuckDBResource) -> dict:
    """
    Calculate population metrics for a specific run.
    """
    con = duckdb.get_connection().connect()
    run_id = config.run_id

    try:
        # 1. Get Run Metadata (for benefit year to calc age)
        meta = con.execute(f"""
            SELECT benefit_year 
            FROM main_runs.run_registry 
            WHERE run_id = '{run_id}'
        """).fetchone()

        benefit_year = meta[0] if meta else 2024  # Default if not found

        # 2. Fetch Data joined with Members for DOB
        # We use a LEFT JOIN in case some members in risk_scores are missing from raw_members
        # (unlikely but possible)
        query = f"""
            WITH base AS (
                SELECT 
                    rs.member_id,
                    rs.risk_score,
                    rs.gender,
                    rs.metal_level,
                    m.dob
                FROM main_runs.risk_scores rs
                LEFT JOIN main_raw.raw_members m ON rs.member_id = m.member_id
                WHERE rs.run_id = '{run_id}'
            )
            SELECT 
                COUNT(*) as total_members,
                AVG(risk_score) as avg_risk_score,
                AVG(date_diff('year', dob, DATE '{benefit_year}-01-01')) as avg_age,
                
                -- Gender Counts
                COUNT(CASE WHEN gender = 'M' THEN 1 END) as count_male,
                COUNT(CASE WHEN gender = 'F' THEN 1 END) as count_female,
                
                -- Metal Level Counts
                COUNT(CASE WHEN LOWER(metal_level) = 'platinum' THEN 1 END) as count_platinum,
                COUNT(CASE WHEN LOWER(metal_level) = 'gold' THEN 1 END) as count_gold,
                COUNT(CASE WHEN LOWER(metal_level) = 'silver' THEN 1 END) as count_silver,
                COUNT(CASE WHEN LOWER(metal_level) = 'bronze' THEN 1 END) as count_bronze,
                COUNT(
                    CASE WHEN LOWER(metal_level) = 'catastrophic' THEN 1 END
                ) as count_catastrophic
            FROM base
        """

        metrics = con.execute(query).fetchone()

        # Unpack
        (
            total,
            avg_score,
            avg_age,
            m_count,
            f_count,
            plat_count,
            gold_count,
            silver_count,
            bronze_count,
            cat_count,
        ) = metrics

        results = {
            "run_id": run_id,
            "description": config.run_description,
            "total_members": total,
            "avg_risk_score": avg_score if avg_score else 0.0,
            "avg_age": avg_age if avg_age else 0.0,
            "gender_dist": {
                "Male": m_count,
                "Female": f_count,
                "Unknown": total - (m_count + f_count),
            },
            "metal_dist": {
                "Platinum": plat_count,
                "Gold": gold_count,
                "Silver": silver_count,
                "Bronze": bronze_count,
                "Catastrophic": cat_count,
                "Other": total
                - (plat_count + gold_count + silver_count + bronze_count + cat_count),
            },
        }

        context.log.info(f"Calculated metrics for run {run_id}: {results}")
        return results

    finally:
        con.close()


@asset
def dashboard_html(context, dashboard_metrics: dict) -> None:
    """
    Generate an HTML dashboard from the calculated metrics.
    """
    run_id = dashboard_metrics["run_id"]
    desc = dashboard_metrics["description"]

    # Format numbers
    total = f"{dashboard_metrics['total_members']:,}"
    avg_score = f"{dashboard_metrics['avg_risk_score']:.3f}"
    avg_age = f"{dashboard_metrics['avg_age']:.1f}"

    # Gender Table Rows
    gender_rows = ""
    for g, count in dashboard_metrics["gender_dist"].items():
        pct = (
            (count / dashboard_metrics["total_members"] * 100)
            if dashboard_metrics["total_members"] > 0
            else 0
        )
        gender_rows += f"<tr><td>{g}</td><td>{count:,}</td><td>{pct:.1f}%</td></tr>"

    # Metal Table Rows
    metal_rows = ""
    for m, count in dashboard_metrics["metal_dist"].items():
        pct = (
            (count / dashboard_metrics["total_members"] * 100)
            if dashboard_metrics["total_members"] > 0
            else 0
        )
        metal_rows += f"<tr><td>{m}</td><td>{count:,}</td><td>{pct:.1f}%</td></tr>"

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Population Dashboard: {desc}</title>
        <style>
            body {{ font-family: sans-serif; margin: 20px; }}
            h1 {{ color: #333; }}
            .metric-card {{ 
                display: inline-block; 
                border: 1px solid #ddd; 
                padding: 20px; 
                margin: 10px; 
                border-radius: 8px; 
                background-color: #f9f9f9;
                min-width: 150px;
                text-align: center;
            }}
            .metric-value {{ font-size: 24px; font-weight: bold; color: #007bff; }}
            .metric-label {{ color: #666; }}
            table {{ border-collapse: collapse; width: 100%; max-width: 600px; margin-top: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            .section {{ margin-top: 40px; }}
    <body>
        <h1>Population Dashboard</h1>
        <h3>Run: {desc} <span style="font-weight:normal; font-size:0.8em; color:#888">
            ({run_id})</span></h3>
        
        <div class="section">
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

        <div class="section">
            <h3>Gender Distribution</h3>
            <table>
                <tr><th>Gender</th><th>Count</th><th>%</th></tr>
                {gender_rows}
            </table>
        </div>

        <div class="section">
            <h3>Metal Level Distribution</h3>
            <table>
                <tr><th>Metal Level</th><th>Count</th><th>%</th></tr>
                {metal_rows}
            </table>
        </div>
    </body>
    </html>
    """

    output_path = VISUALIZATIONS_DIR / f"dashboard_{run_id}.html"
    with open(output_path, "w") as f:
        f.write(html_content)

    context.log.info(f"Dashboard saved to {output_path}")
