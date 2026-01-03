from __future__ import annotations

from pathlib import Path

import altair as alt
import pandas as pd
from dagster import asset

from ra_dagster.resources.duckdb_resource import DuckDBResource

VISUALIZATIONS_DIR = Path(__file__).resolve().parents[1] / "output" / "visualizations"
VISUALIZATIONS_DIR.mkdir(parents=True, exist_ok=True)


@asset(deps=["score_members_aca"])
def scoring_visualizations(context, duckdb: DuckDBResource) -> None:
    """
    Generate visualizations for recent scoring runs.
    """
    con = duckdb.get_connection().connect()

    # Example: Histogram of risk scores for the most recent run
    try:
        # Get most recent scoring run
        latest_run = con.execute("""
            SELECT run_id, run_description 
            FROM main_runs.run_registry 
            WHERE analysis_type = 'scoring' AND status = 'success'
            ORDER BY created_at DESC 
            LIMIT 1
        """).fetchone()

        if latest_run:
            run_id, description = latest_run
            context.log.info(f"Generating visualization for run: {run_id} ({description})")

            df = con.execute(f"""
                SELECT risk_score 
                FROM main_runs.risk_scores 
                WHERE run_id = '{run_id}'
            """).fetch_df()

            chart = (
                alt.Chart(df)
                .mark_bar()
                .encode(
                    alt.X("risk_score", bin=True, title="Risk Score"),
                    y="count()",
                    tooltip=["count()"],
                )
                .properties(title=f"Risk Score Distribution: {description}")
            )

            output_path = VISUALIZATIONS_DIR / f"scoring_{run_id}.html"
            chart.save(str(output_path))
            context.log.info(f"Chart saved to {output_path}")

    except Exception as e:
        context.log.error(f"Failed to generate scoring visualization: {e}")
    finally:
        con.close()


@asset(deps=["compare_runs"])
def comparison_visualizations(context, duckdb: DuckDBResource) -> None:
    """
    Generate visualizations for recent comparison runs.
    """
    con = duckdb.get_connection().connect()

    try:
        # Get most recent comparison run
        latest_run = con.execute("""
            SELECT run_id, run_description 
            FROM main_runs.run_registry 
            WHERE analysis_type = 'comparison' AND status = 'success'
            ORDER BY created_at DESC 
            LIMIT 1
        """).fetchone()

        if latest_run:
            batch_id, description = latest_run
            context.log.info(
                f"Generating visualization for comparison batch: {batch_id} ({description})"
            )

            # 1. Distribution of Deltas
            df_deltas = con.execute(f"""
                SELECT score_diff 
                FROM main_analytics.run_comparison 
                WHERE batch_id = '{batch_id}' 
                  AND match_status IN ('matched', 'both')
                  AND score_diff IS NOT NULL
            """).fetch_df()

            chart_deltas = (
                alt.Chart(df_deltas)
                .mark_bar()
                .encode(
                    alt.X("score_diff", bin=alt.Bin(maxbins=50), title="Score Difference"),
                    y="count()",
                    tooltip=["count()"],
                )
                .properties(title=f"Distribution of Score Deltas: {description}")
            )

            output_path_deltas = VISUALIZATIONS_DIR / f"comparison_deltas_{batch_id}.html"
            chart_deltas.save(str(output_path_deltas))
            context.log.info(f"Delta distribution chart saved to {output_path_deltas}")

            # 2. Mean Delta by Metal Level (requires joining back to risk_scores)
            # We need to find run_id_b to get the metal level
            run_id_b = con.execute(f"""
                SELECT any_value(run_id_b) 
                FROM main_analytics.run_comparison 
                WHERE batch_id = '{batch_id}'
            """).fetchone()[0]

            df_metal = con.execute(f"""
                WITH compare AS (
                  SELECT member_id, score_diff
                  FROM main_analytics.run_comparison
                  WHERE batch_id = '{batch_id}'
                    AND match_status IN ('matched','both')
                    AND score_diff IS NOT NULL
                ),
                risk_scores AS (
                  SELECT member_id, metal_level
                  FROM main_runs.risk_scores
                  WHERE run_id = '{run_id_b}'
                )
                SELECT
                  COALESCE(risk_scores.metal_level, 'UNKNOWN') AS metal_level,
                  AVG(compare.score_diff) AS mean_delta
                FROM compare
                LEFT JOIN risk_scores USING(member_id)
                GROUP BY 1
            """).fetch_df()

            chart_metal = (
                alt.Chart(df_metal)
                .mark_bar()
                .encode()
                .properties(title=f"Mean Delta by Metal Level: {description}")
            )

            output_path_metal = VISUALIZATIONS_DIR / f"comparison_metal_{batch_id}.html"
            chart_metal.save(str(output_path_metal))
            context.log.info(f"Metal level chart saved to {output_path_metal}")

    except Exception as e:
        context.log.error(f"Failed to generate comparison visualization: {e}")
    finally:
        con.close()


@asset(deps=["decompose_runs"])
def decomposition_visualizations(context, duckdb: DuckDBResource) -> None:
    """
    Generate visualizations for recent decomposition runs.
    """
    con = duckdb.get_connection().connect()

    try:
        # Get most recent decomposition run
        latest_run = con.execute("""
            SELECT run_id, run_description 
            FROM main_runs.run_registry 
            WHERE analysis_type = 'decomposition' AND status = 'success'
            ORDER BY created_at DESC 
            LIMIT 1
        """).fetchone()

        if latest_run:
            batch_id, description = latest_run
            context.log.info(
                f"Generating visualization for decomposition batch: {batch_id} ({description})"
            )

            # Waterfall / Bar chart of drivers
            df_drivers = con.execute(f"""
                SELECT driver_name, SUM(impact_value) as impact
                FROM main_analytics.decomposition_scenarios
                WHERE batch_id = '{batch_id}'
                GROUP BY driver_name
            """).fetch_df()

            chart_drivers = (
                alt.Chart(df_drivers)
                .mark_bar()
                .encode(
                    x=alt.X("driver_name", sort="-y"),
                    y="impact",
                    color=alt.condition(
                        alt.datum.impact > 0,
                        alt.value("steelblue"),  # The positive color
                        alt.value("orange"),  # The negative color
                    ),
                    tooltip=["driver_name", "impact"],
                )
                .properties(title=f"Decomposition Drivers: {description}")
            )

            output_path = VISUALIZATIONS_DIR / f"decomposition_drivers_{batch_id}.html"
            chart_drivers.save(str(output_path))
            context.log.info(f"Decomposition drivers chart saved to {output_path}")

    except Exception as e:
        context.log.error(f"Failed to generate decomposition visualization: {e}")
    finally:
        con.close()


@asset(deps=["score_members_aca"])
def lag_trend_visualizations(context, duckdb: DuckDBResource) -> None:
    """
    Generate year-over-year trend lines for lag analysis.
    """
    con = duckdb.get_connection().connect()

    try:
        # 1. Find all Lag Analysis runs
        runs = con.execute("""
            SELECT run_id, run_description 
            FROM main_runs.run_registry 
            WHERE run_description LIKE 'Lag Analysis %' 
              AND status = 'success'
        """).fetchall()

        if not runs:
            context.log.info("No lag analysis runs found.")
            return

        data = []
        for run_id, description in runs:
            # Parse Year and Month from description
            # Expected format: "Lag Analysis 2024 (3-month runout)"
            try:
                parts = description.split()
                year = parts[2]  # "2024"

                # Extract month number
                import re

                match = re.search(r"\((\d+)-month", description)
                if match:
                    month = int(match.group(1))
                else:
                    continue

                # Get average score
                avg_score = con.execute(f"""
                    SELECT AVG(risk_score) 
                    FROM main_runs.risk_scores 
                    WHERE run_id = '{run_id}'
                """).fetchone()[0]

                if avg_score is not None:
                    data.append(
                        {
                            "year": year,
                            "lag_month": month,
                            "avg_risk_score": avg_score,
                            "run_description": description,
                        }
                    )
            except Exception as parse_err:
                context.log.warning(f"Could not parse run description '{description}': {parse_err}")
                continue

        if not data:
            context.log.info("No valid data points extracted for lag trends.")
            return

        df = pd.DataFrame(data)

        # Create Multi-Line Chart
        chart = (
            alt.Chart(df)
            .mark_line(point=True)
            .encode(
                x=alt.X("lag_month:Q", title="Lag (Months)"),
                y=alt.Y(
                    "avg_risk_score:Q", title="Average Risk Score", scale=alt.Scale(zero=False)
                ),
                color=alt.Color("year:N", title="Year"),
                tooltip=["year", "lag_month", "avg_risk_score", "run_description"],
            )
            .properties(title="Year-over-Year Risk Score Maturation (Lag Analysis)")
        )

        output_path = VISUALIZATIONS_DIR / "lag_trend_analysis.html"
        chart.save(str(output_path))
        context.log.info(f"Lag trend chart saved to {output_path}")

    except Exception as e:
        context.log.error(f"Failed to generate lag trend visualization: {e}")
    finally:
        con.close()
