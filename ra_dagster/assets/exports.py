from pathlib import Path
import pandas as pd
from dagster import asset, ResourceParam, AssetKey
from sqlalchemy import text
from ra_dagster.resources.sqlalchemy_resource import SqlAlchemyResource

EXPORT_DIR = Path(__file__).resolve().parents[1] / "output" / "scoring"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

@asset(deps=["scoring_visualizations"])
def scoring_file_outputs(context, database: ResourceParam[SqlAlchemyResource]) -> None:
    """
    Exports scoring analytics views to CSV files for the most recent scoring run.
    """
    engine = database.get_engine()
    con = engine.connect()
    
    try:
        # Get latest success scoring run
        latest_run = con.execute(text("""
            SELECT run_id, run_ref 
            FROM main_runs.run_registry 
            WHERE analysis_type = 'scoring' AND status = 'success'
            ORDER BY created_at DESC 
            LIMIT 1
        """)).fetchone()
        
        if not latest_run:
            context.log.warning("No successful scoring run found to export.")
            return

        run_id, run_ref = latest_run
        
        # Fallback to run_id if run_ref is null, though run_ref is expected
        file_prefix = run_ref if run_ref else run_id
        
        context.log.info(f"Exporting CSVs for prefix: {file_prefix} (run_id: {run_id})")

        tables_to_export = [
            "run_score_summary",
            "run_score_distribution",
            "run_score_by_dim",
            "run_hcc_summary",
            "run_rxc_summary"
        ]

        for table in tables_to_export:
            # Query the analytics table for this run_id
            query = f"SELECT * FROM main_analytics.{table} WHERE run_id = :run_id"
            
            try:
                df = pd.read_sql(text(query), con, params={"run_id": run_id})
                
                if df.empty:
                    context.log.warning(f"No data found for {table} and run_id {run_id}")
                    continue

                filename = f"{file_prefix}_{table}.csv"
                output_path = EXPORT_DIR / filename
                df.to_csv(output_path, index=False)
                context.log.info(f"Exported {output_path}")
            except Exception as e:
                # Catch query errors (e.g. if table doesn't exist or doesn't have run_id)
                context.log.error(f"Failed to export {table}: {e}")

    finally:
        con.close()
