import os
import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL", "duckdb:///risk_adjustment.duckdb")
engine = create_engine(DATABASE_URL)

run_a = "e352b886-1d0e-4c45-bff6-6105ce0abec1"
run_b = "c9a8abd5-d7e8-422e-bae7-e298e8f2aec0"

print(f"Checking comparison between {run_a} and {run_b}")

with engine.connect() as conn:
    # Updated to check risk_scores first
    try:
        print(f"Checking data for run_a: {run_a}")
        count_a = conn.execute(text("SELECT count(*) FROM main_runs.risk_scores WHERE run_id = :r"), {"r": run_a}).scalar()
        print(f"Rows in risk_scores for run_a: {count_a}")

        print(f"Checking data for run_b: {run_b}")
        count_b = conn.execute(text("SELECT count(*) FROM main_runs.risk_scores WHERE run_id = :r"), {"r": run_b}).scalar()
        print(f"Rows in risk_scores for run_b: {count_b}")

        # Check in run_comparison (snapshot table) to find the batch_id
        stmt_find = text("""
            SELECT batch_id, created_at
            FROM main_analytics.run_comparison
            WHERE (run_id_a = :r1 AND run_id_b = :r2) 
               OR (run_id_a = :r2 AND run_id_b = :r1)
            ORDER BY created_at DESC
            LIMIT 1
        """)
        row = conn.execute(stmt_find, {"r1": run_a, "r2": run_b}).fetchone()
        
        if row:
            batch_id = row[0]
            print(f"Found comparison batch_id: {batch_id}, created at: {row[1]}")
            
            # Now check summary table
            stmt_summary = text("SELECT count(*) FROM main_analytics.run_comparison_by_dim WHERE batch_id = :bid")
            count = conn.execute(stmt_summary, {"bid": batch_id}).scalar()
            print(f"Total rows in run_comparison_by_dim for this batch: {count}")
        else:
            print("No comparison found in main_analytics.run_comparison for these runs.")

    except Exception as e:
        print(f"Error: {e}")
