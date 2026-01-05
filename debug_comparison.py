import os
import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL", "duckdb:///risk_adjustment.duckdb")
engine = create_engine(DATABASE_URL)

run_a = "27ae1037-6601-46ca-96b7-fc5efd62cb36"
run_b = "9a4d7fd9-2b61-4fc4-8f37-8e65f5f3e1a2"

print(f"Checking comparison between {run_a} and {run_b}")

with engine.connect() as conn:
    # Check if table exists
    try:
        result = conn.execute(text("SELECT count(*) FROM main_analytics.run_comparison_by_dim")).scalar()
        print(f"Total rows in run_comparison_by_dim: {result}")
        
        # Check for specific comparison (order might matter depending on how it was run)
        stmt = text("""
            SELECT distinct run_id_a, run_id_b 
            FROM main_analytics.run_comparison_by_dim 
            WHERE (run_id_a = :r1 AND run_id_b = :r2) 
               OR (run_id_a = :r2 AND run_id_b = :r1)
        """)
        rows = conn.execute(stmt, {"r1": run_a, "r2": run_b}).fetchall()
        print(f"Found comparisons: {rows}")
        
    except Exception as e:
        print(f"Error: {e}")
