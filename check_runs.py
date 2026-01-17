import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine("duckdb:///risk_adjustment.duckdb")

try:
    with engine.connect() as conn:
        print("\nRun Registry Content:")
        try:
            df = pd.read_sql("SELECT * FROM main_runs.run_registry ORDER BY run_timestamp DESC", conn)
            print(df)
        except Exception as e:
            print(f"Error reading run_registry: {e}")

        print("\nRisk Scores Count:")
        try:
            count = pd.read_sql("SELECT count(*) FROM main_runs.risk_scores", conn)
            print(count)
        except Exception as e:
            print(f"Error reading risk_scores: {e}")
except Exception as e:
    print(f"Global error: {e}")
