import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, select

# Replicate logic from ra_agent/db.py
DATABASE_URL = os.getenv("DATABASE_URL", "duckdb:///risk_adjustment.duckdb")
print(f"Connecting to: {DATABASE_URL}")
engine = create_engine(DATABASE_URL)

def test_list_runs():
    print("\n--- Testing list_runs ---")
    md = MetaData()
    run_registry = Table(
        "run_registry", 
        md, 
        Column("run_id", String),
        Column("run_timestamp", String),
        Column("status", String),
        Column("run_description", String),
        Column("analysis_type", String),
        schema="main_runs"
    )

    stmt = (
        select(run_registry.c.run_id, run_registry.c.run_timestamp, run_registry.c.status, run_registry.c.run_description)
        .where(run_registry.c.analysis_type == "scoring")
        .order_by(run_registry.c.run_timestamp.desc())
        .limit(1)
    )

    try:
        with engine.connect() as conn:
            df = pd.read_sql(stmt, conn)
            print("Success!")
            print(df)
            if not df.empty:
                return df.iloc[0]['run_id']
    except Exception as e:
        print(f"Error: {e}")
    return None

def test_get_run_summary(run_id):
    print(f"\n--- Testing get_run_summary for {run_id} ---")
    md = MetaData()
    summary_table = Table(
        "run_score_summary", 
        md, 
        Column("run_id", String),
        Column("member_count", Integer),
        Column("avg_score", Float),
        schema="main_analytics"
    )

    stmt = select(summary_table).where(summary_table.c.run_id == run_id)

    try:
        with engine.connect() as conn:
            df = pd.read_sql(stmt, conn)
            print("Success!")
            print(df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    run_id = test_list_runs()
    if run_id:
        test_get_run_summary(run_id)
