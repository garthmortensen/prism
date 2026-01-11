import os
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

# Uses SQLAlchemy so we can point to DuckDB (dev) or Snowflake (prod)
DATABASE_URL = os.getenv("DATABASE_URL", "duckdb:///risk_adjustment.duckdb")

# Use NullPool to ensure connections are closed immediately, releasing locks
# This is critical for DuckDB when triggering external processes (like Dagster jobs)
engine = create_engine(DATABASE_URL, poolclass=NullPool)

def resolve_run_id(run_id_or_ref: str) -> str:
    """Helper to resolve a run_ref to run_id if needed."""
    from sqlalchemy import text
    
    # Heuristic: UUIDs are 36 chars long. run_refs are usually shorter and readable.
    if len(run_id_or_ref) == 36 and "-" in run_id_or_ref:
        return run_id_or_ref

    with engine.connect() as conn:
        # Use text query for simplicity and to avoid reflecting the whole table
        result = conn.execute(
            text("SELECT run_id FROM main_runs.run_registry WHERE run_ref = :ref"), 
            {"ref": run_id_or_ref}
        ).scalar()
        
    if result:
        return result
    
    # If not found, assume it might be a run_id (or will fail later)
    return run_id_or_ref
