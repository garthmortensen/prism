import os
from sqlalchemy import create_engine, inspect, text

DATABASE_URL = os.getenv("DATABASE_URL", "duckdb:///risk_adjustment.duckdb")
print(f"Connecting to: {DATABASE_URL}")
engine = create_engine(DATABASE_URL)

try:
    with engine.connect() as conn:
        print("Connection successful.")
        inspector = inspect(conn)
        schemas = inspector.get_schema_names()
        print(f"Schemas found: {schemas}")
        
        for schema in schemas:
            if schema in ['main_analytics', 'risk_adjustment', 'main', 'public']:
                try:
                    tables = inspector.get_table_names(schema=schema)
                    print(f"Tables in {schema}: {tables}")
                    if "run_score_summary" in tables:
                        print(f"FOUND run_score_summary in {schema}")
                except Exception as e:
                    print(f"Could not list tables in {schema}: {e}")

except Exception as e:
    print(f"Error: {e}")
