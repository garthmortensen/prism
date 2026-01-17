import sys
import traceback

try:
    print("Attempting to import ra_dagster.definitions...")
    import ra_dagster.definitions
    print("Successfully imported ra_dagster.definitions")
except Exception:
    print("Failed to import ra_dagster.definitions")
    traceback.print_exc()
