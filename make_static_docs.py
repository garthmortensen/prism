"""
Static dbt Documentation Generator

This script converts dbt's standard documentation (which requires separate JSON files)
into a single, self-contained HTML file with all data embedded inline.

Benefits:
- Single file deployment (no external JSON dependencies)
- Faster loading (no additional HTTP requests)
- Cleaner documentation (removes internal dbt projects)
- Better for static hosting (GitHub Pages, Netlify, etc.)
"""

import json
import re
import os

# Get current working directory (should be the dbt project root)
PATH_DBT_PROJECT = os.getcwd()

# This is the JavaScript pattern we're looking for in the dbt-generated HTML
# It loads manifest.json and catalog.json as external files
search_str = 'o=[i("manifest","manifest.json"+t),i("catalog","catalog.json"+t)]'

# Read the generated HTML file from dbt docs generate
with open(os.path.join(PATH_DBT_PROJECT, "target", "index.html"), "r") as f:
    content_index = f.read()

# Read the manifest JSON (contains all dbt project metadata)
with open(os.path.join(PATH_DBT_PROJECT, "target", "manifest.json"), "r") as f:
    json_manifest = json.loads(f.read())

# Clean up internal dbt projects from the documentation
# These are technical details that end users don't need to see
IGNORE_PROJECTS = [
    "dbt",
]  # Add more projects here if needed (e.g., 'dbt_bigquery', 'dbt_utils')

# Remove internal dbt projects from all relevant sections of the manifest
for element_type in ["nodes", "sources", "macros", "parent_map", "child_map"]:
    # Convert to list to avoid changing dict size during iteration
    # Use .get() with default {} to handle missing keys gracefully
    for key in list(json_manifest.get(element_type, {}).keys()):
        for ignore_project in IGNORE_PROJECTS:
            # Match keys that contain the ignored project name
            # Pattern: anything.dbt.anything (e.g., "macro.dbt.some_macro")
            if re.match(rf"^.*\.{ignore_project}\.", key):
                del json_manifest[element_type][key]

# Read the catalog JSON (contains column-level metadata, if available)
catalog_path = os.path.join(PATH_DBT_PROJECT, "target", "catalog.json")
if os.path.exists(catalog_path):
    with open(catalog_path, "r") as f:
        json_catalog = json.loads(f.read())
    print("Using existing catalog.json")
else:
    print("catalog.json not found, creating empty catalog (no database connection)")
    json_catalog = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/catalog/v1.json",
            "generated_at": "2024-01-01T00:00:00.000000Z",
        },
        "nodes": {},
        "sources": {},
    }

# Write the modified HTML file with embedded JSON data
with open(os.path.join(PATH_DBT_PROJECT, "target", "index.html"), "w") as f:
    # Replace the external file loading with inline data
    # This embeds the manifest and catalog data directly in the HTML
    new_str = (
        "o=[{label: 'manifest', data: "
        + json.dumps(json_manifest)
        + "},{label: 'catalog', data: "
        + json.dumps(json_catalog)
        + "}]"
    )
    new_content = content_index.replace(search_str, new_str)

    if search_str not in content_index:
        print(
            f"WARNING: Search string '{search_str}' not found in index.html. Static embedding might have failed."
        )
        # Try alternative search string for newer dbt versions if needed
        # search_str_v2 = '...'

    f.write(new_content)

print("Static dbt documentation generated successfully!")
print(f"Output: {os.path.join(PATH_DBT_PROJECT, 'target', 'index.html')}")
print("Ready for deployment to static hosting!")
