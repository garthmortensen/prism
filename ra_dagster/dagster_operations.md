# Dagster Operations & Concepts

## 1. Configuration: Launchpad vs. Blueprint

When viewing run details in the `run_registry`, you will see two distinct configuration fields. They serve different purposes:

### `launchpad_config` (The "How")
*   **Purpose:** The **Dagster orchestration configuration**. Contains the exact JSON payload required to re-launch the job in the Dagster UI.
*   **Structure:** Deeply nested, following Dagster's schema requirements (`ops` → `op_name` → `config` and `resources`).
*   **Use Case:** Copy-paste this into the Dagster Launchpad to "Re-run with same config."

### `blueprint_yml` (The "What")
*   **Purpose:** The **logical configuration** (or "Business Logic" parameters). It is a dump of the Pydantic config object (e.g., `ScoringConfig`).
*   **Structure:** A **flat dictionary** containing only the relevant parameters for the calculation (e.g., `diy_model_year`, `claims_view`), stripped of Dagster-specific nesting.
*   **Use Case:** For analysts and auditors to see exactly what parameters were fed into the algorithm. Often includes runtime metadata (like `run_id_actual`) injected during execution.

---

## 2. Reloading Definitions

**"Reload Definitions"** forces the Dagster daemon and web server (UI) to re-import your Python code and rebuild the internal graph of jobs, assets, and schedules.

### What it does
1.  **Re-executes the global scope** of Python files (imports, function definitions, decorators).
2.  **Re-parses configuration schemas** (Pydantic models, Config objects).
3.  **Updates the Launchpad** with new fields or defaults.
4.  **Refreshes resources** initialized at definition time.

### When to Reload
You **MUST** reload (or restart `dagster dev`) when you change:
*   **Pydantic Models / Config Schemas:** Adding/removing fields (e.g., `run_ref`, `batch_ref`).
*   **Asset/Op Signatures:** Changing inputs (`AssetIn`) or outputs.
*   **Top-level YAMLs:** If your code reads a YAML file at the module level to generate assets.
*   **Job/Schedule Definitions:** Renaming jobs, changing cron schedules.

### When it's (usually) not needed
*   **Logic inside a function:** If you only change the math/logic *inside* a `def my_asset(...)` body, the Executor will pick up the new code on the next run without a UI reload.

---

## 3. Deployment

**Deployment** refers to the **environment** and **infrastructure** where your Dagster definitions are loaded, managed, and executed.

### Concepts
*   **Code Locations:** "Pointers" to your Python code (e.g., your local project running via `dagster dev`). The Deployment manages these locations.
*   **Definitions:** The actual objects (`@job`, `@asset`) inside your code.
*   **Deployment Tab:** The control center in the UI that shows the health of your Code Locations (e.g., successful load vs. `ModuleNotFoundError`).

### Why "Reload" is inside Deployment
When you click **Reload Definitions** in the Deployment tab, you are telling the deployment manager:
> *"The code in my Python files has changed. Please update the 'deployed' version of my graph to match my local files."*
