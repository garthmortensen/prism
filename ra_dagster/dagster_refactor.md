# Refactoring ACA Risk Scoring for Dagster

This document outlines the architectural changes made to the ACA Risk Scoring engine to transition from a monolithic script to a Dagster Asset-based pipeline. It also provides a guide on how to leverage Dagster's features with this new design.

## 1. Key Definitions (Glossary)

*   **Asset**: A piece of data (like a database table, file, or report) that you want to exist. You define *how* to compute it, and Dagster ensures it stays up-to-date. In this project, each step of the calculation (e.g., specific risk scores) is an asset.
*   **DAG (Directed Acyclic Graph)**: A visual map showing how data flows. If Asset B requires Asset A, there is a line from A (upstream) to B (downstream). It's "acyclic" because data flows forward and never loops back to the start.
*   **Materialize**: The act of running the code to create or update an asset. When you click "Materialize" in the UI, you are telling Dagster to execute the Python function and save the result.
*   **Job**: A set of assets or tasks grouped together to be executed. For example, `aca_scoring_detailed_job` is a job that runs all the scoring assets in the correct order.

## 2. Architecture Overview

### Before (Monolithic)
*   **Structure:** A single asset (`score_members_aca`) treated the `ACACalculator` class as a black box.
*   **Execution:** Loaded data, calculated features, and scored risks in one opaque Python function call.
*   **Downsides:**
    *   No visibility into intermediate steps (e.g., "Did the HCC mapping fail or the coefficient summing?").
    *   No caching (CMS tables re-loaded on every run).
    *   Hard to debug specific logic errors without print statements.

### After (Asset-Based)
The logic has been decomposed into a **Directed Acyclic Graph (DAG)** of 17 granular assets.

#### Locating in Deployment
In the Dagster UI (measured on v1.12.7), you can find these assets by navigating to the **Lineage** section in the left-hand sidebar (or at `/asset-groups`) and looking for:

#### A. Reference Data Assets (Group: `aca_reference_data`)
*Defined in: `ra_dagster/assets/aca_reference_data.py`*

These assets load official CMS tables. They are designed to be computed **once per model year**.
* `aca_icd_to_cc_mapping`
* `aca_hcc_hierarchies`
* `aca_coefficients`
* `aca_model_exclusions`
* `aca_hcc_groups_adult`
* `aca_hcc_groups_child`
* `aca_ndc_to_rxc_mapping`
* `aca_rxc_hierarchies`
* `aca_hcc_labels`
* `aca_rxc_labels`

#### B. Scoring Pipeline Assets (Group: `aca_scoring_detailed`)
*Defined in: `ra_dagster/assets/aca_scoring_detailed.py`*

These assets represent increasing levels of data enrichment.
1.  **`aca_member_input`**: Raw member data from DB.
2.  **`aca_demographics`**: Age/Sex/Metal level factors.
3.  **`aca_raw_ccs`**: Diagnosis codes mapped to condition categories.
4.  **`aca_hccs_post_hierarchy`**: Hierarchy logic applied.
5.  **`aca_hccs_final`**: Interactions & Groupings applied.
6.  **`aca_scores`**: Final risk score calculation.

#### C. Logic Layer (`ra_calculators/aca_risk_score_calculator/scoring_steps.py`)
The `ACACalculator` class methods were extracted into **pure functions**. These functions are stateless and easy to unit test, removing dependency on the class instance state.

---

## 3. Benefits & How to Use Them

### A. Observability (The UI Graph)
**Goal:** Understand logic flow and data dependencies.
1.  Navigate to **Lineage** in the left sidebar.
2.  Select the `aca_scoring_detailed` group.
    *   *Benefit:* You can visually verify if `aca_scores` depends on the correct hierarchy tables.

### B. Intermediate Inspection (Metadata)
**Goal:** Debug logic without print statements.
1.  Run the `aca_scoring_detailed_job`.
2.  Click on an intermediate asset node (e.g., `aca_raw_ccs`).
3.  Look at the **Metadata** panel on the right.
    *   *Benefit:* You will see `unique_diagnosis_codes` or `mapped_cc_count`. If a member has 0 HCCs, you can check `aca_raw_ccs` to see if the mapping happened correctly *before* checking the hierarchies.

### C. Performance & Caching
**Goal:** Iterate faster during development.
1.  Materialize the **ACA Reference Data** group once.
2.  Run the scoring job.
    *   *Benefit:* The scoring job will *read* the persisted reference data (pickled objects) instead of re-parsing the DIY tables. This drastically reduces startup time for runs.

### D. Root Cause Analysis
**Goal:** Identify bottlenecks.
1.  Go to **Runs** and select a completed run.
2.  Click the **Timeline** tab.
    *   *Benefit:* See exactly which step (e.g., RXC mapping vs. Demographics) takes the most time.

---

## 4. Developer Guide

### Running the Pipeline
```bash
# Run via CLI
dagster job execute -j aca_scoring_detailed_job

# Or Launch via UI
# Go to Overview -> Jobs -> aca_scoring_detailed_job -> Launch Run
```

### Adding New Logic
If CMS changes the logic (e.g., a new interaction term):
1.  Modify the pure function in `scoring_steps.py`.
2.  The change automatically reflects in the `aca_hccs_final` asset.
3.  Reload Dagster; the graph remains the same, but the transformation logic updates.

### Configuration
The new job `aca_scoring_detailed_job` uses a consolidated configuration structure in `definitions.py`. It shares the same config schema (database views, year) across the granular assets.
