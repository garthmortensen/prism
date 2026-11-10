# Dagster Jobs Documentation

This document outlines the operational jobs within the Prism `ra_dagster` project. Each job is a collection of "assets" (steps) that execute in dependency order to achieve a specific analytical outcome.

---

## 1. Scoring Job (`scoring_job`)
**Purpose**: The core pipeline for calculating ACA risk scores for a population. It transforms raw data into risk scores and actionable analytics.

| Step (Asset) | Description | Why is this needed? |
| :--- | :--- | :--- |
| **1. dbt_analytics_assets** | Executes `dbt build` to refresh the data warehouse. | Ensures the `dbt_raw` (seeds/source) tables are correctly transformed into `intermediate` views (e.g., `int_aca_risk_input`) that the scoring engine reads from. It establishes the "Ground Truth" data state before scoring begins. |
| **2. score_members_aca** | Runs the Python-based HHS-HCC risk adjustment calculator. | Validates member inputs, applies the complex federal risk scoring logic (hierarchies, interactions, coefficients), and writes the granular results to `dag_runs.risk_scores` in the database. |
| **3. scoring_visualizations** | Generates static HTML charts (histograms, distributions) from the scoring results. | Provides immediate visual feedback on the quality of the run (e.g., "Do the risk scores follow a normal distribution?") without needing to query the database manually. |

---

### Why is `dbt_analytics_assets` managed here?

You might wonder: *"Why is dbt included in the Python scoring job? Doesn't dbt run on its own?"*

1.  **Orchestration, not Automagic**: `dbt` is a command-line tool. It only runs when you tell it to (e.g., typing `dbt build` in your terminal). In a production system, **Dagster** acts as the "Traffic Controller" causing dbt to run at the right time.
2.  **Order of Operations**: The Python scoring Step (2) reads from the `intermediate.int_aca_risk_input` table. This table is creating/refreshed by `dbt` in Step (1).
    *   **If dbt runs first**: The Python calculator reads fresh, clean data.
    *   **If dbt doesn't run**: The Python calculator reads *yesterday's* data (or fails if the table is empty).
3.  **One "Button" for Everything**: By wrapping dbt inside the Dagster job, you press one "Launch" button to refresh the data *and* score it. You don't need to manually run `dbt build` in a terminal and then switch to the UI.

---

## 2. Comparison Job (`comparison_job`)
**Purpose**: Analyzes the differences between two distinct scoring runs (e.g., "Current Month" vs "Prior Month", or "Production Model" vs "Proposed Model").

| Step (Asset) | Description | Why is this needed? |
| :--- | :--- | :--- |
| **1. compare_runs** | Loads two sets of scores from `dag_runs.risk_scores` and aligns them by Member ID. | Calculates the exact delta (`Score B - Score A`) for every member, identifying who changed and by how much. |
| **2. comparison_visualizations** | Creates delta distribution charts. | Visualizes the "shape" of the change. (e.g., Did the entire population shift up? Or did only a specific segment Change?) |

---

## 3. Decomposition Job (`decomposition_job`)
**Purpose**: Explains *why* risk scores changed between two runs (Attribution Analysis).

| Step (Asset) | Description | Why is this needed? |
| :--- | :--- | :--- |
| **1. decompose_runs** | Runs a multi-step attribution algorithm (Marginal or Shapley-like). | Breaks down a total score change into buckets like "Demographic Changes", "Disease Prevalence", "Model Version Change", or "Regulatory Updates". |
| **2. decomposition_visualizations** | Generates waterfall charts of the drivers. | Executive summary view: "Risk rose 2.5%, composed of +1.0% from aging population and +1.5% from new coding guidelines." |

---

## 4. Population Dashboard Job (`dashboard_job`)
**Purpose**: Generates a high-level population health report for a single run.

| Step (Asset) | Description | Why is this needed? |
| :--- | :--- | :--- |
| **1. dashboard_metrics** | Aggregates key KPIs (Avg Risk Score, Member Count, Metal Level mix). | Pre-calculates summary statistics for rapid reporting. |
| **2. dashboard_html** | Renders a styled HTML report. | Provides a shareable artifact for business stakeholders (Non-technical view). |

---

## 5. Comparison Dashboard Job (`comparison_dashboard_job`)
**Purpose**: Generates a high-level impact report comparing two runs (A/B Testing view).

| Step (Asset) | Description | Why is this needed? |
| :--- | :--- | :--- |
| **1. comparison_dashboard_metrics** | Aggregates retention rates, churn, and average score deltas. | Summarizes the stability of the population between the two timeframes. |
| **2. comparison_dashboard_html** | Renders a styled HTML comparison report. | Final sign-off artifact for model updates or monthly closes. |
