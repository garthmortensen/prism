# lan: Refactor ACA Calculator for Dagster Asset-Based Design
The current setup uses a single monolithic asset (score_members_aca) that invokes the ACACalculator class, treating it as a black box. To make the DAG more meaningful and leverage Dagster features (observability, incremental materialization, debugging), the internal steps of the calculator should be exposed as individual assets. Reference data will be materialized once per model year and cached.

Steps
Create assets for reference data loading in a new file ra_dagster/assets/aca_reference_data.py. Define @asset functions for each table loaded by table_loader.py (e.g., icd_to_cc_mapping, coefficients, hcc_hierarchies, hcc_groups, ndc_to_rxc_mapping, rxc_hierarchies). Use FreshnessPolicy or AutoMaterializePolicy so these only re-run when model year changes.

Create an asset for raw member input in ra_dagster/assets/aca_scoring.py. Split the data-fetching logic from score_members_aca into a dedicated aca_member_input asset that reads from the database and outputs a DataFrame of MemberInput-like records.

Create intermediate transformation assets in ra_dagster/assets/aca_scoring.py. Break down the scoring logic into assets representing each key stage:

aca_demographics: Calculates age, model type, and demographic variable/coefficient for each member.
aca_raw_ccs: Maps diagnoses to raw Condition Categories using icd_to_cc_mapping.
aca_hccs_post_hierarchy: Applies hierarchies to raw CCs using hcc_hierarchies.
aca_hccs_final: Applies model exclusions and groupings using hcc_groups.
aca_rxcs: Maps NDCs to RXCs and applies RXC hierarchies using ndc_to_rxc_mapping and rxc_hierarchies.
aca_scores: Sums coefficients for the final risk score output.
Refactor ACACalculator into pure functions in a new file ra_calculators/aca_risk_score_calculator/scoring_steps.py. Extract the class methods (e.g., _map_diagnoses_to_ccs, _apply_groupings, _apply_rxc_hierarchies) into standalone, testable functions. The ACACalculator class can remain as a convenience wrapper that calls these functions.

Update definitions.py to register the new assets from aca_reference_data.py and aca_scoring.py, and define a new job (e.g., aca_scoring_detailed_job) that materializes the full chain.

Update __init__.py to export the new pure functions from scoring_steps.py for direct use by Dagster assets.
