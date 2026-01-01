# Prism warehouse ERD

This is an auto-generated ERD of the DuckDB warehouse (`risk_adjustment.duckdb`).

Regenerate:

```bash
/home/garth/garage/prism/.venv/bin/python tools/generate_warehouse_erd.py
```

Notes:

- Only primary keys are declared in DuckDB here; foreign keys are inferred from common key columns.
- Table names in diagrams are prefixed by schema alias: raw/staging/intermediate/runs/analytics/raw_year.

## Core relationships

```mermaid
erDiagram
  runs__run_registry {
    VARCHAR run_id PK
    VARCHAR run_timestamp
    BIGINT group_id
    VARCHAR run_description
    VARCHAR analysis_type
    VARCHAR status
    VARCHAR trigger_source
    VARCHAR blueprint_id
    TIMESTAMP created_at
    TIMESTAMP updated_at
  }
  runs__risk_scores {
    VARCHAR run_id PK
    VARCHAR member_id PK
    DOUBLE risk_score
    DOUBLE hcc_score
    DOUBLE rxc_score
    DOUBLE demographic_score
    VARCHAR gender
    VARCHAR metal_level
    INTEGER enrollment_months
    VARCHAR run_timestamp
    TIMESTAMP created_at
  }
  analytics__run_comparison {
    VARCHAR batch_id PK
    VARCHAR run_id_a
    VARCHAR run_id_b
    VARCHAR member_id PK
    VARCHAR match_status
    DOUBLE score_a
    DOUBLE score_b
    DOUBLE score_diff
    TIMESTAMP created_at
  }
  analytics__decomposition_definitions {
    VARCHAR batch_id PK
    INTEGER step_index PK
    VARCHAR driver_name
    TIMESTAMP created_at
  }
  analytics__decomposition_scenarios {
    VARCHAR batch_id PK
    VARCHAR driver_name PK
    DOUBLE impact_value
    VARCHAR run_id
    TIMESTAMP created_at
  }
  raw__raw_members {
    VARCHAR member_id
    DATE dob
    VARCHAR gender
    VARCHAR state
    VARCHAR zip
    DECIMAL fpl_ratio
    VARCHAR region
    INTEGER year
  }
  raw__raw_enrollments {
    VARCHAR enrollment_id
    VARCHAR member_id
    VARCHAR plan_id
    DATE start_date
    DATE end_date
    DECIMAL premium_paid
    VARCHAR csr_variant
  }
  raw__raw_claims {
    VARCHAR claim_id
    VARCHAR member_id
    VARCHAR provider_id
    VARCHAR plan_id
    DATE service_date
    DECIMAL claim_amount
    DECIMAL allowed_amount
    DECIMAL paid_amount
    VARCHAR diagnosis_code
    VARCHAR procedure_code
    VARCHAR claim_type
    VARCHAR major_service_category
  }
  raw__raw_plans {
    VARCHAR plan_id
    VARCHAR metal_tier
    DECIMAL monthly_premium
    INTEGER deductible
    INTEGER oop_max
    INTEGER effective_year
  }
  raw__raw_providers {
    VARCHAR provider_id
    VARCHAR npi
    VARCHAR specialty
    VARCHAR state
  }
  raw__raw_members ||--o{ raw__raw_claims : "member (member_id->member_id)"
  raw__raw_members ||--o{ raw__raw_enrollments : "member (member_id->member_id)"
  raw__raw_plans ||--o{ raw__raw_claims : "plan (plan_id->plan_id)"
  raw__raw_plans ||--o{ raw__raw_enrollments : "plan (plan_id->plan_id)"
  raw__raw_providers ||--o{ raw__raw_claims : "provider (provider_id->provider_id)"
  runs__run_registry ||--o{ runs__risk_scores : "scores (run_id->run_id)"
  runs__run_registry ||--o{ analytics__run_comparison : "compare batch (run_id->batch_id)"
  runs__run_registry ||--o{ analytics__run_comparison : "run A (run_id->run_id_a)"
  runs__run_registry ||--o{ analytics__run_comparison : "run B (run_id->run_id_b)"
  runs__run_registry ||--o{ analytics__decomposition_definitions : "decomp batch (run_id->batch_id)"
  runs__run_registry ||--o{ analytics__decomposition_scenarios : "decomp batch (run_id->batch_id)"
  runs__run_registry ||--o{ analytics__decomposition_scenarios : "scenario run (run_id->run_id)"
  raw__raw_members ||--o{ runs__risk_scores : "member (member_id->member_id)"
```

## Schema: analytics (main_analytics)

```mermaid
erDiagram
  analytics__decomposition_definitions {
    VARCHAR batch_id PK
    INTEGER step_index PK
    VARCHAR driver_name
    TIMESTAMP created_at
  }
  analytics__decomposition_scenarios {
    VARCHAR batch_id PK
    VARCHAR driver_name PK
    DOUBLE impact_value
    VARCHAR run_id
    TIMESTAMP created_at
  }
  analytics__run_comparison {
    VARCHAR batch_id PK
    VARCHAR run_id_a
    VARCHAR run_id_b
    VARCHAR member_id PK
    VARCHAR match_status
    DOUBLE score_a
    DOUBLE score_b
    DOUBLE score_diff
    TIMESTAMP created_at
  }
```

## Schema: intermediate (main_intermediate)

```mermaid
erDiagram
  intermediate__int_aca_risk_input {
    VARCHAR member_id
    BIGINT enrollment_months
    VARCHAR gender
    VARCHAR metal_level
    DATE date_of_birth
  }
  intermediate__int_member_diagnoses {
    VARCHAR member_id
  }
  intermediate__int_member_months {
    VARCHAR member_id
    BIGINT enrollment_months
    VARCHAR gender
    VARCHAR metal_level
    DATE date_of_birth
  }
  intermediate__int_member_rx {
    VARCHAR member_id
  }
  intermediate__int_risk_score_components {
    VARCHAR member_id
    VARCHAR run_timestamp
    DOUBLE risk_score
    VARCHAR metal_level
    TIMESTAMP_TZ created_at
  }
```

## Schema: raw_year (main_main_raw)

```mermaid
erDiagram
  raw_year__raw_claims_2021 {
    VARCHAR claim_id
    VARCHAR member_id
    VARCHAR provider_id
    VARCHAR plan_id
    DATE service_date
    DECIMAL claim_amount
    DECIMAL allowed_amount
    DECIMAL paid_amount
    VARCHAR diagnosis_code
    VARCHAR procedure_code
    VARCHAR claim_type
    VARCHAR major_service_category
  }
  raw_year__raw_claims_2022 {
    VARCHAR claim_id
    VARCHAR member_id
    VARCHAR provider_id
    VARCHAR plan_id
    DATE service_date
    DECIMAL claim_amount
    DECIMAL allowed_amount
    DECIMAL paid_amount
    VARCHAR diagnosis_code
    VARCHAR procedure_code
    VARCHAR claim_type
    VARCHAR major_service_category
  }
  raw_year__raw_claims_2023 {
    VARCHAR claim_id
    VARCHAR member_id
    VARCHAR provider_id
    VARCHAR plan_id
    DATE service_date
    DECIMAL claim_amount
    DECIMAL allowed_amount
    DECIMAL paid_amount
    VARCHAR diagnosis_code
    VARCHAR procedure_code
    VARCHAR claim_type
    VARCHAR major_service_category
  }
  raw_year__raw_claims_2024 {
    VARCHAR claim_id
    VARCHAR member_id
    VARCHAR provider_id
    VARCHAR plan_id
    DATE service_date
    DECIMAL claim_amount
    DECIMAL allowed_amount
    DECIMAL paid_amount
    VARCHAR diagnosis_code
    VARCHAR procedure_code
    VARCHAR claim_type
    VARCHAR major_service_category
  }
  raw_year__raw_claims_2025 {
    VARCHAR claim_id
    VARCHAR member_id
    VARCHAR provider_id
    VARCHAR plan_id
    DATE service_date
    DECIMAL claim_amount
    DECIMAL allowed_amount
    DECIMAL paid_amount
    VARCHAR diagnosis_code
    VARCHAR procedure_code
    VARCHAR claim_type
    VARCHAR major_service_category
  }
  raw_year__raw_enrollments_2021 {
    VARCHAR enrollment_id
    VARCHAR member_id
    VARCHAR plan_id
    DATE start_date
    DATE end_date
    DECIMAL premium_paid
    VARCHAR csr_variant
  }
  raw_year__raw_enrollments_2022 {
    VARCHAR enrollment_id
    VARCHAR member_id
    VARCHAR plan_id
    DATE start_date
    DATE end_date
    DECIMAL premium_paid
    VARCHAR csr_variant
  }
  raw_year__raw_enrollments_2023 {
    VARCHAR enrollment_id
    VARCHAR member_id
    VARCHAR plan_id
    DATE start_date
    DATE end_date
    DECIMAL premium_paid
    VARCHAR csr_variant
  }
  raw_year__raw_enrollments_2024 {
    VARCHAR enrollment_id
    VARCHAR member_id
    VARCHAR plan_id
    DATE start_date
    DATE end_date
    DECIMAL premium_paid
    VARCHAR csr_variant
  }
  raw_year__raw_enrollments_2025 {
    VARCHAR enrollment_id
    VARCHAR member_id
    VARCHAR plan_id
    DATE start_date
    DATE end_date
    DECIMAL premium_paid
    VARCHAR csr_variant
  }
  raw_year__raw_members_2021 {
    VARCHAR member_id
    DATE dob
    VARCHAR gender
    VARCHAR state
    VARCHAR zip
    DECIMAL fpl_ratio
    VARCHAR region
    INTEGER year
  }
  raw_year__raw_members_2022 {
    VARCHAR member_id
    DATE dob
    VARCHAR gender
    VARCHAR state
    VARCHAR zip
    DECIMAL fpl_ratio
    VARCHAR region
    INTEGER year
  }
  raw_year__raw_members_2023 {
    VARCHAR member_id
    DATE dob
    VARCHAR gender
    VARCHAR state
    VARCHAR zip
    DECIMAL fpl_ratio
    VARCHAR region
    INTEGER year
  }
  raw_year__raw_members_2024 {
    VARCHAR member_id
    DATE dob
    VARCHAR gender
    VARCHAR state
    VARCHAR zip
    DECIMAL fpl_ratio
    VARCHAR region
    INTEGER year
  }
  raw_year__raw_members_2025 {
    VARCHAR member_id
    DATE dob
    VARCHAR gender
    VARCHAR state
    VARCHAR zip
    DECIMAL fpl_ratio
    VARCHAR region
    INTEGER year
  }
  raw_year__raw_members_2021 ||--o{ raw_year__raw_claims_2021 : "member 2021 (member_id->member_id)"
  raw_year__raw_members_2021 ||--o{ raw_year__raw_enrollments_2021 : "member 2021 (member_id->member_id)"
  raw_year__raw_members_2022 ||--o{ raw_year__raw_claims_2022 : "member 2022 (member_id->member_id)"
  raw_year__raw_members_2022 ||--o{ raw_year__raw_enrollments_2022 : "member 2022 (member_id->member_id)"
  raw_year__raw_members_2023 ||--o{ raw_year__raw_claims_2023 : "member 2023 (member_id->member_id)"
  raw_year__raw_members_2023 ||--o{ raw_year__raw_enrollments_2023 : "member 2023 (member_id->member_id)"
  raw_year__raw_members_2024 ||--o{ raw_year__raw_claims_2024 : "member 2024 (member_id->member_id)"
  raw_year__raw_members_2024 ||--o{ raw_year__raw_enrollments_2024 : "member 2024 (member_id->member_id)"
  raw_year__raw_members_2025 ||--o{ raw_year__raw_claims_2025 : "member 2025 (member_id->member_id)"
  raw_year__raw_members_2025 ||--o{ raw_year__raw_enrollments_2025 : "member 2025 (member_id->member_id)"
```

## Schema: raw (main_raw)

```mermaid
erDiagram
  raw__raw_claims {
    VARCHAR claim_id
    VARCHAR member_id
    VARCHAR provider_id
    VARCHAR plan_id
    DATE service_date
    DECIMAL claim_amount
    DECIMAL allowed_amount
    DECIMAL paid_amount
    VARCHAR diagnosis_code
    VARCHAR procedure_code
    VARCHAR claim_type
    VARCHAR major_service_category
  }
  raw__raw_enrollments {
    VARCHAR enrollment_id
    VARCHAR member_id
    VARCHAR plan_id
    DATE start_date
    DATE end_date
    DECIMAL premium_paid
    VARCHAR csr_variant
  }
  raw__raw_members {
    VARCHAR member_id
    DATE dob
    VARCHAR gender
    VARCHAR state
    VARCHAR zip
    DECIMAL fpl_ratio
    VARCHAR region
    INTEGER year
  }
  raw__raw_plans {
    VARCHAR plan_id
    VARCHAR metal_tier
    DECIMAL monthly_premium
    INTEGER deductible
    INTEGER oop_max
    INTEGER effective_year
  }
  raw__raw_providers {
    VARCHAR provider_id
    VARCHAR npi
    VARCHAR specialty
    VARCHAR state
  }
  raw__raw_members ||--o{ raw__raw_claims : "member (member_id->member_id)"
  raw__raw_members ||--o{ raw__raw_enrollments : "member (member_id->member_id)"
  raw__raw_plans ||--o{ raw__raw_claims : "plan (plan_id->plan_id)"
  raw__raw_plans ||--o{ raw__raw_enrollments : "plan (plan_id->plan_id)"
  raw__raw_providers ||--o{ raw__raw_claims : "provider (provider_id->provider_id)"
```

## Schema: runs (main_runs)

```mermaid
erDiagram
  runs__risk_scores {
    VARCHAR run_id PK
    VARCHAR member_id PK
    DOUBLE risk_score
    DOUBLE hcc_score
    DOUBLE rxc_score
    DOUBLE demographic_score
    VARCHAR gender
    VARCHAR metal_level
    INTEGER enrollment_months
    VARCHAR run_timestamp
    TIMESTAMP created_at
  }
  runs__run_registry {
    VARCHAR run_id PK
    VARCHAR run_timestamp
    BIGINT group_id
    VARCHAR run_description
    VARCHAR analysis_type
    VARCHAR status
    VARCHAR trigger_source
    VARCHAR blueprint_id
    TIMESTAMP created_at
    TIMESTAMP updated_at
  }
  runs__run_registry ||--o{ runs__risk_scores : "scores (run_id->run_id)"
```

## Schema: staging (main_staging)

```mermaid
erDiagram
  staging__stg_claims_dx {
    VARCHAR claim_id
    VARCHAR member_id
    DATE service_date
    VARCHAR diagnosis_code
  }
  staging__stg_claims_rx {
    VARCHAR claim_id
    VARCHAR member_id
    DATE fill_date
    VARCHAR ndc_code
  }
  staging__stg_enrollment {
    VARCHAR member_id
    DATE start_date
    DATE end_date
    VARCHAR gender
    VARCHAR metal_level
    DATE date_of_birth
  }
```

