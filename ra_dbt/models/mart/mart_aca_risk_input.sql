{{ config(enabled=false) }}

-- Deprecated: dbt stops at the intermediate layer.
-- Replaced by `models/intermediate/int_aca_risk_input.sql`.
select 1 as deprecated
