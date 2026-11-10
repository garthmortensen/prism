# dbt_raw year-filtered views

These dbt models create **views** in the `dbt_raw` schema that filter the seed tables
(`raw_claims`, `raw_enrollments`, `raw_members`) by service/benefit year.

They are intended as convenience views for year-specific analyses and pipelines.
