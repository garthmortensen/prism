-- Clear existing data
TRUNCATE TABLE raw.raw_claims;
TRUNCATE TABLE raw.raw_members;
TRUNCATE TABLE raw.raw_enrollments;
TRUNCATE TABLE raw.raw_plans;
TRUNCATE TABLE raw.raw_providers;

-- Fast load from mounted CSVs
-- NOTE: The path /app/ra_dbt/seeds/... assumes the container path
COPY raw.raw_claims FROM '/mnt/seeds/raw_claims.csv' DELIMITER ',' CSV HEADER;
COPY raw.raw_members FROM '/mnt/seeds/raw_members.csv' DELIMITER ',' CSV HEADER;
COPY raw.raw_enrollments FROM '/mnt/seeds/raw_enrollments.csv' DELIMITER ',' CSV HEADER;
COPY raw.raw_plans FROM '/mnt/seeds/raw_plans.csv' DELIMITER ',' CSV HEADER;
COPY raw.raw_providers FROM '/mnt/seeds/raw_providers.csv' DELIMITER ',' CSV HEADER;
