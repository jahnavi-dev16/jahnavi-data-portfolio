FIRST RUN (Initial SILVER build)
-- Create SILVER schema (one time)
CREATE SCHEMA IF NOT EXISTS demo_silver;

-- Build SILVER table
CREATE OR REPLACE TABLE demo_silver.users_silver AS
SELECT
    raw_data:id::INT        AS id,
    raw_data:name::STRING   AS name,
    raw_data:email::STRING  AS email,
    raw_data:status::STRING AS status,
    raw_data:event_ts::TIMESTAMP AS event_ts
FROM (
    SELECT
        raw_data,
        load_ts,
        ROW_NUMBER() OVER (
            PARTITION BY raw_data:id::INT
            ORDER BY load_ts DESC
        ) AS rn
    FROM demo_raw.raw_users_json
)
WHERE rn = 1;

SECOND RUN (Re-run / Refresh SILVER)
-- Re-run uses the SAME code
CREATE OR REPLACE TABLE demo_silver.users_silver AS
SELECT
    raw_data:id::INT        AS id,
    raw_data:name::STRING   AS name,
    raw_data:email::STRING  AS email,
    raw_data:status::STRING AS status,
    raw_data:event_ts::TIMESTAMP AS event_ts
FROM (
    SELECT
        raw_data,
        load_ts,
        ROW_NUMBER() OVER (
            PARTITION BY raw_data:id::INT
            ORDER BY load_ts DESC
        ) AS rn
    FROM demo_raw.raw_users_json
)
WHERE rn = 1;

CHECK DATA 
SELECT * FROM demo_silver.users_silver ORDER BY id;
SELECT * FROM demo_silver.users_silver WHERE id = 3;

  


  





























  

