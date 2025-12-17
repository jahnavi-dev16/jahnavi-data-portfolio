-- Create GOLD Schema & Table
CREATE SCHEMA IF NOT EXISTS demo_gold;

CREATE OR REPLACE TABLE demo_gold.users_gold (
    id INT,
    name STRING,
    email STRING,
    status STRING,
    event_ts TIMESTAMP,
    hash_value STRING,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN,
    is_deleted BOOLEAN
);
-- GOLD Layer SCD-2 MERGE Logic
MERGE INTO demo_gold.users_gold g
USING (
    SELECT
        id,
        name,
        email,
        status,
        event_ts,
        SHA2(CONCAT(id, name, email, status)) AS hash_value
    FROM demo_silver.users_silver
) s
ON g.id = s.id
AND g.is_current = TRUE

-- If record exists and data changed → close old record
WHEN MATCHED AND g.hash_value != s.hash_value THEN
    UPDATE SET
        g.end_date   = s.event_ts,
        g.is_current = FALSE,
        g.is_deleted = (s.status = 'DELETED')

-- Insert new record or new version
WHEN NOT MATCHED THEN
    INSERT (
        id,
        name,
        email,
        status,
        event_ts,
        hash_value,
        start_date,
        end_date,
        is_current,
        is_deleted
    )
    VALUES (
        s.id,
        s.name,
        s.email,
        s.status,
        s.event_ts,
        s.hash_value,
        s.event_ts,
        NULL,
        TRUE,
        (s.status = 'DELETED')
    );
-- Validate GOLD Output
SELECT *
FROM demo_gold.users_gold
WHERE is_current = TRUE
ORDER BY id;
-- Full History for a User
SELECT *
FROM demo_gold.users_gold
WHERE id = 3
ORDER BY start_date;


--view
-creating view
CREATE OR REPLACE VIEW demo_gold.users_gold_vw AS
SELECT *
FROM demo_gold.users_gold
ORDER BY id, start_date;

--to see output view
SELECT * FROM demo_gold.users_gold_vw;

--to see particular one view output
SELECT *
FROM demo_gold.users_gold_vw
WHERE id = 3
ORDER BY start_date;
-- to see raw data
SELECT
    raw_data:id::INT AS id,
    raw_data:name::STRING AS name,
    raw_data:email::STRING AS email,
    raw_data:status::STRING AS status,
    raw_data:event_ts::TIMESTAMP AS event_ts,
    load_ts
FROM demo_raw.raw_users_json
WHERE raw_data:id::INT = 3
ORDER BY load_ts DESC
LIMIT 1;
-- creating Materialized View 
CREATE OR REPLACE MATERIALIZED VIEW demo_gold.users_gold_mv AS
SELECT
    id,
    name,
    email,
    status,
    event_ts,
    hash_value,
    start_date,
    end_date,
    is_current,
    is_deleted
FROM demo_gold.users_gold
ORDER BY id, start_date;












