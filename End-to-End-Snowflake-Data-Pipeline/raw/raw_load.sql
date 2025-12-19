--Create Database & RAW Schema
CREATE DATABASE IF NOT EXISTS demo_project;
CREATE SCHEMA IF NOT EXISTS demo_raw;
--Create RAW Table
CREATE OR REPLACE TABLE demo_raw.raw_users_json (
    raw_data VARIANT,
    load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
--Insert Source Data into RAW
INSERT INTO demo_raw.raw_users_json (raw_data)
SELECT PARSE_JSON('{
  "id": 1,
  "name": "John Miller",
  "email": "john@gmail.com",
  "status": "ACTIVE",
  "event_ts": "2025-11-27 11:39:00"
}');
sql
INSERT INTO demo_raw.raw_users_json (raw_data)
SELECT PARSE_JSON('{
  "id": 2,
  "name": "Ravi Teja",
  "email": "ravi@gmail.com",
  "status": "ACTIVE",
  "event_ts": "2025-11-27 11:40:00"
}');
sql
INSERT INTO demo_raw.raw_users_json (raw_data)
SELECT PARSE_JSON('{
  "id": 3,
  "name": "Sara Khan",
  "email": "sara@gmail.com",
  "status": "ACTIVE",
  "event_ts": "2025-11-27 21:15:06"
  -- Insert Updates / Deletes (CDC Events)
  -- Updated record
INSERT INTO demo_raw.raw_users_json (raw_data)
SELECT PARSE_JSON('{
  "id": 1,
  "name": "John Miller",
  "email": "john.miller2025@gmail.com",
  "status": "ACTIVE",
  "event_ts": "2025-11-27 21:10:01"
}');
sql
-- Logical delete event
INSERT INTO demo_raw.raw_users_json (raw_data)
SELECT PARSE_JSON('{
  "id": 2,
  "name": "Ravi Teja",
  "email": "ravi@gmail.com",
  "status": "DELETED",
  "event_ts": "2025-11-27 21:09:05"
}');
  --Insert Duplicate & Late-Arriving Data
-- Updated record
INSERT INTO demo_raw.raw_users_json (raw_data)
SELECT PARSE_JSON('{
  "id": 1,
  "name": "John Miller",
  "email": "john.miller2025@gmail.com",
  "status": "ACTIVE",
  "event_ts": "2025-11-27 21:10:01"
}');
sql

-- Logical delete event
INSERT INTO demo_raw.raw_users_json (raw_data)
SELECT PARSE_JSON('{
  "id": 2,
  "name": "Ravi Teja",
  "email": "ravi@gmail.com",
  "status": "DELETED",
  "event_ts": "2025-11-27 21:09:05"
}');
  -- Insert Duplicate & Late-Arriving Data
  -- Duplicate event
INSERT INTO demo_raw.raw_users_json (raw_data)
SELECT PARSE_JSON('{
  "id": 3,
  "name": "Sara Khan",
  "email": "sara@gmail.com",
  "status": "ACTIVE",
  "event_ts": "2025-11-27 21:15:06"
}');
sql

-- Late-arriving record
INSERT INTO demo_raw.raw_users_json (raw_data)
SELECT PARSE_JSON('{
  "id": 4,
  "name": "Khan",
  "email": "khan@gmail.com",
  "status": "ACTIVE",
  "event_ts": "2025-11-27 04:15:06"
}');
--View RAW Data (Audit & Validation)
  SELECT
    raw_data:id::INT        AS id,
    raw_data:name::STRING   AS name,
    raw_data:email::STRING  AS email,
    raw_data:status::STRING AS status,
    raw_data:event_ts::TIMESTAMP AS event_ts,
    load_ts
FROM demo_raw.raw_users_json
ORDER BY load_ts DESC;
--Delete Specific RAW Records (Demo Cleanup)
DELETE FROM demo_raw.raw_users_json
WHERE raw_data:id::INT = 3
  AND raw_data:email::STRING = 'sara@gmail.com';
--Final RAW to see output
  SELECT *
FROM demo_raw.raw_users_json;
  
























  

