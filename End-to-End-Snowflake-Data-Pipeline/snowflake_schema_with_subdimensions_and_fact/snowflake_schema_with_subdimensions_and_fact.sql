/* =========================================================
   SNOWFLAKE SCHEMA – FROM GOLD LAYER (RE-RUNNABLE)
   ========================================================= */

CREATE SCHEMA IF NOT EXISTS demo_snowflake;

/* -------------------------------
   STEP 1: SUB-DIMENSIONS
-------------------------------- */

CREATE OR REPLACE TABLE demo_snowflake.dim_name (
    name_sk INT AUTOINCREMENT,
    name STRING,
    PRIMARY KEY (name_sk)
);

CREATE OR REPLACE TABLE demo_snowflake.dim_email (
    email_sk INT AUTOINCREMENT,
    email STRING,
    PRIMARY KEY (email_sk)
);

CREATE OR REPLACE TABLE demo_snowflake.dim_status (
    status_sk INT AUTOINCREMENT,
    status STRING,
    PRIMARY KEY (status_sk)
);

/* -------------------------------
   STEP 2: LOAD SUB-DIMENSIONS
-------------------------------- */

TRUNCATE TABLE demo_snowflake.dim_name;
INSERT INTO demo_snowflake.dim_name (name)
SELECT DISTINCT name
FROM demo_gold.users_gold
WHERE is_current = TRUE;

TRUNCATE TABLE demo_snowflake.dim_email;
INSERT INTO demo_snowflake.dim_email (email)
SELECT DISTINCT email
FROM demo_gold.users_gold
WHERE is_current = TRUE;

TRUNCATE TABLE demo_snowflake.dim_status;
INSERT INTO demo_snowflake.dim_status (status)
SELECT DISTINCT status
FROM demo_gold.users_gold
WHERE is_current = TRUE;

/* -------------------------------
   STEP 3: MAIN DIMENSION
-------------------------------- */

CREATE OR REPLACE TABLE demo_snowflake.dim_user (
    user_sk INT AUTOINCREMENT,
    id INT,
    name_sk INT,
    email_sk INT,
    status_sk INT,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN,
    is_deleted BOOLEAN,
    PRIMARY KEY (user_sk)
);

TRUNCATE TABLE demo_snowflake.dim_user;

INSERT INTO demo_snowflake.dim_user (
    id, name_sk, email_sk, status_sk,
    start_date, end_date, is_current, is_deleted
)
SELECT
    g.id,
    n.name_sk,
    e.email_sk,
    s.status_sk,
    g.start_date,
    g.end_date,
    g.is_current,
    g.is_deleted
FROM demo_gold.users_gold g
JOIN demo_snowflake.dim_name n   ON g.name   = n.name
JOIN demo_snowflake.dim_email e  ON g.email  = e.email
JOIN demo_snowflake.dim_status s ON g.status = s.status
WHERE g.is_current = TRUE;

/* -------------------------------
   STEP 4: FACT TABLE
-------------------------------- */

CREATE OR REPLACE TABLE demo_snowflake.fact_user_activity (
    fact_sk INT AUTOINCREMENT,
    user_sk INT,
    event_ts TIMESTAMP,
    status_sk INT
);

TRUNCATE TABLE demo_snowflake.fact_user_activity;

INSERT INTO demo_snowflake.fact_user_activity (
    user_sk, event_ts, status_sk
)
SELECT
    u.user_sk,
    g.event_ts,
    s.status_sk
FROM demo_gold.users_gold g
JOIN demo_snowflake.dim_user u
        ON g.id = u.id AND g.is_current = TRUE
JOIN demo_snowflake.dim_status s
        ON g.status = s.status;

/* -------------------------------
   STEP 5: FINAL QUERY (VALIDATION)
-------------------------------- */

SELECT
    f.fact_sk,
    u.id,
    n.name,
    e.email,
    st.status,
    f.event_ts,
    u.start_date,
    u.end_date,
    u.is_current,
    u.is_deleted
FROM demo_snowflake.fact_user_activity f
JOIN demo_snowflake.dim_user u   ON f.user_sk = u.user_sk
JOIN demo_snowflake.dim_name n   ON u.name_sk = n.name_sk
JOIN demo_snowflake.dim_email e  ON u.email_sk = e.email_sk
JOIN demo_snowflake.dim_status st ON u.status_sk = st.status_sk
ORDER BY f.fact_sk;
