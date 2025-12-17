/* =========================================================
   STAR SCHEMA IMPLEMENTATION FROM GOLD LAYER
   ========================================================= */

/* ---------- STEP 1: CREATE STAR SCHEMA ---------- */
CREATE SCHEMA IF NOT EXISTS demo_star;


/* ---------- STEP 2: CREATE DIMENSION TABLE ---------- */
CREATE OR REPLACE TABLE demo_star.dim_user (
    user_sk INT AUTOINCREMENT,
    id INT,
    name STRING,
    email STRING,
    status STRING,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN,
    is_deleted BOOLEAN,
    PRIMARY KEY (user_sk)
);


/* ---------- STEP 3: FIRST RUN – LOAD DIMENSION ---------- */
INSERT INTO demo_star.dim_user (
    id,
    name,
    email,
    status,
    start_date,
    end_date,
    is_current,
    is_deleted
)
SELECT
    id,
    name,
    email,
    status,
    start_date,
    end_date,
    is_current,
    is_deleted
FROM demo_gold.users_gold
WHERE is_current = TRUE;


/* ---------- STEP 4: CREATE FACT TABLE ---------- */
CREATE OR REPLACE TABLE demo_star.fact_user_activity (
    fact_sk INT AUTOINCREMENT,
    user_sk INT,
    event_ts TIMESTAMP,
    status STRING
);


/* ---------- STEP 5: FIRST RUN – LOAD FACT TABLE ---------- */
INSERT INTO demo_star.fact_user_activity (
    user_sk,
    event_ts,
    status
)
SELECT
    d.user_sk,
    g.event_ts,
    g.status
FROM demo_gold.users_gold g
JOIN demo_star.dim_user d
    ON g.id = d.id
   AND g.is_current = TRUE;


/* ---------- STEP 6: VALIDATION QUERIES ---------- */

-- Dimension output
SELECT * 
FROM demo_star.dim_user 
ORDER BY user_sk;

-- Fact output
SELECT * 
FROM demo_star.fact_user_activity 
ORDER BY fact_sk;

-- Full STAR schema view
SELECT
    f.fact_sk,
    f.user_sk,
    d.id,
    d.name,
    d.email,
    f.event_ts,
    f.status,
    d.start_date,
    d.end_date,
    d.is_current,
    d.is_deleted
FROM demo_star.fact_user_activity f
JOIN demo_star.dim_user d
    ON f.user_sk = d.user_sk
ORDER BY f.fact_sk;


/* =========================================================
   SECOND RUN – STAR SCHEMA REFRESH (IDEMPOTENT)
   ========================================================= */

-- Refresh DIMENSION
TRUNCATE TABLE demo_star.dim_user;

INSERT INTO demo_star.dim_user (
    id,
    name,
    email,
    status,
    start_date,
    end_date,
    is_current,
    is_deleted
)
SELECT
    id,
    name,
    email,
    status,
    start_date,
    end_date,
    is_current,
    is_deleted
FROM demo_gold.users_gold
WHERE is_current = TRUE;

-- Refresh FACT
TRUNCATE TABLE demo_star.fact_user_activity;

INSERT INTO demo_star.fact_user_activity (
    user_sk,
    event_ts,
    status
)
SELECT
    d.user_sk,
    g.event_ts,
    g.status
FROM demo_gold.users_gold g
JOIN demo_star.dim_user d
    ON g.id = d.id
   AND g.is_current = TRUE;


/* ---------- FINAL CHECK ---------- */
SELECT
    f.fact_sk,
    f.user_sk,
    d.id,
    d.name,
    d.email,
    f.event_ts,
    f.status,
    d.start_date,
    d.end_date,
    d.is_current,
    d.is_deleted
FROM demo_star.fact_user_activity f
JOIN demo_star.dim_user d
    ON f.user_sk = d.user_sk
ORDER BY f.fact_sk;
