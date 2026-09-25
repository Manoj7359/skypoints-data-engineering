-- Defensive transformation from raw source strings to staging/current state.
-- In a production ingestion job, map the formal source layout into this table.

INSERT INTO SKYPOINTS.STG_MEMBER
SELECT
    NULLIF(TRIM(member_name), '') AS member_name,
    NULLIF(TRIM(member_id), '') AS member_id,
    TRY_TO_DATE(enrollment_date_raw, 'YYYYMMDD') AS enrollment_date,
    TRY_TO_DATE(flight_date_raw, 'YYYYMMDD') AS flight_date,
    NULLIF(UPPER(TRIM(tier_code)), '') AS tier_code,
    agent_name,
    state,
    UPPER(country) AS country,
    TRY_TO_NUMBER(post_code_raw) AS post_code,
    TRY_TO_DATE(dob_raw, 'DDMMYYYY') AS dob,
    active_member,
    CASE
      WHEN dob_raw IS NOT NULL
      THEN DATEDIFF(year, TRY_TO_DATE(dob_raw, 'DDMMYYYY'), :AS_OF_DATE)
           - IFF(DATEADD(year, DATEDIFF(year, TRY_TO_DATE(dob_raw, 'DDMMYYYY'), :AS_OF_DATE),
                   TRY_TO_DATE(dob_raw, 'DDMMYYYY')) > :AS_OF_DATE, 1, 0)
      ELSE NULL
    END AS age,
    CASE
      WHEN TRY_TO_DATE(flight_date_raw, 'YYYYMMDD') IS NULL THEN NULL
      ELSE DATEDIFF(day, TRY_TO_DATE(flight_date_raw, 'YYYYMMDD'), :AS_OF_DATE) > 90
    END AS stale_member,
    :BATCH_ID,
    CURRENT_TIMESTAMP()
FROM SKYPOINTS.RAW_MEMBER_PARSED;

-- Latest valid record wins when a member changes country.
MERGE INTO SKYPOINTS.DIM_MEMBER_CURRENT tgt
USING (
    SELECT *
    FROM (
        SELECT s.*,
               ROW_NUMBER() OVER (
                   PARTITION BY member_id
                   ORDER BY enrollment_date DESC NULLS LAST,
                            flight_date DESC NULLS LAST,
                            ingestion_ts DESC
               ) AS rn
        FROM SKYPOINTS.STG_MEMBER s
        WHERE member_id IS NOT NULL
          AND member_name IS NOT NULL
          AND enrollment_date IS NOT NULL
          AND tier_code IN ('PLT','GLD','SLV')
          AND (flight_date IS NULL OR flight_date >= enrollment_date)
          AND (dob IS NULL OR dob <= :AS_OF_DATE)
    )
    WHERE rn = 1
) src
ON tgt.member_id = src.member_id
WHEN MATCHED THEN UPDATE SET
    member_name = src.member_name,
    enrollment_date = src.enrollment_date,
    flight_date = src.flight_date,
    tier_code = src.tier_code,
    agent_name = src.agent_name,
    state = src.state,
    country = src.country,
    post_code = src.post_code,
    dob = src.dob,
    active_member = src.active_member,
    age = src.age,
    stale_member = src.stale_member,
    updated_ts = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    member_id, member_name, enrollment_date, flight_date, tier_code,
    agent_name, state, country, post_code, dob, active_member,
    age, stale_member, updated_ts
) VALUES (
    src.member_id, src.member_name, src.enrollment_date, src.flight_date, src.tier_code,
    src.agent_name, src.state, src.country, src.post_code, src.dob, src.active_member,
    src.age, src.stale_member, CURRENT_TIMESTAMP()
);
