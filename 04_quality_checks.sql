-- Mandatory field checks
SELECT COUNT(*) AS missing_mandatory
FROM SKYPOINTS.STG_MEMBER
WHERE member_id IS NULL
   OR member_name IS NULL
   OR enrollment_date IS NULL;

-- Key uniqueness
SELECT member_id, COUNT(*) AS record_count
FROM SKYPOINTS.STG_MEMBER
GROUP BY member_id
HAVING COUNT(*) > 1;

-- Invalid tier
SELECT *
FROM SKYPOINTS.STG_MEMBER
WHERE tier_code IS NOT NULL
  AND tier_code NOT IN ('PLT','GLD','SLV');

-- Flight before enrollment
SELECT *
FROM SKYPOINTS.STG_MEMBER
WHERE flight_date IS NOT NULL
  AND enrollment_date IS NOT NULL
  AND flight_date < enrollment_date;

-- Future DOB
SELECT *
FROM SKYPOINTS.STG_MEMBER
WHERE dob > :AS_OF_DATE;

-- Duplicate redemption transactions
SELECT txn_id, COUNT(*) AS record_count
FROM SKYPOINTS.FACT_REDEMPTION
GROUP BY txn_id
HAVING COUNT(*) > 1;

-- Negative mileage
SELECT *
FROM SKYPOINTS.FACT_REDEMPTION
WHERE miles_redeemed < 0;

-- Orphan redemptions
SELECT r.*
FROM SKYPOINTS.FACT_REDEMPTION r
LEFT JOIN SKYPOINTS.DIM_MEMBER_CURRENT m
  ON r.member_id = m.member_id
WHERE m.member_id IS NULL;

-- Accepted statuses
SELECT DISTINCT status
FROM SKYPOINTS.FACT_REDEMPTION
WHERE status NOT IN ('COMPLETED','PENDING','CANCELLED');
