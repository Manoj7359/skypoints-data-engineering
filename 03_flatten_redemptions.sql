-- Assume RAW_REDEMPTION_FEED has:
-- member_id STRING, feed_date STRING, redemptions VARIANT

INSERT INTO SKYPOINTS.FACT_REDEMPTION (
    member_id, feed_date, txn_id, txn_date, partner, miles_redeemed, status
)
SELECT
    member_id::STRING,
    TRY_TO_DATE(feed_date::STRING, 'YYYYMMDD'),
    r.value:txn_id::STRING,
    TRY_TO_DATE(r.value:txn_date::STRING, 'YYYYMMDD'),
    r.value:partner::STRING,
    r.value:miles_redeemed::NUMBER,
    UPPER(r.value:status::STRING)
FROM SKYPOINTS.RAW_REDEMPTION_FEED,
LATERAL FLATTEN(input => redemptions) r
WHERE r.value:txn_id IS NOT NULL;

-- Join back to the latest/current member profile.
SELECT
    m.member_id,
    m.member_name,
    m.country,
    m.tier_code,
    r.txn_id,
    r.txn_date,
    r.partner,
    r.miles_redeemed,
    r.status
FROM SKYPOINTS.FACT_REDEMPTION r
JOIN SKYPOINTS.DIM_MEMBER_CURRENT m
  ON r.member_id = m.member_id;
