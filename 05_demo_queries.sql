-- Demo: current country distribution
SELECT country, COUNT(*) AS members
FROM SKYPOINTS.DIM_MEMBER_CURRENT
GROUP BY country
ORDER BY country;

-- Demo: members with redemptions
SELECT
    m.country,
    m.tier_code,
    COUNT(DISTINCT m.member_id) AS members,
    SUM(r.miles_redeemed) AS total_miles
FROM SKYPOINTS.DIM_MEMBER_CURRENT m
JOIN SKYPOINTS.FACT_REDEMPTION r
  ON m.member_id = r.member_id
GROUP BY m.country, m.tier_code
ORDER BY m.country, m.tier_code;
