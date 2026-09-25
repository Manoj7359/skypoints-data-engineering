-- SkyPoints Data Engineering Assessment
-- Snowflake DDL

CREATE OR REPLACE SCHEMA SKYPOINTS;

CREATE OR REPLACE TABLE SKYPOINTS.RAW_MEMBER_FEED (
    source_file STRING,
    load_batch_id STRING,
    raw_record VARIANT,
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE SKYPOINTS.STG_MEMBER (
    member_name STRING,
    member_id STRING,
    enrollment_date DATE,
    flight_date DATE,
    tier_code STRING,
    agent_name STRING,
    state STRING,
    country STRING,
    post_code NUMBER,
    dob DATE,
    active_member STRING,
    age NUMBER,
    stale_member BOOLEAN,
    load_batch_id STRING,
    ingestion_ts TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE SKYPOINTS.DIM_MEMBER_CURRENT (
    member_id STRING NOT NULL,
    member_name STRING NOT NULL,
    enrollment_date DATE NOT NULL,
    flight_date DATE,
    tier_code STRING,
    agent_name STRING,
    state STRING,
    country STRING,
    post_code NUMBER,
    dob DATE,
    active_member STRING,
    age NUMBER,
    stale_member BOOLEAN,
    updated_ts TIMESTAMP_NTZ,
    CONSTRAINT pk_dim_member PRIMARY KEY (member_id)
);

CREATE OR REPLACE TABLE SKYPOINTS.FACT_REDEMPTION (
    member_id STRING NOT NULL,
    feed_date DATE NOT NULL,
    txn_id STRING NOT NULL,
    txn_date DATE,
    partner STRING,
    miles_redeemed NUMBER,
    status STRING,
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_redemption PRIMARY KEY (txn_id)
);

CREATE OR REPLACE TABLE SKYPOINTS.DQ_MEMBER_REJECT (
    member_id STRING,
    source_file STRING,
    rejection_reason STRING,
    raw_payload VARIANT,
    rejected_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE SKYPOINTS.DQ_REDEMPTION_REJECT (
    member_id STRING,
    txn_id STRING,
    rejection_reason STRING,
    rejected_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Country-specific views provide the same logical contract as country tables
-- while avoiding duplicate physical storage. They can be materialized tables
-- if downstream systems require physical tables.
CREATE OR REPLACE VIEW SKYPOINTS.TABLE_INDIA AS
SELECT * FROM SKYPOINTS.DIM_MEMBER_CURRENT WHERE country = 'IND';

CREATE OR REPLACE VIEW SKYPOINTS.TABLE_USA AS
SELECT * FROM SKYPOINTS.DIM_MEMBER_CURRENT WHERE country = 'USA';

CREATE OR REPLACE VIEW SKYPOINTS.TABLE_AUS AS
SELECT * FROM SKYPOINTS.DIM_MEMBER_CURRENT WHERE country = 'AUS';
