# SkyPoints Global Loyalty — Data Engineering Assessment

An end-to-end, production-oriented data engineering solution for the SkyPoints airline loyalty assessment.

## What this project demonstrates

- Raw/landing, staging, curated and country-specific target layers
- PySpark transformations for large-scale processing
- SQL/Snowflake DDL and transformation logic
- Age and stale-member derivations
- Latest-record-wins logic when a member changes country
- JSON redemption-feed flattening
- Data-quality checks for mandatory fields, duplicate keys, invalid dates and invalid tier/country values
- Idempotent processing using batch/feed dates
- Unit tests with `pytest`
- Incremental/partition-aware design suitable for billions of records/day
- Git-friendly project structure and incremental-commit plan

The assessment explicitly asks for production-quality code, tests, architectural decisions, intentional AI usage, and incremental commits. fileciteturn0file0L8-L14

## Source problem

The source system supplies:
1. A daily flat-file member profile feed.
2. A daily semi-structured JSON redemption feed.

The assessment requires country-specific target tables, derived `Age` and `Stale_Member`, latest-record-wins country movement logic, JSON flattening, joins, and data validations. fileciteturn0file0L20-L25 fileciteturn0file0L78-L91

## Architecture

```text
                 +----------------------+
                 | Daily Source Feeds   |
                 |----------------------|
                 | Member flat files    |
                 | Redemption JSON      |
                 +----------+-----------+
                            |
                            v
                 +----------------------+
                 | RAW / LANDING        |
                 | immutable + audit    |
                 +----------+-----------+
                            |
                            v
                 +----------------------+
                 | STAGING              |
                 | standardize/validate |
                 | parse dates          |
                 | derive age/stale     |
                 +----------+-----------+
                            |
              +-------------+-------------+
              |                           |
              v                           v
   +----------------------+     +----------------------+
   | MEMBER CURATED       |     | REDEMPTION FLATTENED |
   | latest record/member |     | one row per txn      |
   +----------+-----------+     +----------+-----------+
              |                            |
              +-------------+--------------+
                            |
                            v
                 +----------------------+
                 | COUNTRY TARGETS      |
                 | India / USA / AUS... |
                 +----------------------+
```

The assessment's design calls for raw/landing, staging and country-specific target tables. fileciteturn0file0L81-L87

## Project structure

```text
skypoints-de-project/
├── README.md
├── requirements.txt
├── .gitignore
├── data/
│   └── raw/
│       ├── USA.csv
│       ├── AUS.csv
│       ├── India.csv
│       └── redemptions.json
├── sql/
│   ├── 01_ddl_snowflake.sql
│   ├── 02_transform_members.sql
│   ├── 03_flatten_redemptions.sql
│   └── 04_quality_checks.sql
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── member_pipeline.py
│   └── redemption_pipeline.py
├── tests/
│   └── test_pipeline.py
└── docs/
    ├── architecture.md
    └── git_commit_plan.md
```

## Source sample supplied for this implementation

The assessment PDF provides a formal source-file specification and sample flat-file/JSON structures. fileciteturn0file0L26-L34 fileciteturn0file0L50-L66

The implementation also includes the three business samples supplied with this assessment:

### USA sample

```text
Unique ID | Member Name | Tier Type | Date of Birth | Date of Enrollment | Date of Flight
1         | Mike        | PLT       | NULL          | 2022-05-11         | 2022-08-01
2         | Jonnathan   | GLD       | 1997-12-13    | 2021-13-13         | 2022-01-05
3         | Cristina    | SLV       | 1998-03-12    | 2022-03-12         | 2022-03-20
```

`2021-13-13` is intentionally invalid and should be rejected/quarantined by date validation.

### AUS sample

```text
ID | Name  | TierCode | EnrollmentDate | FlightDate
1  | Sam   | PLT      | 6152022        | 8202022
2  | John  | SLV      | 1052022        | 1152022
3  | Mike  | GLD      | 12282021       | 12302021
```

The dates are interpreted as `MDDYYYY`/`MMDDYYYY`-style source strings and normalized during ingestion.

### India sample

```text
ID | Name   | DOB       | TierCode | EnrollmentDate | IndividualOrCorporate | FlightDate
1  | Vikas  | 12/1/1998 | SLV      | 1/1/2022       | I                    | 6/15/2022
2  | Rahul  | 8/13/1982  | GLD      | 3/5/2022       | C                    | 3/10/2022
3  | Sameer | 8/13/1952  | GLD      | 2/20/2022      | I                    | 2/25/2022
```

## Key business rules

### 1. Mandatory fields

The assessment marks Member Name, Member ID and Enrollment Date as mandatory/key fields in the formal source specification. fileciteturn0file0L37-L49

### 2. Latest record wins

A member can move between countries. The current record is selected using:

```sql
ROW_NUMBER() OVER (
    PARTITION BY member_id
    ORDER BY enrollment_date DESC, flight_date DESC, ingestion_ts DESC
)
```

Only the latest valid record is loaded into the current country target.

### 3. Age

```text
age = floor(months_between(as_of_date, dob) / 12)
```

For production, `as_of_date` is supplied as a batch parameter instead of using an uncontrolled current timestamp.

### 4. Stale member

```text
stale_member = true when days_since_flight > 90
```

This follows the assessment requirement. fileciteturn0file0L84-L85

If `flight_date` is null, the pipeline marks the record as `UNKNOWN` for stale status rather than incorrectly classifying it as stale.

### 5. Country routing

`USA`, `AUS`, `IND` and other valid ISO-like country codes are routed dynamically. The implementation does not hard-code only three countries in the transformation.

### 6. Redemption join

The flattened redemption table joins to the current member dimension on:

```text
redemptions.member_id = member.member_id
```

The assessment's JSON example contains `member_id`, `feed_date`, transaction id/date, partner, miles and status. fileciteturn0file0L58-L68

## Data quality strategy

Checks include:

- Mandatory member fields
- Member ID uniqueness within a source/batch
- Valid dates
- Enrollment date not after flight date
- Valid tier code
- Valid country code
- DOB not in the future
- Negative/invalid miles
- Redemption transaction uniqueness
- Redemption member existence
- Accepted redemption statuses
- Quarantine of rejected records

The assessment specifically asks for mandatory checks, key uniqueness and additional checks for issues visible in the sample data. fileciteturn0file0L90-L91

## Running locally

### 1. Install

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/macOS
source .venv/bin/activate

pip install -r requirements.txt
```

### 2. Run member pipeline

```bash
python src/member_pipeline.py \
  --input data/raw/USA.csv \
  --country USA \
  --as-of-date 2022-08-15
```

For a real Spark cluster:

```bash
spark-submit src/member_pipeline.py \
  --input abfss://raw@storage.dfs.core.windows.net/members/ \
  --country ALL \
  --as-of-date 2026-09-24
```

### 3. Run redemption pipeline

```bash
python src/redemption_pipeline.py \
  --input data/raw/redemptions.json \
  --output data/processed/redemptions
```

### 4. Run tests

```bash
pytest -q
```

## Scaling to billions of records/day

For the assessment's billion-record requirement, do not process everything through one driver or a single Python process.

Recommended production pattern:

- Object storage: ADLS Gen2/S3/GCS
- Compute: Databricks/Spark
- Raw format: immutable files, preferably Parquet/Delta after landing
- Partition by `feed_date` and optionally country
- Incremental processing by feed/batch date
- Use Spark SQL/DataFrame operations instead of Python row loops
- Broadcast only genuinely small reference data
- Repartition by `member_id` before window/dedup operations when appropriate
- Avoid `collect()`, `toPandas()` and driver-side loops
- Use Delta MERGE for current-state tables
- Compact small files
- Use schema enforcement/evolution deliberately
- Maintain quarantine and audit tables
- Add metrics: input count, valid count, rejected count, duplicate count, target count, processing duration
- Make each batch idempotent using a `batch_id`/`feed_date`

The source assessment explicitly says to design for billions of records per day across both feeds. fileciteturn0file0L78-L80

## Snowflake implementation

Run these in order:

```text
01_ddl_snowflake.sql
02_transform_members.sql
03_flatten_redemptions.sql
04_quality_checks.sql
```

The SQL uses `TRY_TO_DATE`/`TRY_TO_TIMESTAMP`-style defensive parsing so malformed source values can be identified instead of failing the whole batch.

## Git commit strategy

Use small, meaningful commits rather than one giant upload.

Example:

```text
git init
git add README.md .gitignore requirements.txt
git commit -m "chore: initialize SkyPoints DE assessment"

git add sql/01_ddl_snowflake.sql
git commit -m "feat: add raw staging and target DDL"

git add src/member_pipeline.py
git commit -m "feat: add member transformation and country routing"

git add src/redemption_pipeline.py sql/03_flatten_redemptions.sql
git commit -m "feat: flatten redemption transactions"

git add tests/
git commit -m "test: add data quality and transformation tests"

git add docs/
git commit -m "docs: add architecture and scale design"

git add data/
git commit -m "testdata: add assessment sample feeds"

git add .
git commit -m "docs: finalize assessment implementation"
```

The assessment explicitly asks for incremental commits so the evolution of the solution can be reviewed. fileciteturn0file0L12-L14

## Publish to GitHub

Create a public repository, then:

```bash
git branch -M main
git remote add origin https://github.com/<your-username>/skypoints-data-engineering.git
git push -u origin main
```

Do not commit credentials, connection strings, `.env` files or production data.

## Interview walkthrough

A strong live demo sequence is:

1. Show the architecture.
2. Show raw sample data and intentionally invalid USA date.
3. Run validation.
4. Show staging columns including age/stale flag.
5. Show latest-record-wins logic.
6. Show India/Australia/USA routing.
7. Show flattened redemption rows.
8. Explain the member join.
9. Run tests.
10. Explain how the same design scales to billions of rows.

The assessment states that a live demonstration may be requested during the interview. fileciteturn0file0L92-L92
