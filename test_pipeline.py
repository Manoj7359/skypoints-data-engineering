import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from member_pipeline import parse_india, parse_aus, parse_usa, standardize, latest_record_wins


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[2]").appName("tests").getOrCreate()
    yield s
    s.stop()


def test_india_date_parsing(spark):
    df = spark.createDataFrame([
        ("1", "Vikas", "12/1/1998", "SLV", "1/1/2022", "I", "6/15/2022")
    ], ["id","name","dob","tier_code","enrollment_date","individual_or_corporate","flight_date"])

    out = parse_india(df).collect()[0]
    assert str(out.dob) == "1998-12-01"
    assert str(out.enrollment_date) == "2022-01-01"
    assert str(out.flight_date) == "2022-06-15"


def test_aus_date_parsing(spark):
    df = spark.createDataFrame([
        ("1", "Sam", "PLT", "6152022", "8202022")
    ], ["id","name","tier_code","enrollment_date","flight_date"])

    out = parse_aus(df).collect()[0]
    assert str(out.enrollment_date) == "2022-06-15"
    assert str(out.flight_date) == "2022-08-20"


def test_invalid_usa_enrollment_is_rejected(spark):
    df = spark.createDataFrame([
        ("2", "Jonnathan", "GLD", "1997-12-13", "2021-13-13", "2022-01-05")
    ], ["unique_id","member_name","tier_type","date_of_birth","date_of_enrollment","date_of_flight"])

    staged = standardize(parse_usa(df), "2022-08-15")
    row = staged.collect()[0]
    assert row.enrollment_date is None
    assert row.dq_enrollment_missing is True


def test_latest_record_wins_on_member_country_move(spark):
    df = spark.createDataFrame([
        ("1", "Mike", "GLD", None, "2022-01-01", "2022-01-05", "USA"),
        ("1", "Mike", "GLD", None, "2022-06-01", "2022-06-10", "IND"),
    ], ["member_id","member_name","tier_code","dob","enrollment_date","flight_date","country"])

    df = (
        df.withColumn("enrollment_date", F.to_date("enrollment_date"))
          .withColumn("flight_date", F.to_date("flight_date"))
          .withColumn("dob", F.to_date("dob"))
          .withColumn("ingestion_ts", F.current_timestamp())
    )
    out = latest_record_wins(df).collect()
    assert len(out) == 1
    assert out[0].country == "IND"


def test_stale_member(spark):
    df = spark.createDataFrame([
        ("1", "Sam", "PLT", None, "2022-01-01", "2022-01-01", "USA")
    ], ["member_id","member_name","tier_code","dob","enrollment_date","flight_date","country"])
    df = (
        df.withColumn("enrollment_date", F.to_date("enrollment_date"))
          .withColumn("flight_date", F.to_date("flight_date"))
          .withColumn("dob", F.to_date("dob"))
          .withColumn("ingestion_ts", F.current_timestamp())
    )
    out = standardize(df, "2022-04-10").collect()[0]
    assert out.stale_member is True
