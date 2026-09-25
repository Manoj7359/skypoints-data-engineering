import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from config import VALID_TIERS


def build_spark():
    return (
        SparkSession.builder
        .appName("SkyPoints-Member-Pipeline")
        .master("local[*]")
        .getOrCreate()
    )


def parse_usa(df):
    return (
        df.select(
            F.col("unique_id").cast("string").alias("member_id"),
            "member_name",
            "tier_type",
            F.when(F.upper(F.col("date_of_birth")) == "NULL", None)
             .otherwise(F.col("date_of_birth")).alias("dob_raw"),
            "date_of_enrollment",
            "date_of_flight",
        )
        .withColumn("country", F.lit("USA"))
        .withColumn("tier_code", F.upper(F.col("tier_type")))
        .withColumn("dob", F.to_date("dob_raw", "yyyy-MM-dd"))
        .withColumn("enrollment_date", F.to_date("date_of_enrollment", "yyyy-MM-dd"))
        .withColumn("flight_date", F.to_date("date_of_flight", "yyyy-MM-dd"))
        .drop("tier_type", "dob_raw", "date_of_enrollment", "date_of_flight")
    )


def parse_aus(df):
    # Source samples use MDDYYYY/MMDDYYYY-style values.
    return (
        df.select(
            F.col("id").cast("string").alias("member_id"),
            F.col("name").alias("member_name"),
            F.col("tier_code"),
            F.col("enrollment_date").cast("string").alias("enrollment_raw"),
            F.col("flight_date").cast("string").alias("flight_raw"),
        )
        .withColumn("country", F.lit("AUS"))
        .withColumn("enrollment_date", F.to_date("enrollment_raw", "MMddyyyy"))
        .withColumn("flight_date", F.to_date("flight_raw", "MMddyyyy"))
        .withColumn("dob", F.lit(None).cast("date"))
        .drop("enrollment_raw", "flight_raw")
    )


def parse_india(df):
    return (
        df.select(
            F.col("id").cast("string").alias("member_id"),
            F.col("name").alias("member_name"),
            "dob",
            "tier_code",
            "enrollment_date",
            "individual_or_corporate",
            "flight_date",
        )
        .withColumn("country", F.lit("IND"))
        .withColumn("dob", F.to_date("dob", "M/d/yyyy"))
        .withColumn("enrollment_date", F.to_date("enrollment_date", "M/d/yyyy"))
        .withColumn("flight_date", F.to_date("flight_date", "M/d/yyyy"))
    )


def standardize(df, as_of_date: str):
    return (
        df.withColumn("tier_code", F.upper(F.trim("tier_code")))
          .withColumn("member_name", F.trim("member_name"))
          .withColumn("age", F.when(F.col("dob").isNotNull(),
                                   F.floor(F.months_between(F.to_date(F.lit(as_of_date)), "dob") / 12)))
          .withColumn(
              "stale_member",
              F.when(F.col("flight_date").isNull(), F.lit(None).cast("boolean"))
               .otherwise(F.datediff(F.to_date(F.lit(as_of_date)), "flight_date") > 90)
          )
          .withColumn("dq_member_id_missing", F.col("member_id").isNull() | (F.trim("member_id") == ""))
          .withColumn("dq_name_missing", F.col("member_name").isNull() | (F.trim("member_name") == ""))
          .withColumn("dq_enrollment_missing", F.col("enrollment_date").isNull())
          .withColumn("dq_invalid_tier", ~F.col("tier_code").isin(VALID_TIERS))
          .withColumn("dq_flight_before_enrollment",
                      F.col("flight_date").isNotNull() &
                      F.col("enrollment_date").isNotNull() &
                      (F.col("flight_date") < F.col("enrollment_date")))
          .withColumn("dq_future_dob",
                      F.col("dob").isNotNull() &
                      (F.col("dob") > F.to_date(F.lit(as_of_date))))
    )


def latest_record_wins(df):
    w = Window.partitionBy("member_id").orderBy(
        F.col("enrollment_date").desc_nulls_last(),
        F.col("flight_date").desc_nulls_last(),
        F.col("ingestion_ts").desc()
    )
    return (
        df.withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .drop("rn")
    )


def run(input_path: str, country: str, as_of_date: str, output_path: str | None = None):
    spark = build_spark()
    try:
        raw = (
            spark.read.option("header", True).option("inferSchema", False)
            .csv(input_path)
        )

        if country == "USA":
            staged = parse_usa(raw)
        elif country == "AUS":
            staged = parse_aus(raw)
        elif country == "IND":
            staged = parse_india(raw)
        else:
            raise ValueError("country must be USA, AUS or IND")

        staged = (
            staged.withColumn("ingestion_ts", F.current_timestamp())
                  .withColumn("batch_id", F.lit(as_of_date.replace("-", "")))
        )
        staged = standardize(staged, as_of_date)
        current = latest_record_wins(staged)

        valid = current.filter(
            ~(
                F.col("dq_member_id_missing") |
                F.col("dq_name_missing") |
                F.col("dq_enrollment_missing") |
                F.col("dq_invalid_tier") |
                F.col("dq_flight_before_enrollment") |
                F.col("dq_future_dob")
            )
        )
        rejected = current.filter(
            F.col("dq_member_id_missing") |
            F.col("dq_name_missing") |
            F.col("dq_enrollment_missing") |
            F.col("dq_invalid_tier") |
            F.col("dq_flight_before_enrollment") |
            F.col("dq_future_dob")
        )

        print("=== STAGING ===")
        staged.show(truncate=False)
        print("=== VALID COUNTRY TARGET ===")
        valid.show(truncate=False)
        print("=== QUARANTINE ===")
        rejected.show(truncate=False)

        if output_path:
            valid.write.mode("overwrite").partitionBy("country").parquet(output_path)

        return valid, rejected
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--country", required=True)
    parser.add_argument("--as-of-date", required=True)
    parser.add_argument("--output")
    args = parser.parse_args()
    run(args.input, args.country, args.as_of_date, args.output)
