import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, LongType


REDEMPTION_SCHEMA = StructType([
    StructField("member_id", StringType(), False),
    StructField("feed_date", StringType(), False),
    StructField("redemptions", ArrayType(
        StructType([
            StructField("txn_id", StringType(), False),
            StructField("txn_date", StringType(), False),
            StructField("partner", StringType(), True),
            StructField("miles_redeemed", LongType(), True),
            StructField("status", StringType(), True),
        ])
    ), True)
])


def build_spark():
    return SparkSession.builder.appName("SkyPoints-Redemption-Pipeline").master("local[*]").getOrCreate()


def flatten_redemptions(input_path: str):
    spark = build_spark()
    try:
        df = spark.read.schema(REDEMPTION_SCHEMA).json(input_path)
        return (
            df.select(
                "member_id",
                F.to_date("feed_date", "yyyyMMdd").alias("feed_date"),
                F.explode_outer("redemptions").alias("r")
            )
            .select(
                "member_id",
                "feed_date",
                F.col("r.txn_id").alias("txn_id"),
                F.to_date("r.txn_date", "yyyyMMdd").alias("txn_date"),
                F.col("r.partner").alias("partner"),
                F.col("r.miles_redeemed").alias("miles_redeemed"),
                F.upper(F.col("r.status")).alias("status"),
            )
        )
    except Exception:
        spark.stop()
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = build_spark()
    flattened = (
        spark.read.schema(REDEMPTION_SCHEMA).json(args.input)
        .select(
            "member_id",
            F.to_date("feed_date", "yyyyMMdd").alias("feed_date"),
            F.explode_outer("redemptions").alias("r")
        )
        .select(
            "member_id",
            "feed_date",
            F.col("r.txn_id").alias("txn_id"),
            F.to_date("r.txn_date", "yyyyMMdd").alias("txn_date"),
            F.col("r.partner").alias("partner"),
            F.col("r.miles_redeemed").alias("miles_redeemed"),
            F.upper(F.col("r.status")).alias("status"),
        )
    )
    flattened.write.mode("overwrite").partitionBy("feed_date").parquet(args.output)
    flattened.show(truncate=False)
    spark.stop()
