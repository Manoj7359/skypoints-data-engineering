from pyspark.sql import functions as F

VALID_TIERS = ("PLT", "GLD", "SLV")
VALID_COUNTRIES = ("USA", "AUS", "IND")
VALID_REDEMPTION_STATUSES = ("COMPLETED", "PENDING", "CANCELLED")


def add_audit_columns(df, batch_id: str):
    return (
        df.withColumn("batch_id", F.lit(batch_id))
          .withColumn("ingestion_ts", F.current_timestamp())
    )
