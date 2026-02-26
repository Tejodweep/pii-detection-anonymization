"""
spark_utils.py
Helper to create or retrieve a SparkSession.
"""

from pyspark.sql import SparkSession


def get_or_create_spark(app_name: str = "PII_Detection_Anonymization") -> SparkSession:
    """
    Return an existing SparkSession or create a new one.

    For local development a minimal config is applied.
    In a cluster environment (Databricks, EMR, etc.) the session
    is already available via SparkSession.builder.getOrCreate().

    Args:
        app_name: Name shown in the Spark UI.

    Returns:
        A SparkSession instance.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Local mode with all available cores — remove for cluster deployments
        .master("local[*]")
        # Suppress verbose INFO logs during development
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark