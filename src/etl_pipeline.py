"""
House Sale Data ETL Pipeline
============================
Implement the three functions below to complete the ETL pipeline.

Steps:
  1. EXTRACT   – load the CSV into a PySpark DataFrame
  2. TRANSFORM – split the data by neighborhood and save each as a separate CSV
  3. LOAD      – insert each neighborhood DataFrame into its own PostgreSQL table
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def extract(spark: SparkSession, csv_path: str) -> DataFrame:
    """
    Extract step:
    Load the CSV file into a PySpark DataFrame.
    """
    df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)
    return df


def transform(df: DataFrame, output_dir: str) -> dict[str, DataFrame]:
    """
    Transform step:
    Split the data by neighborhood and save each neighborhood as a separate CSV file.

    Returns:
        A dictionary where:
          key   = cleaned neighborhood name
          value = filtered DataFrame for that neighborhood
    """
    os.makedirs(output_dir, exist_ok=True)

    possible_cols = ["neighborhood", "Neighborhood", "NEIGHBORHOOD"]
    neighborhood_col = next((c for c in possible_cols if c in df.columns), None)

    if neighborhood_col is None:
        raise ValueError(
            f"Could not find a neighborhood column. Available columns: {df.columns}"
        )

    df = df.filter(F.col(neighborhood_col).isNotNull())

    neighborhoods = [
        row[neighborhood_col]
        for row in df.select(neighborhood_col).distinct().collect()
    ]

    neighborhood_dfs: dict[str, DataFrame] = {}

    for neighborhood in neighborhoods:
        safe_name = re.sub(
            r"[^a-zA-Z0-9_]",
            "_",
            str(neighborhood).strip().lower().replace(" ", "_"),
        )

        neighborhood_df = df.filter(F.col(neighborhood_col) == neighborhood)
        neighborhood_dfs[safe_name] = neighborhood_df

        temp_dir = os.path.join(output_dir, f"_{safe_name}_tmp")
        final_csv = os.path.join(output_dir, f"{safe_name}.csv")

        # Write one Spark part file
        (
            neighborhood_df.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(temp_dir)
        )

        # Find the generated part file and rename it
        for file_name in os.listdir(temp_dir):
            if file_name.startswith("part-") and file_name.endswith(".csv"):
                os.replace(
                    os.path.join(temp_dir, file_name),
                    final_csv,
                )
                break

        # Remove Spark metadata/temp files
        for file_name in os.listdir(temp_dir):
            file_path = os.path.join(temp_dir, file_name)
            if os.path.isfile(file_path):
                os.remove(file_path)
        os.rmdir(temp_dir)

    return neighborhood_dfs

def load(
    neighborhood_dfs: dict[str, DataFrame],
    jdbc_url: str,
    connection_properties: dict[str, str],
) -> None:
    """
    Load step:
    Insert each neighborhood DataFrame into its own PostgreSQL table.
    """
    for table_name, neighborhood_df in neighborhood_dfs.items():
        (
            neighborhood_df.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode="overwrite",
                properties=connection_properties,
            )
        )


def main() -> None:
    load_dotenv()

    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "house_sales")
    db_user = os.getenv("DB_USER", "joannaloja")
    db_password = os.getenv("DB_PASSWORD", "")

    csv_path = "dataset/historical_purchases.csv"
    output_dir = "output/by_neighborhood"

    spark = (
        SparkSession.builder.appName("HouseSaleETLPipeline")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )

    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    connection_properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver",
    }

    try:
        # EXTRACT
        df = extract(spark, csv_path)

        # TRANSFORM
        neighborhood_dfs = transform(df, output_dir)

        # LOAD only if credentials are present
        if db_password.strip():
            load(neighborhood_dfs, jdbc_url, connection_properties)
            print("PostgreSQL load completed.")
        else:
            print("Skipping PostgreSQL load because DB_PASSWORD is not set.")

        print("ETL pipeline completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
