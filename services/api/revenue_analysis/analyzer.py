from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, expr
from pyspark.sql.functions import (
    col, date_format, round, sum, max, min, count,
    to_timestamp, expr, floor
)

from pyspark.sql import functions as func
from pyspark.sql.dataframe import DataFrame


class Analyzer:
    def __init__(self, spark_client: SparkSession, minio_client: Minio, bucket_name: str):
        self.spark_client = spark_client
        self.minio_client = minio_client
        self.bucket_name = bucket_name
        self.minio_url = f"s3a://{self.bucket_name}/sms_analysis"
        self.pay_type = None

    def __load_dataset(self, csv_path: str) -> DataFrame:
        data_frame = self.spark_client.read.csv(csv_path, header=True)
        data_frame = data_frame.withColumn(
            "RECORD_DATE",
            to_timestamp("RECORD_DATE", "yyyy/MM/dd HH:mm:ss")
        ).withColumn(
            "interval_15min",
            expr(
                "date_format(date_trunc('MINUTE', RECORD_DATE) - MINUTE(RECORD_DATE) % 15 * \\"
                "INTERVAL '1' MINUTE, 'yyyy-MM-dd HH:mm')")
        )
        self.pay_type = self.spark_client.createDataFrame([
            (0, "Prepaid"),
            (1, "Postpaid")
        ], ["PayType", "value"])

        return data_frame

    def __store_original_debit(self, dataframe: DataFrame):
        dataframe.select("DEBIT_AMOUNT_42").write.csv(
            f"{self.minio_url}/money_amount.csv",
            header=True,
            mode="overwrite"
        )

    def __calculate_store_revenue_by_pay_type_interval(self, dataframe: DataFrame):
        revenue_by_paytype = dataframe.groupBy("interval_15min", "PAYTYPE_515").agg(
            round(sum(col("DEBIT_AMOUNT_42").cast("double")) / 10000, 2).alias("income_tomans")
        ).join(self.pay_type, col("PAYTYPE_515") == col("PayType")).select(
            "interval_15min",
            "PAYTYPE_515",
            "income_tomans",
            "value"
        ).orderBy("interval_15min", "PAYTYPE_515")

        revenue_by_paytype.write.csv(
            f"{self.minio_url}/revenue_by_pay_type_15min.csv",
            header=True,
            mode="overwrite"
        )

    def __max_min_income_calculation_interval(self, dataframe: DataFrame):
        interval_revenue = dataframe.groupBy("interval_15min", "PAYTYPE_515").agg(
            round(sum(col("DEBIT_AMOUNT_42").cast("double")) / 10000, 2).alias("income_tomans")
        ).groupBy("PAYTYPE_515").agg(
            max("income_tomans").alias("max_income"),
            min("income_tomans").alias("min_income")
        ).join(self.pay_type, col("PAYTYPE_515") == col("PayType")).select(
            "PAYTYPE_515",
            "max_income",
            "min_income",
            "value"
        ).orderBy("PAYTYPE_515")

        interval_revenue.write.csv(
            f"{self.minio_url}/max_min_by_pay_type.csv",
            header=True,
            mode="overwrite"
        )

    def __revenue_pay_type_record_count_interval(self, dataframe: DataFrame):
        revenue_count_by_pay_type = dataframe.groupBy("interval_15min", "PAYTYPE_515").agg(
            round(sum(col("DEBIT_AMOUNT_42").cast("double")) / 10000, 2).alias("income_tomans"),
            count("*").alias("Record_Count")
        ).join(self.pay_type, col("PAYTYPE_515") == col("PayType")).select(
            "interval_15min",
            "PAYTYPE_515",
            "income_tomans",
            "Record_Count",
            "value"
        ).orderBy("interval_15min", "PAYTYPE_515")

        revenue_count_by_pay_type.write.csv(
            f"{self.minio_url}/revenue_count_by_pay_type.csv",
            header=True,
            mode="overwrite"
        )

    def calculate_revenue(self, dataset_path: str):
        print("method calculate called")
        dataframe = self.__load_dataset(dataset_path)
        self.__store_original_debit(dataframe)
        self.__calculate_store_revenue_by_pay_type_interval(dataframe)
        self.__max_min_income_calculation_interval(dataframe)
        self.__revenue_pay_type_record_count_interval(dataframe)
