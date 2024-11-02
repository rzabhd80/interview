from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, DoubleType, TimestampType, IntegerType, StructType
from pyspark.sql import functions as func
from pyspark.sql.dataframe import DataFrame


class Analyzer:
    def __init__(self, spark_client: SparkSession, minio_client: Minio, bucket_name: str):
        self.spark_client = spark_client
        self.minio_client = minio_client
        self.bucket_name = bucket_name
        self.minio_url = f"s3a://{self.bucket_name}/sms_analysis"

    def __load_dataset(self, csv_path: str) -> DataFrame:
        sms_schema = StructType([
            StructField("ROAMSTATE_519", StringType(), True),
            StructField("CUST_LOCAL_START_DATE_15", TimestampType(), True),
            StructField("CDR_ID_1", StringType(), True),
            StructField("CDR_SUB_ID_2", StringType(), True),
            StructField("CDR_TYPE_3", StringType(), True),
            StructField("SPLIT_CDR_REASON_4", StringType(), True),
            StructField("RECORD_DATE", TimestampType(), True),
            StructField("PAYTYPE_515", IntegerType(), True),
            StructField("DEBIT_AMOUNT_42", DoubleType(), True),  # This is in mili rials
            StructField("SERVICEFLOW_498", StringType(), True),
            StructField("EVENTSOURCE_CATE_17", StringType(), True),
            StructField("USAGE_SERVICE_TYPE_19", StringType(), True),
            StructField("SPECIALNUMBERINDICATOR_534", StringType(), True),
            StructField("BE_ID_30", StringType(), True),
            StructField("CALLEDPARTYIMSI_495", StringType(), True),
            StructField("CALLINGPARTYIMSI_494", StringType(), True)
        ])

        data_frame = self.spark_client.read.csv(csv_path, schema=sms_schema, header=True)
        data_frame = data_frame.withColumn("revenue_tomans", func.col("DEBIT_AMOUNT_42") / func.lit(10000))
        data_frame = data_frame.withColumn(
            "interval_15min",
            func.date_format(func.date_trunc("15 minutes", "RECORD_DATE"), "HH:mm:ss yyyy/MM/dd")
        )
        return data_frame

    def __store_original_debit(self, dataframe: DataFrame):
        dataframe.select("DEBIT_AMOUNT_42").write.csv(
            f"{self.minio_url}/money_amount.csv",
            header=True,
            mode="overwrite"
        )

    def __calculate_store_revenue_by_pay_type_interval(self, dataframe: DataFrame):
        revenue_by_paytype = dataframe.groupBy(
            "interval_15min",
            "PAYTYPE_515"
        ).agg(
            func.sum("revenue_tomans").alias("revenue")
        ).orderBy("interval_15min", "PAYTYPE_515")

        revenue_by_paytype.write.csv(
            f"{self.minio_url}/revenue_by_pay_type_15min.csv",
            header=True,
            mode="overwrite"
        )

    def __max_min_revenue_pay_type_interval(self, dataframe: DataFrame):
        interval_revenue = dataframe.groupBy(
            "interval_15min",
            "PAYTYPE_515"
        ).agg(
            func.sum("revenue_tomans").alias("interval_revenue")
        )

        max_min_by_paytype = interval_revenue.groupBy(
            "PAYTYPE_515"
        ).agg(
            func.max("interval_revenue").alias("max_revenue"),
            func.min("interval_revenue").alias("min_revenue")
        ).orderBy("PAYTYPE_515")

        max_min_by_paytype.write.csv(
            f"{self.minio_url}/max_min_by_pay_type.csv",
            header=True,
            mode="overwrite"
        )

    def __revenue_pay_type_record_interval(self, dataframe: DataFrame):
        revenue_count_by_pay_type = dataframe.groupBy(
            "interval_15min",
            "PAYTYPE_515"
        ).agg(
            func.count("*").alias("Record_Count"),
            func.sum("revenue_tomans").alias("revenue")
        ).orderBy("interval_15min", "PAYTYPE_515")

        revenue_count_by_pay_type.write.csv(
            f"{self.minio_url}/revenue_count_by_pay_type.csv",
            header=True,
            mode="overwrite"
        )

    def calculate_revenue(self, dataset_path: str):
        dataframe = self.__load_dataset(dataset_path)
        self.__store_original_debit(dataframe)
        self.__calculate_store_revenue_by_pay_type_interval(dataframe)
        self.__max_min_revenue_pay_type_interval(dataframe)
        self.__revenue_pay_type_record_interval(dataframe)
