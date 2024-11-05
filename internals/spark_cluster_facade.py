from minio import Minio
from pyspark.sql import SparkSession
from exceptions.exception import SparkConnectionException, FacadeInstanceCreation
from os import environ


class SparkClusterFacade:
    __spark = None
    __minio_client = None

    def __init__(self) -> None:
        raise FacadeInstanceCreation()

    @classmethod
    def setup_spark(cls, spark_host: str, spark_port: str, minio_host: str, minio_port: str, minio_access_key: str,
                    minio_pass: str) -> None:
        SparkClusterFacade.__minio_client = Minio(endpoint=f"{minio_host}:{minio_port}",
                                                  access_key=minio_access_key,
                                                  secret_key=minio_pass, secure=False)
        if not SparkClusterFacade.__minio_client.bucket_exists("analysis"):
            SparkClusterFacade.__minio_client.make_bucket("analysis")
        SparkClusterFacade.__spark = SparkSession.builder.appName("analysis").master(
            f'spark://{spark_host}:{spark_port}') \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_host}:{minio_port}") \
            .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", minio_pass) \
            .config("spark.hadoop.fs.s3a.path.style.access", True) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        try:
            SparkClusterFacade.__spark = SparkClusterFacade.__spark.getOrCreate()
        except Exception:
            raise SparkConnectionException()

    @classmethod
    def get_spark(cls) -> SparkSession:
        return SparkClusterFacade.__spark

    @classmethod
    def get_minio(cls) -> Minio:
        return SparkClusterFacade.__minio_client
