from flask import Blueprint

from internals.spark_cluster_facade import SparkClusterFacade

router = Blueprint("revenue_analysis", __name__)
spark_client = SparkClusterFacade.get_spark()
minio_client = SparkClusterFacade.get_minio()


@router.get("/analysis", method=['POST'])
def data_analysis():
    pass
