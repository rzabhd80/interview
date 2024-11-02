from internals.spark_cluster_facade import SparkClusterFacade
from services.api.revenue_analysis.analyzer import Analyzer


class RevenueAnalyzer:

    def __init__(self):
        self.analyzer = Analyzer(SparkClusterFacade.get_spark(),
                                 SparkClusterFacade.get_minio(), "analysis")


