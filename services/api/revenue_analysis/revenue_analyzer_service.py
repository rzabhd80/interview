import os

from internals.celery_facade import CeleryFacade
from internals.spark_cluster_facade import SparkClusterFacade
from services.api.revenue_analysis.analyzer import Analyzer


class RevenueAnalyzerService:

    def __init__(self):
        self.analyzer = Analyzer(SparkClusterFacade.get_spark(),
                                 SparkClusterFacade.get_minio(), "analysis")

    @CeleryFacade.celery_instance.task
    def analyze_revenue(self):
        dataset_path = os.getenv("REVENUE_DATASET_PATH")
        self.analyzer.calculate_revenue(dataset_path)
