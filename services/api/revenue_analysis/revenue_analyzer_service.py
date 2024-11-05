import os

from celery import Celery

from internals.celery_facade import CeleryFacade
from internals.spark_cluster_facade import SparkClusterFacade
from services.api.revenue_analysis.analyzer import Analyzer


class RevenueAnalyzerService:
    if not CeleryFacade.celery_instance:
        CeleryFacade.setup(redis_host=os.getenv("REDIS_HOST"), redis_port=os.getenv("REDIS_PORT"))

    @staticmethod
    @CeleryFacade.celery_instance.task
    def analyze_revenue():
        analyzer = Analyzer(SparkClusterFacade.get_spark(),
                            SparkClusterFacade.get_minio(), "analysis")
        dataset_path = os.getenv("REVENUE_DATASET_PATH")
        analyzer.calculate_revenue(dataset_path)


def create_revenue_analyzer_service() -> RevenueAnalyzerService:
    revenue = RevenueAnalyzerService()
    return revenue
