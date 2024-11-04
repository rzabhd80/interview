import os

from celery import Celery

from internals.celery_facade import CeleryFacade
from internals.spark_cluster_facade import SparkClusterFacade
from services.api.revenue_analysis.analyzer import Analyzer


class CeleryTaskWrapper:
    celery_instance: Celery

    def __init__(self):
        pass

    @classmethod
    def apply(cls, celery: Celery):
        cls.celery_instance = celery


class RevenueAnalyzerService:

    def __init__(self, celery_instance: Celery):
        self.analyzer = Analyzer(SparkClusterFacade.get_spark(),
                                 SparkClusterFacade.get_minio(), "analysis")
        CeleryTaskWrapper.apply(celery_instance)

    @CeleryTaskWrapper.celery_instance.task
    def analyze_revenue(self):
        dataset_path = os.getenv("REVENUE_DATASET_PATH")
        self.analyzer.calculate_revenue(dataset_path)
