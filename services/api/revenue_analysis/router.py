from flask import Blueprint, request, jsonify

from internals.spark_cluster_facade import SparkClusterFacade
from services.api.revenue_analysis.revenue_analyzer_service import RevenueAnalyzerService
from utils.global_error_handler import global_error_handler


def create_revenue_analysis_service() -> Blueprint:
    router = Blueprint("revenue_analysis", __name__)
    spark_client = SparkClusterFacade.get_spark()
    minio_client = SparkClusterFacade.get_minio()

    analyzer_service = RevenueAnalyzerService()

    @global_error_handler
    @router.route("/analysis", method=['POST'])
    def data_analysis():
        task = analyzer_service.analyze_revenue.delay()

        return jsonify({
            'task_id': task.id,
            'status': 'Processing',
            'message': 'Analysis started successfully'
        }), 202

    return router
