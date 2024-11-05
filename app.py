import os

from flask import Flask
from dotenv import load_dotenv

from internals.redis_facade import RedisFacade
from internals.spark_cluster_facade import SparkClusterFacade
from services.api.revenue_analysis import router
from internals.celery_facade import CeleryFacade
from services.api.countries.router import create_country_service


def setup_application() -> Flask:
    load_dotenv()
    redis_host = os.getenv("REDIS_HOST")
    redis_port = os.getenv("REDIS_PORT")
    RedisFacade.setup(host=redis_host, port=int(redis_port))
    CeleryFacade.setup(redis_host=redis_host, redis_port=redis_port)
    RedisFacade.preload_dataset(path=f"./{os.getenv('COUNTRY_CITY_DATASET_PATH')}")

    SparkClusterFacade.setup_spark(spark_host=os.getenv("SPARK_HOST"), spark_port=os.getenv("SPARK_PORT"),
                                   minio_host=os.getenv("MINIO_HOST"),
                                   minio_port=os.getenv("MINIO_PORT"), minio_access_key=os.getenv("MINIO_AC_KEY"),
                                   minio_pass=os.getenv("MINIO_PASS"))

    app = Flask(__name__)
    country_router = create_country_service()
    revenue_router = router.create_revenue_analysis_service()
    app.register_blueprint(country_router, url_prefix="/country")
    app.register_blueprint(revenue_router, url_prefix="/analysis")
    return app


app = setup_application()
app.run()
