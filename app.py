import os

from flask import Flask
from dotenv import load_dotenv

from internals.redis_facade import RedisFacade
from internals.spark_cluster_facade import SparkClusterFacade
from internals.celery_facade import CeleryFacade
from services.api.countries.router import router
from services.api.revenue_analysis import router as revenue_router

redis_host = os.getenv("REDIS_HOST")
redis_port = os.getenv("REDIS_PORT")
RedisFacade.setup(host=redis_host, port=int(redis_port))
RedisFacade.preload_dataset(path=f"./{os.getenv('COUNTRY_CITY_DATASET_PATH')}")

SparkClusterFacade.setup_spark(spark_host=os.getenv("SPARK_HOST"), spark_port=os.getenv("SPARK_PORT"),
                               minio_host=os.getenv("MINIO_AC_KEY"),
                               minio_port=os.getenv("MINIO_PORT"), minio_access_key=os.getenv("MINIO_AC_KEY"),
                               minio_pass=os.getenv("MINIO_PASS"))

CeleryFacade.setup(redis_host=redis_host, redis_port=redis_port)

load_dotenv()

app = Flask(__name__)

app.register_blueprint(router, url_prefix="/country")
app.register_blueprint()

if __name__ == "__main__":
    app.run()
