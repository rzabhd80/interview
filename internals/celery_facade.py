from celery import Celery


class CeleryFacade:
    __celery_instance = None

    def __init__(self):
        pass

    @classmethod
    def setup(cls, redis_host: str, redis_port: str):
        CeleryFacade.__celery_instance = Celery("analysis", broker=f'redis://{redis_host}:{redis_port}/0')

    @classmethod
    def get_celery(cls) -> Celery:
        return CeleryFacade.__celery_instance
