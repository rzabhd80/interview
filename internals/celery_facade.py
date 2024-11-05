from celery import Celery

from exceptions.exception import FacadeInstanceCreation


class CeleryFacade:
    celery_instance: Celery = None

    def __init__(self):
        raise FacadeInstanceCreation()

    @classmethod
    def setup(cls, redis_host: str, redis_port: str):
        cls.celery_instance = Celery("analysis", broker=f'redis://{redis_host}:{redis_port}/0')

    @classmethod
    def get_celery(cls) -> Celery:
        return CeleryFacade.celery_instance
