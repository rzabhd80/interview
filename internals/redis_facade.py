import os
from collections import defaultdict
import pandas as pd
from redis import Redis

from exceptions.exception import RedisConnectionException


class RedisFacade:
    __redis_client = None

    def __init__(self) -> None:
        raise Exception("")

    @classmethod
    def setup(cls, host: str, port: int) -> None:
        try:
            RedisFacade.__redis_client = Redis(host=host, port=port)
        except Exception:
            raise RedisConnectionException

    @classmethod
    def redis_client(cls) -> Redis:
        return RedisFacade.__redis_client

    @classmethod
    def preload_dataset(cls, path: str) -> None:
        for chunk in pd.read_csv(path, header=None, chunksize=2500):
            country_cities = defaultdict(list)

            for _, row in chunk.iterrows():
                country_cities[str(row[0])].append(str(row[1]))

            with RedisFacade.__redis_client.pipeline() as pipe:
                for country, cities in country_cities.items():
                    pipe.sadd(country, *cities)
                pipe.execute()
