from flask import jsonify
from redis import Redis


class CountryService:
    def __init__(self, redis_client: Redis):
        self.redis_client = redis_client

    def get_country_cities(self, country_code: str):
        cities = self.redis_client.smembers(country_code)
        return jsonify({country_code: list(cities)})

    def store_country_city(self, data: dict):
        country_code = data['country_code']
        cities = data['cities']
        duplicate = False
        if self.redis_client.exists(country_code):
            duplicate = True
        self.redis_client.sadd(country_code, *cities)
        return jsonify({"message": f"{'country and cities' if not duplicate else 'city'} added"})
