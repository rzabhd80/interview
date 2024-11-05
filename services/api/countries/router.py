from flask import Blueprint

from internals.redis_facade import RedisFacade
from services.api.countries.country_service import CountryService
from utils.global_error_handler import global_error_handler


def create_country_service() -> Blueprint:
    router = Blueprint("countries", __name__)
    redis_client = RedisFacade.redis_client()
    country_service = CountryService(redis_client=redis_client)

    @global_error_handler
    @router.route("/<string:country_code>", methods=['GET'])
    def get_cities(country_code: str):
        return country_service.get_country_cities(country_code)

    @global_error_handler
    @router.route("/", methods=['POST'])
    def set_city(body: dict):
        return country_service.store_country_city(data=body)

    return router
