from flask import Blueprint

from internals.redis_facade import RedisFacade
from services.api.countries.country_service import CountryService

router = Blueprint("countries", __name__)
redis_client = RedisFacade.redis_client()
country_service = CountryService(redis_client=redis_client)


@router.get("/<str:country_code>", methods=['GET'])
def get_cities(country_code: str):
    return country_service.get_country_cities(country_code)


@router.post("/", methods=['POST'])
def set_city(body: dict):
    return country_service.store_country_city(data=body)
