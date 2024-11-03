from typing import Callable

from flask import jsonify
from loguru import logger


def global_error_handler(func: Callable) -> Callable:
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error triggering analysis: {str(e)}")
            return jsonify({
                'error': 'Failed to start analysis',
                'message': str(e)
            }), 500
