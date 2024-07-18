from enum import IntEnum

class HTTPStatus(IntEnum):
    """An enumeration of HTTP status codes."""
    OK = 200
    TOO_MANY_REQUESTS = 429