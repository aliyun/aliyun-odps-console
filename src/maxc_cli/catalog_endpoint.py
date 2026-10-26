"""Normalize SDK-discovered Catalog authorities without changing explicit routing."""

import re
from urllib.parse import urlsplit

from .exceptions import ValidationError

_BARE_AUTHORITY = re.compile(
    r"(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+"
    r"[A-Za-z0-9-]+(?::[0-9]{1,5})?"
)


def normalize_catalog_endpoint(endpoint):
    if not isinstance(endpoint, str) or not endpoint:
        raise ValidationError("Catalog endpoint must be a nonempty HTTP(S) service URL.")
    endpoint = endpoint.rstrip("/")
    if _BARE_AUTHORITY.fullmatch(endpoint):
        endpoint = "https://" + endpoint
    try:
        parsed = urlsplit(endpoint)
        valid = (
            parsed.scheme in ("https", "http")
            and parsed.hostname
            and not parsed.username
            and not parsed.password
            and not parsed.query
            and not parsed.fragment
            and not any(c.isspace() for c in endpoint)
            and "\\" not in endpoint
            and "%" not in parsed.hostname
        )
        # Accessing port checks malformed/non-numeric/out-of-range values.
        if parsed.port is not None and not 1 <= parsed.port <= 65535:
            valid = False
    except ValueError:
        valid = False
    if not valid:
        raise ValidationError(
            "Catalog endpoint must be an HTTP(S) service URL without credentials, query, or fragment."
        )
    return endpoint
