"""Explicit unsigned ODPS requests for a trusted credential-injecting egress."""

from email.utils import formatdate

from odps.accounts import BaseAccount


class ProxyAccount(BaseAccount):
    """Leave authentication to the egress; never resolve local credentials."""

    def sign_request(self, req, endpoint, region_name=None):
        # Reused prepared requests must not retain a previous account's auth.
        for name in ("Authorization", "x-odps-bearer-token", "x-odps-security-token",
                     "x-odps-app-authentication"):
            req.headers.pop(name, None)
        req.headers.setdefault("Date", formatdate(usegmt=True))
