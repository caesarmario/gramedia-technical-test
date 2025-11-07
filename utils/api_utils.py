####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## Utility functions for API extraction ops.
####

from __future__ import annotations

import requests

from typing import Any, Dict, Optional, Tuple
from requests.adapters import HTTPAdapter, Retry

class APIHelper:
    """
    Networking utilities for API calls.
    """

    @staticmethod
    def _session_with_retry(
        total: int = 3,
        backoff: float = 0.5,
        status_forcelist: Tuple[int, ...] = (429, 500, 502, 503, 504),
        allowed_methods: Tuple[str, ...] = ("GET",),
    ) -> requests.Session:
        retry = Retry(
            total=total,
            backoff_factor=backoff,
            status_forcelist=list(status_forlist := status_forcelist),
            allowed_methods=list(allowed_methods),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        s = requests.Session()
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        return s


    @staticmethod
    def get_json(
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Tuple[int, int] = (5, 30),
    ) -> Any:
        """
        Perform a GET request and return parsed JSON with retry & sane timeouts.
        """
        session = APIHelper._session_with_retry()
        resp = session.get(url, params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
