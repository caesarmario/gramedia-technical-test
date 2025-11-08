####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## Utility functions for API extraction ops.
####

from __future__ import annotations

import requests
from typing import Any, Dict, Optional, Tuple
from requests.adapters import HTTPAdapter, Retry

from utils.logging_utils import logger


class APIHelper:
    """
    Networking utilities for API calls.

    This helper keeps HTTP concerns (session/retry/timeouts) in one place so
    extractors remain small. Only GET JSON is implemented here because the
    FakeStore extractors do read-only pulls.
    """

    @staticmethod
    def _session_with_retry(
        total: int = 3,
        backoff: float = 0.5,
        status_forcelist: Tuple[int, ...] = (429, 500, 502, 503, 504),
        allowed_methods: Tuple[str, ...] = ("GET",),
    ) -> requests.Session:
        """
        Build a requests.Session configured with sane retry/backoff settings.

        Args:
            total: Max total retry attempts (across all retryable failures).
            backoff: Exponential backoff factor (e.g., 0.5 → 0.5, 1, 2... seconds).
            status_forcelist: HTTP codes that should trigger a retry.
            allowed_methods: HTTP methods that are retryable.

        Returns:
            A requests.Session with mounted HTTPAdapter that handles retries.
        """
        try:
            logger.debug(
                "Creating retry session | total=%s backoff=%s statuses=%s methods=%s",
                total, backoff, status_forcelist, allowed_methods,
            )

            retry = Retry(
                total=total,
                backoff_factor=backoff,
                status_forcelist=list(status_forlist := status_forcelist),  # keep original pattern
                allowed_methods=list(allowed_methods),
                raise_on_status=False,
            )
            adapter = HTTPAdapter(max_retries=retry)

            s = requests.Session()
            s.mount("https://", adapter)
            s.mount("http://", adapter)

            logger.debug("Retry session created successfully")
            return s

        except Exception as e:
            # Log and re-raise: caller should fail fast if we cannot create a session.
            logger.exception("Failed to create retry-enabled session: %s", e)
            raise


    @staticmethod
    def get_json(
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Tuple[int, int] = (5, 30),
    ) -> Any:
        """
        Perform a GET request with retry and return parsed JSON.

        Args:
            url: Absolute URL to fetch.
            params: Optional querystring parameters.
            headers: Optional request headers (e.g., auth).
            timeout: (connect_timeout, read_timeout) in seconds.

        Returns:
            Parsed JSON payload (dict/list/primitive) from the response.

        Raises:
            requests.RequestException: On network/HTTP issues after retries.
            ValueError: If the response cannot be parsed as JSON.
        """
        logger.info("HTTP GET → %s | params=%s", url, params)

        try:
            session = APIHelper._session_with_retry()
        except Exception:
            # Session creation already logged; bubble up as-is.
            raise

        try:
            resp = session.get(url, params=params, headers=headers, timeout=timeout)
            logger.debug("Response received | status=%s url=%s", resp.status_code, resp.url)

            # Raise for 4xx/5xx after retry logic has already been applied by adapter
            resp.raise_for_status()

        except requests.RequestException as http_err:
            # Capture status code/body when available to aid debugging
            status = getattr(http_err.response, "status_code", None)
            text = getattr(http_err.response, "text", "")
            logger.error(
                "HTTP error while requesting %s | status=%s body=%s err=%s",
                url, status, (text[:500] + "…") if text and len(text) > 500 else text, http_err,
            )
            raise

        try:
            payload = resp.json()
            logger.debug("JSON parsed successfully (%s bytes)", len(resp.content))
            return payload
        except ValueError as json_err:
            # JSON parsing failed (invalid/malformed content)
            logger.error(
                "Failed to parse JSON from %s | status=%s body_head=%r err=%s",
                url, resp.status_code, resp.text[:200], json_err,
            )
            raise
