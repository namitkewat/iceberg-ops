"""
iceberg_ops.testing
===================
Public test helpers for tools that integrate with iceberg-ops.

Today the module exposes :class:`FakeSession` and
:class:`FakeResponse` - a lightweight stand-in for a
:class:`requests.Session` and :class:`requests.Response` pair. The
experimental iceberg-ops Polaris client
(:class:`~iceberg_ops.polaris.client.PolarisClient`) takes an injectable
``session=`` kwarg so that downstream consumers - and our own test
suite - can drive it against this in-process fake instead of a live
HTTP service. Its endpoint paths do not match the Polaris 1.x policy
API yet, so the fake is for exercising the client, not for modelling
a real Polaris server.

Usage
-----
::

    from iceberg_ops.testing import FakeResponse, FakeSession

    sess = FakeSession()
    sess.register("GET", "/policies", FakeResponse(json_body={"policies": []}))

    client = PolarisClient(
        "https://polaris.example.com",
        catalog="mycat",
        token="t",
        session=sess,
    )
    client.list_policies("ns")

    # Inspect what the client actually sent.
    assert sess.calls[0]["method"] == "GET"
    assert "/policies" in sess.calls[0]["url"]

Behaviour
---------
- :class:`FakeSession` records every ``request()`` / ``post()`` call
  in :attr:`FakeSession.calls`.
- Responses are matched by ``(method, path_substring)`` and consumed
  on first match (subsequent calls to the same endpoint require
  another :meth:`FakeSession.register`).
- An unmatched call raises :class:`AssertionError` so tests fail
  loudly rather than silently producing surprising output.

Stability
---------
Both classes are part of the documented public API. Their
attributes / methods follow semantic versioning: additions are
non-breaking; renames or removals are breaking and bump the major
version. Code outside this repository may rely on:

* :class:`FakeSession.__init__()` signature
* :meth:`FakeSession.register`, :meth:`FakeSession.request`,
  :meth:`FakeSession.post`, :attr:`FakeSession.calls`
* :class:`FakeResponse.__init__` keyword-only signature, plus the
  attributes ``status_code`` and ``text`` and the methods ``json``
  and ``raise_for_status``.
"""

from __future__ import annotations

import json as _json
from typing import Any


class FakeResponse:
    """Minimal stand-in for :class:`requests.Response`.

    Exposes the four attributes a typical REST client touches:
    ``status_code``, ``text``, :meth:`json`, and
    :meth:`raise_for_status` (the last is a no-op here, kept for
    forward-compatibility if a client adopts it).

    Parameters
    ----------
    status_code : int, optional
        HTTP status code. Default ``200``.
    json_body : Any, optional
        Body returned by :meth:`json`. ``None`` means the response
        carries no JSON body and :meth:`json` raises.
    text : str, optional
        Raw response text. Defaults to a JSON-serialised
        ``json_body`` when ``json_body`` is given, otherwise the
        empty string.
    """

    def __init__(
        self,
        *,
        status_code: int = 200,
        json_body: Any | None = None,
        text: str | None = None,
    ) -> None:
        self.status_code = status_code
        self._json_body = json_body
        self.text = (
            text if text is not None else (_json.dumps(json_body) if json_body is not None else "")
        )

    def json(self) -> Any:
        """Return the response body decoded as JSON.

        Raises
        ------
        ValueError
            If no ``json_body`` was supplied at construction.
        """
        if self._json_body is None:
            raise ValueError("response had no JSON body")
        return self._json_body

    def raise_for_status(self) -> None:
        """No-op stand-in for :meth:`requests.Response.raise_for_status`."""


class FakeSession:
    """Records every request and returns canned responses.

    Each call inspects the ``(method, path_substring)`` pair and
    returns the matching response. Tests register response sequences
    via :meth:`register`. Unmatched calls raise
    :class:`AssertionError` - keeps tests honest about which calls
    they expect.

    Attributes
    ----------
    calls : list[dict]
        Every call this session has serviced, in chronological
        order. Each dict carries the keys ``method``, ``url``, plus
        the per-call kwargs (``params``, ``json``, ``headers``,
        ``timeout`` for :meth:`request`; ``data`` and ``timeout``
        for :meth:`post`).
    """

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self._responses: list[tuple[str, str, FakeResponse]] = []

    def register(self, method: str, path_substring: str, response: FakeResponse) -> None:
        """Queue *response* for the next ``method``/``path_substring`` match.

        Each registration is consumed on first match. To service the
        same endpoint twice, call :meth:`register` twice.

        Parameters
        ----------
        method : str
            HTTP method (case-insensitive).
        path_substring : str
            Substring matched against the request URL. Useful when a
            client builds its own full URL from a base + path.
        response : FakeResponse
            The response to return when the next call matches.
        """
        self._responses.append((method.upper(), path_substring, response))

    def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> FakeResponse:
        """Mock for :meth:`requests.Session.request`.

        Records the call and returns the next registered response
        whose ``(method, path_substring)`` matches.

        Raises
        ------
        AssertionError
            When no registered response matches.
        """
        self.calls.append(
            {
                "method": method.upper(),
                "url": url,
                "params": params,
                "json": json,
                "headers": headers or {},
                "timeout": timeout,
            }
        )
        return self._consume(method.upper(), url)

    def post(
        self,
        url: str,
        *,
        data: Any = None,
        timeout: float | None = None,
    ) -> FakeResponse:
        """Mock for :meth:`requests.Session.post`.

        Some clients call ``session.post(url, data=...)`` directly
        for OAuth2 token requests instead of going through
        :meth:`request`. Both paths land here in the right place.
        """
        self.calls.append(
            {
                "method": "POST",
                "url": url,
                "data": data,
                "timeout": timeout,
            }
        )
        return self._consume("POST", url)

    def _consume(self, method: str, url: str) -> FakeResponse:
        """Return + consume the first registered response that matches."""
        for idx, (m, sub, response) in enumerate(self._responses):
            if m == method and sub in url:
                self._responses.pop(idx)
                return response
        raise AssertionError(f"unexpected {method} {url}; registered: {self._responses}")


__all__ = ["FakeResponse", "FakeSession"]
