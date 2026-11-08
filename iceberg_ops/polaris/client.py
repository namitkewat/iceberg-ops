"""
iceberg_ops.polaris.client
==========================
Thin HTTP client for Apache Polaris policy endpoints.

Experimental
------------
The endpoint paths this client builds have not been verified against
a live Polaris server and do **not** match the Polaris 1.x policy API
(``/api/catalog/polaris/v1/{prefix}/...``). Fetching policies from a
real Polaris server therefore fails with :class:`PolarisError` in
this release. The client is kept so the policy pipeline can be driven
end to end against :class:`iceberg_ops.testing.FakeSession`.

Auth
----
The client supports two ways to provide auth:

* ``token=<bearer>`` - a pre-fetched bearer token. Use this for
  external / OIDC identity providers: fetch the token yourself and
  pass it via ``token=`` (or ``--polaris-token`` on the CLI).
* ``oauth2_credentials=(client_id, client_secret)`` - the client
  fetches a token with the client-credentials grant from
  ``base_url + token_endpoint`` (default
  ``/api/catalog/v1/oauth/tokens``, Polaris's internal token endpoint;
  still served by Polaris 1.8.0 but deprecated in the Iceberg REST
  spec) on first use.

Either way the token is cached for the lifetime of the client. There
is **no** refresh on ``401`` or on ``expires_in``. Polaris's default
token lifetime is one hour (``PT1H``), so create one client per run.

Lazy ``requests`` import
------------------------
``requests`` is imported inside the methods that hit the network so
importing this module without the ``[polaris]`` extra installed does
not blow up - only methods that actually make HTTP calls touch
``requests``. Tests inject a session via the ``session=`` kwarg
(typically :class:`iceberg_ops.testing.FakeSession`) and skip the
``requests`` import entirely.

Endpoint paths
--------------
Policy requests go to ``base_url + management_prefix`` (default
``/api/management/v1``) followed by
``/catalogs/{catalog}/namespaces/{ns}/...``. These paths are
unverified and differ from the Polaris 1.x policy API; see
*Experimental* above.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# Default OAuth2 scope used by Polaris. Lets every principal role granted
# to the principal act on its grants.
_DEFAULT_OAUTH_SCOPE = "PRINCIPAL_ROLE:ALL"


class PolarisError(Exception):
    """Raised on HTTP failures or unparseable responses from Polaris.

    The exception carries the HTTP status code (when one is
    available) and the response body so callers can surface useful
    diagnostics. ``status_code`` is ``None`` when the failure
    happened before a response was received (DNS failure, timeout,
    refused connection).

    Attributes
    ----------
    status_code : int or None
        HTTP status code from the response, or ``None`` when no
        response was received.
    body : str or None
        Truncated response body for diagnostics, or ``None`` when no
        response was received.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        body: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class PolarisClient:
    """Thin HTTP client for Polaris policy endpoints (experimental).

    Experimental: the endpoint paths are unverified against a live
    Polaris server and do not match the Polaris 1.x policy API, so
    calls against a real server fail with :class:`PolarisError`. The
    bearer token (static or OAuth2-fetched) is cached for the client's
    lifetime with no refresh; create one client per run.

    Parameters
    ----------
    base_url : str
        Polaris host, e.g. ``"https://polaris.example.com"``. No
        trailing slash required; the client normalises it.
    catalog : str
        Polaris catalog name. All policy endpoints are scoped to a
        catalog, so this is bound at construction time rather than
        per-call.
    token : str, optional
        Pre-fetched bearer token. Mutually exclusive with
        ``oauth2_credentials``. Useful for tests and ad-hoc
        invocations. Surrounding whitespace (for example a trailing
        CR/LF read from a file) is stripped.
    oauth2_credentials : tuple of (str, str), optional
        ``(client_id, client_secret)``. The client fetches a token
        via OAuth2 client-credentials grant on first use.
    oauth2_scope : str, optional
        OAuth2 scope to request. Default ``"PRINCIPAL_ROLE:ALL"``
        matches Polaris's standard principal-role flow.
    token_endpoint : str, optional
        Path on ``base_url`` to the OAuth2 token endpoint. Default
        ``"/api/catalog/v1/oauth/tokens"`` (Polaris internal auth).
    management_prefix : str, optional
        Path prefix for the policy endpoints. Default
        ``"/api/management/v1"`` (unverified; see the module notes).
    timeout : float, optional
        Per-request timeout in seconds. Default 30.
    session : requests.Session-like, optional
        Inject a custom session for connection pooling or testing.
        Anything with the same ``request`` / ``post`` interface as
        :class:`requests.Session` will work; tests use
        :class:`iceberg_ops.testing.FakeSession` here.

    Raises
    ------
    ValueError
        If neither ``token`` nor ``oauth2_credentials`` is provided,
        or if both are, or if ``token`` is empty or contains
        whitespace or control characters after stripping. The message
        never includes the token.
    ImportError
        At first HTTP call if ``requests`` is not installed (i.e.
        the user installed the base package without ``[polaris]``)
        and no ``session`` was supplied.
    """

    def __init__(
        self,
        base_url: str,
        catalog: str,
        *,
        token: str | None = None,
        oauth2_credentials: tuple[str, str] | None = None,
        oauth2_scope: str = _DEFAULT_OAUTH_SCOPE,
        token_endpoint: str = "/api/catalog/v1/oauth/tokens",
        management_prefix: str = "/api/management/v1",
        timeout: float = 30.0,
        session: Any = None,
    ) -> None:
        if (token is None) == (oauth2_credentials is None):
            raise ValueError(
                "PolarisClient requires exactly one of `token` or "
                "`oauth2_credentials`; got "
                f"token={token is not None}, "
                f"oauth2_credentials={oauth2_credentials is not None}."
            )
        if token is not None:
            token = token.strip()
            if not _is_header_safe(token):
                # Never echo the value: it is a credential.
                raise ValueError("token is empty or contains whitespace or control characters")

        self.base_url = base_url.rstrip("/")
        self.catalog = catalog
        self._static_token = token
        self._oauth2_credentials = oauth2_credentials
        self._oauth2_scope = oauth2_scope
        self._token_endpoint = token_endpoint
        self._management_prefix = management_prefix.rstrip("/")
        self._timeout = timeout
        self._session = session
        # Cached bearer token (either the static one or the OAuth2-fetched
        # one). Resolved lazily on the first authenticated call.
        self._cached_token: str | None = token

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _ensure_session(self) -> Any:
        """Lazy-build the HTTP session (or return the injected one).

        Importing ``requests`` here lets the module load cleanly when
        the ``[polaris]`` extra is missing - only callers who actually
        hit the network see the ImportError.
        """
        if self._session is not None:
            return self._session
        try:
            import requests  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "PolarisClient requires the `requests` package. "
                "Install it via `pip install 'iceberg-ops[polaris]'`."
            ) from exc
        self._session = requests.Session()
        return self._session

    def _secrets(self) -> tuple[str | None, ...]:
        """Return every credential value an error message must not show."""
        client_secret = self._oauth2_credentials[1] if self._oauth2_credentials else None
        return (self._cached_token, client_secret)

    def _ensure_token(self) -> str:
        """Return a bearer token, fetching one via OAuth2 if needed."""
        if self._cached_token is not None:
            return self._cached_token
        # Must be the oauth2 path - the constructor enforces exactly
        # one of token / oauth2_credentials.
        assert self._oauth2_credentials is not None
        client_id, client_secret = self._oauth2_credentials
        url = self.base_url + self._token_endpoint
        session = self._ensure_session()
        # Polaris accepts client_id / client_secret as form fields;
        # this avoids the basic-auth header subtlety of base64
        # encoding plus characters that can confuse some proxies.
        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": self._oauth2_scope,
        }
        try:
            response = session.post(url, data=data, timeout=self._timeout)
        except Exception as exc:  # connection failure, timeout, etc.
            # ``from None`` keeps the raw exception, which may carry the
            # request, out of tracebacks; the message is scrubbed.
            raise PolarisError(
                f"Failed to reach Polaris token endpoint at {url}: "
                f"{_describe(exc, (client_secret,))}"
            ) from None
        if not _ok(response):
            raise PolarisError(
                f"OAuth2 token request to {url} failed",
                status_code=getattr(response, "status_code", None),
                body=_body(response),
            )
        # The token response carries secrets, so error messages below
        # report its shape only - never its values.
        try:
            payload = response.json()
        except ValueError as exc:
            text = getattr(response, "text", None) or ""
            raise PolarisError(
                "OAuth2 token endpoint returned a non-JSON body "
                f"(status {getattr(response, 'status_code', '?')}, {len(str(text))} chars)"
            ) from exc
        if not isinstance(payload, dict):
            raise PolarisError(
                f"OAuth2 token endpoint returned a JSON {type(payload).__name__}, expected an object"
            )
        access_token = payload.get("access_token")
        if not access_token:
            raise PolarisError(
                "OAuth2 token endpoint response missing 'access_token' "
                f"(keys: {sorted(payload)}; error={payload.get('error')!r}; "
                f"error_description={payload.get('error_description')!r})"
            )
        if not _is_header_safe(str(access_token)):
            raise PolarisError(
                "OAuth2 token endpoint returned an 'access_token' containing "
                "whitespace or control characters"
            )
        self._cached_token = str(access_token)
        return self._cached_token

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
    ) -> Any:
        """Issue an authenticated request and return the parsed JSON body.

        ``path`` is appended to ``self.base_url + self._management_prefix``,
        so callers pass relative paths like ``/catalogs/foo/...``.
        """
        token = self._ensure_token()
        session = self._ensure_session()
        url = self.base_url + self._management_prefix + path
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        try:
            response = session.request(
                method,
                url,
                params=params,
                json=json_body,
                headers=headers,
                timeout=self._timeout,
            )
        except Exception as exc:
            # The Authorization header can appear in the exception text
            # (requests' InvalidHeader quotes it), so scrub the message
            # and keep the raw exception out of tracebacks.
            raise PolarisError(
                f"HTTP {method} {url} failed before response: {_describe(exc, self._secrets())}"
            ) from None
        if not _ok(response):
            raise PolarisError(
                f"Polaris {method} {url} returned {getattr(response, 'status_code', '?')}",
                status_code=getattr(response, "status_code", None),
                body=_body(response),
            )
        # 204 No Content is legitimate for some calls.
        if getattr(response, "status_code", None) == 204:
            return None
        try:
            return response.json()
        except ValueError as exc:
            raise PolarisError(
                f"Polaris {method} {url} returned non-JSON body: {_body(response)!r}"
            ) from exc

    # ------------------------------------------------------------------
    # Public endpoints
    # ------------------------------------------------------------------

    def list_policies(self, namespace: str) -> list[dict[str, Any]]:
        """List policies *defined* in ``namespace``.

        Returns the raw policy dicts as returned by the Polaris API.
        Use :func:`iceberg_ops.polaris.parse_policy` to turn each
        into a :class:`~iceberg_ops.polaris.Policy` if you want the
        parsed form.

        Note: policies *defined* in a namespace may or may not be
        attached to specific tables. Use
        :meth:`list_applicable_policies_for_table` to get the
        policies that actually apply to a given table (which includes
        inherited policies from parent namespaces).

        Parameters
        ----------
        namespace : str
            Namespace name. Multi-level namespaces are accepted in
            dot-separated form (``"db.subdb"``) and encoded with
            Polaris's ``%1F`` separator on the wire.

        Returns
        -------
        list of dict
            Raw policy dicts as returned by Polaris.
        """
        path = f"/catalogs/{self.catalog}/namespaces/{_encode_ns(namespace)}/policies"
        body = self._request("GET", path)
        return _extract_list(body, "policies")

    def get_policy(self, namespace: str, name: str) -> dict[str, Any]:
        """Fetch a single policy by name from ``namespace``.

        Parameters
        ----------
        namespace : str
            Namespace name (dot-separated for multi-level).
        name : str
            Policy name within the namespace.

        Returns
        -------
        dict
            Raw policy dict.

        Raises
        ------
        PolarisError
            On HTTP failure or when the response body is not a JSON
            object.
        """
        path = f"/catalogs/{self.catalog}/namespaces/{_encode_ns(namespace)}/policies/{name}"
        body = self._request("GET", path)
        if not isinstance(body, dict):
            raise PolarisError(f"Polaris get_policy({name!r}) returned non-object: {body!r}")
        return body

    def list_applicable_policies_for_table(
        self, namespace: str, table: str
    ) -> list[dict[str, Any]]:
        """List policies that apply to ``namespace.table``.

        Includes policies attached directly to the table and policies
        inherited from parent namespaces. The Polaris API returns the
        already-resolved set; the client does not re-walk the
        inheritance hierarchy itself.

        Parameters
        ----------
        namespace : str
            Namespace name (dot-separated for multi-level).
        table : str
            Table name within the namespace.

        Returns
        -------
        list of dict
            Raw policy dicts (the resolved applicable set).
        """
        path = (
            f"/catalogs/{self.catalog}/namespaces/{_encode_ns(namespace)}"
            f"/tables/{table}/applicable-policies"
        )
        body = self._request("GET", path)
        return _extract_list(body, "applicable-policies")

    def list_applicable_policies_for_namespace(self, namespace: str) -> list[dict[str, Any]]:
        """List policies that apply at the namespace level.

        Returns inherited + directly-attached policies for the
        namespace. Useful for users who want to drive maintenance
        from policies attached at namespace granularity rather than
        per-table.

        Parameters
        ----------
        namespace : str
            Namespace name (dot-separated for multi-level).

        Returns
        -------
        list of dict
            Raw policy dicts (the resolved applicable set).
        """
        path = f"/catalogs/{self.catalog}/namespaces/{_encode_ns(namespace)}/applicable-policies"
        body = self._request("GET", path)
        return _extract_list(body, "applicable-policies")


# ===========================================================================
# Module-level helpers
# ===========================================================================


def _encode_ns(namespace: str) -> str:
    """Encode a (possibly multi-level) namespace for a Polaris URL.

    Polaris encodes multi-level namespaces with ``%1F`` (unit
    separator) between levels. Single-level namespaces pass through
    unchanged.

    Accepts both ``"db"`` and ``"db.subdb"`` forms; dots are
    translated to ``%1F`` so the URL works for nested namespaces too.
    """
    return namespace.replace(".", "%1F")


def _is_header_safe(token: str) -> bool:
    """True if *token* is non-empty and has no whitespace or control characters."""
    return bool(token) and not any(c.isspace() or ord(c) < 0x20 or ord(c) == 0x7F for c in token)


def _scrub(text: str, secrets: tuple[str | None, ...]) -> str:
    """Replace each non-empty secret in *text* with ``***``.

    Both the raw value and its ``repr`` form (as quoted by, for
    example, requests' ``InvalidHeader``) are replaced.
    """
    forms = {form for s in secrets if s for form in (s, repr(s)[1:-1])}
    for form in sorted(forms, key=len, reverse=True):
        text = text.replace(form, "***")
    return text


def _describe(exc: BaseException, secrets: tuple[str | None, ...]) -> str:
    """Describe *exc* as ``"<type>: <message>"`` with *secrets* scrubbed."""
    return _scrub(f"{type(exc).__name__}: {exc}", secrets)


def _ok(response: Any) -> bool:
    """Return ``True`` when the response has a 2xx status code."""
    code = getattr(response, "status_code", None)
    return code is not None and 200 <= code < 300


def _body(response: Any) -> str:
    """Best-effort string body for error messages.

    Truncates to keep the resulting :class:`PolarisError` message
    small enough to log without overwhelming stdout.
    """
    text = getattr(response, "text", None)
    if not text:
        return ""
    text_str = str(text)
    return text_str[:1000] + ("..." if len(text_str) > 1000 else "")


def _extract_list(body: Any, key: str) -> list[dict[str, Any]]:
    """Extract a list from a Polaris response.

    Polaris responses use either of two shapes:

    * ``{"<key>": [{...}, {...}]}`` - the standard envelope
    * ``[{...}, {...}]`` - bare array (some endpoints return this)

    The helper also falls back to a singular form of the key and a
    generic ``"items"`` / ``"data"`` envelope so the client tolerates
    minor envelope-shape drift across Polaris releases.

    Anything else raises :class:`PolarisError`.
    """
    if isinstance(body, list):
        return [item for item in body if isinstance(item, dict)]
    if isinstance(body, dict):
        items = body.get(key)
        if isinstance(items, list):
            return [item for item in items if isinstance(item, dict)]
        # Some Polaris versions use a different envelope key - try
        # the singular form and a generic "items" as fallbacks before
        # giving up.
        for fallback in (key.rstrip("s"), "items", "data"):
            items = body.get(fallback)
            if isinstance(items, list):
                return [item for item in items if isinstance(item, dict)]
    raise PolarisError(f"Polaris response did not contain a list under {key!r}: {body!r}")
