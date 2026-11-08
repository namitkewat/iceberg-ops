"""Tests for :mod:`iceberg_ops.polaris.client`.

All tests are 100% in-process - the HTTP layer is replaced with the
public :class:`iceberg_ops.testing.FakeSession` injected via the
client's ``session=`` kwarg. No Docker, no network, no live Polaris
instance required.
"""

from __future__ import annotations

import pytest

from iceberg_ops.polaris import PolarisClient, PolarisError
from iceberg_ops.testing import FakeResponse, FakeSession

# ===========================================================================
# Construction & validation
# ===========================================================================


class TestPolarisClientConstruction:
    def test_requires_token_or_credentials(self) -> None:
        with pytest.raises(ValueError, match="exactly one"):
            PolarisClient("https://polaris.example.com", "cat")

    def test_rejects_both_token_and_credentials(self) -> None:
        with pytest.raises(ValueError, match="exactly one"):
            PolarisClient(
                "https://polaris.example.com",
                "cat",
                token="t",
                oauth2_credentials=("c", "s"),
            )

    def test_strips_trailing_slash_from_base_url(self) -> None:
        c = PolarisClient("https://polaris.example.com/", "cat", token="t")
        assert c.base_url == "https://polaris.example.com"

    def test_keeps_catalog(self) -> None:
        c = PolarisClient("https://x", "mycat", token="t")
        assert c.catalog == "mycat"

    def test_management_prefix_trailing_slash_normalised(self) -> None:
        # The constructor strips a trailing slash on management_prefix
        # too, since paths are concatenated naively in _request.
        c = PolarisClient(
            "https://x",
            "cat",
            token="t",
            management_prefix="/api/management/v1/",
        )
        assert c._management_prefix == "/api/management/v1"

    def test_token_surrounding_whitespace_is_stripped(self) -> None:
        """A token read from a CRLF file still works."""
        sess = FakeSession()
        sess.register("GET", "/policies", FakeResponse(json_body={"policies": []}))
        c = PolarisClient("https://x", "cat", token=" SUPERSECRET123\r\n", session=sess)
        c.list_policies("ns")
        assert sess.calls[0]["headers"]["Authorization"] == "Bearer SUPERSECRET123"

    @pytest.mark.parametrize(
        "token",
        ["SUPER SECRET123", "SUPERSECRET\r\nX-Injected: 1", "SUPERSECRET\x00123", "", " \r\n"],
    )
    def test_unsafe_token_rejected_without_echo(self, token: str) -> None:
        with pytest.raises(ValueError, match="whitespace or control characters") as excinfo:
            PolarisClient("https://x", "cat", token=token)
        assert "SUPER" not in str(excinfo.value)


# ===========================================================================
# Authentication
# ===========================================================================


class TestPolarisClientAuth:
    def test_static_token_is_used_directly(self) -> None:
        sess = FakeSession()
        sess.register("GET", "/policies", FakeResponse(json_body={"policies": []}))
        c = PolarisClient("https://x", "cat", token="static-bearer", session=sess)
        c.list_policies("ns")
        # Authorization header carries the static token; no token-fetch call.
        assert sess.calls[0]["headers"]["Authorization"] == "Bearer static-bearer"
        # Only the management call happened - no token fetch.
        assert all("oauth/tokens" not in call["url"] for call in sess.calls)

    def test_oauth2_credentials_fetch_token_first(self) -> None:
        sess = FakeSession()
        sess.register(
            "POST",
            "/api/catalog/v1/oauth/tokens",
            FakeResponse(json_body={"access_token": "fetched-bearer", "expires_in": 3600}),
        )
        sess.register("GET", "/policies", FakeResponse(json_body={"policies": []}))
        c = PolarisClient(
            "https://polaris.example.com",
            "cat",
            oauth2_credentials=("client-id", "client-secret"),
            session=sess,
        )
        c.list_policies("ns")
        # Two calls: token fetch first, then management.
        assert sess.calls[0]["url"].endswith("/api/catalog/v1/oauth/tokens")
        assert sess.calls[0]["data"]["grant_type"] == "client_credentials"
        assert sess.calls[0]["data"]["client_id"] == "client-id"
        assert sess.calls[0]["data"]["client_secret"] == "client-secret"
        assert sess.calls[0]["data"]["scope"] == "PRINCIPAL_ROLE:ALL"
        assert sess.calls[1]["headers"]["Authorization"] == "Bearer fetched-bearer"

    def test_oauth2_token_cached_across_calls(self) -> None:
        sess = FakeSession()
        sess.register("POST", "/oauth/tokens", FakeResponse(json_body={"access_token": "tok"}))
        sess.register("GET", "/policies", FakeResponse(json_body={"policies": []}))
        sess.register("GET", "/policies", FakeResponse(json_body={"policies": []}))
        c = PolarisClient(
            "https://x",
            "cat",
            oauth2_credentials=("c", "s"),
            session=sess,
        )
        c.list_policies("ns")
        c.list_policies("ns")
        # Token fetched once; reused on the second list_policies.
        token_calls = [call for call in sess.calls if "oauth/tokens" in call["url"]]
        assert len(token_calls) == 1

    def test_oauth2_token_endpoint_failure_raises(self) -> None:
        sess = FakeSession()
        sess.register(
            "POST",
            "/oauth/tokens",
            FakeResponse(status_code=401, text="bad creds"),
        )
        c = PolarisClient(
            "https://x",
            "cat",
            oauth2_credentials=("c", "s"),
            session=sess,
        )
        with pytest.raises(PolarisError, match="OAuth2 token request") as excinfo:
            c.list_policies("ns")
        assert excinfo.value.status_code == 401
        assert "bad creds" in (excinfo.value.body or "")

    def test_oauth2_token_endpoint_missing_access_token(self) -> None:
        sess = FakeSession()
        sess.register(
            "POST",
            "/oauth/tokens",
            FakeResponse(json_body={"oops": "no token"}),
        )
        c = PolarisClient(
            "https://x",
            "cat",
            oauth2_credentials=("c", "s"),
            session=sess,
        )
        with pytest.raises(PolarisError, match="missing 'access_token'"):
            c.list_policies("ns")

    def test_oauth2_token_endpoint_non_json_response(self) -> None:
        sess = FakeSession()
        # 200 OK but text body, no JSON.
        sess.register("POST", "/oauth/tokens", FakeResponse(text="<html/>"))
        c = PolarisClient(
            "https://x",
            "cat",
            oauth2_credentials=("c", "s"),
            session=sess,
        )
        with pytest.raises(PolarisError, match="non-JSON"):
            c.list_policies("ns")

    def test_missing_access_token_error_does_not_echo_payload_values(self) -> None:
        # A camelCase token is still a secret; the error names the keys only.
        sess = FakeSession()
        sess.register(
            "POST",
            "/oauth/tokens",
            FakeResponse(json_body={"accessToken": "SECRET123", "error": "odd_shape"}),
        )
        c = PolarisClient("https://x", "cat", oauth2_credentials=("c", "s"), session=sess)
        with pytest.raises(PolarisError) as excinfo:
            c.list_policies("ns")
        message = str(excinfo.value)
        assert "SECRET123" not in message
        assert "SECRET123" not in (excinfo.value.body or "")
        assert "accessToken" in message
        assert "odd_shape" in message

    def test_non_json_token_response_does_not_echo_body(self) -> None:
        sess = FakeSession()
        sess.register("POST", "/oauth/tokens", FakeResponse(text="token=SECRET123"))
        c = PolarisClient("https://x", "cat", oauth2_credentials=("c", "s"), session=sess)
        with pytest.raises(PolarisError, match="non-JSON") as excinfo:
            c.list_policies("ns")
        assert "SECRET123" not in str(excinfo.value)

    def test_non_object_token_response_raises(self) -> None:
        sess = FakeSession()
        sess.register("POST", "/oauth/tokens", FakeResponse(json_body=["SECRET123"]))
        c = PolarisClient("https://x", "cat", oauth2_credentials=("c", "s"), session=sess)
        with pytest.raises(PolarisError, match="expected an object") as excinfo:
            c.list_policies("ns")
        assert "SECRET123" not in str(excinfo.value)

    def test_custom_oauth_scope_is_used(self) -> None:
        sess = FakeSession()
        sess.register("POST", "/oauth/tokens", FakeResponse(json_body={"access_token": "t"}))
        sess.register("GET", "/policies", FakeResponse(json_body={"policies": []}))
        c = PolarisClient(
            "https://x",
            "cat",
            oauth2_credentials=("c", "s"),
            oauth2_scope="CATALOG_ADMIN",
            session=sess,
        )
        c.list_policies("ns")
        assert sess.calls[0]["data"]["scope"] == "CATALOG_ADMIN"

    def test_token_endpoint_connection_failure(self) -> None:
        class _BoomSession(FakeSession):
            def post(self, *_a, **_kw):
                raise OSError("dns failure")

        sess = _BoomSession()
        c = PolarisClient(
            "https://x",
            "cat",
            oauth2_credentials=("c", "s"),
            session=sess,
        )
        with pytest.raises(PolarisError, match="Failed to reach Polaris token endpoint"):
            c.list_policies("ns")

    def test_token_endpoint_failure_never_echoes_client_secret(self) -> None:
        class _EchoSession(FakeSession):
            def post(self, url, *, data=None, timeout=None):
                raise OSError(f"proxy rejected body {data!r}")

        c = PolarisClient(
            "https://x",
            "cat",
            oauth2_credentials=("client-id", "TOPSECRET\\value"),
            session=_EchoSession(),
        )
        with pytest.raises(
            PolarisError, match="Failed to reach Polaris token endpoint"
        ) as excinfo:
            c.list_policies("ns")
        assert "TOPSECRET" not in str(excinfo.value)
        assert "OSError" in str(excinfo.value)
        assert excinfo.value.__cause__ is None
        assert excinfo.value.__suppress_context__ is True

    def test_fetched_token_with_control_characters_rejected(self) -> None:
        sess = FakeSession()
        sess.register(
            "POST",
            "/api/catalog/v1/oauth/tokens",
            FakeResponse(json_body={"access_token": "FETCHEDSECRET\r\nX-Injected: 1"}),
        )
        c = PolarisClient("https://x", "cat", oauth2_credentials=("c", "s"), session=sess)
        with pytest.raises(PolarisError, match="whitespace or control characters") as excinfo:
            c.list_policies("ns")
        assert "FETCHEDSECRET" not in str(excinfo.value)
        # No management call was attempted with the unsafe token.
        assert all(call["method"] == "POST" for call in sess.calls)


# ===========================================================================
# Endpoint URL construction
# ===========================================================================


class TestPolarisClientEndpoints:
    @staticmethod
    def _client(session: FakeSession) -> PolarisClient:
        return PolarisClient(
            "https://polaris.example.com",
            "mycat",
            token="t",
            session=session,
        )

    def test_list_policies_url(self) -> None:
        sess = FakeSession()
        sess.register("GET", "/policies", FakeResponse(json_body={"policies": []}))
        self._client(sess).list_policies("sales")
        url = sess.calls[0]["url"]
        assert url == (
            "https://polaris.example.com/api/management/v1"
            "/catalogs/mycat/namespaces/sales/policies"
        )

    def test_list_policies_encodes_nested_namespace(self) -> None:
        sess = FakeSession()
        sess.register("GET", "/policies", FakeResponse(json_body={"policies": []}))
        self._client(sess).list_policies("sales.eu")
        url = sess.calls[0]["url"]
        # Multi-level namespaces are encoded with %1F (unit separator).
        assert "/namespaces/sales%1Feu/policies" in url

    def test_get_policy_url(self) -> None:
        sess = FakeSession()
        sess.register(
            "GET",
            "/policies/snap-expiry",
            FakeResponse(json_body={"name": "snap-expiry", "type": "system.snapshot-expiry"}),
        )
        self._client(sess).get_policy("sales", "snap-expiry")
        url = sess.calls[0]["url"]
        assert url.endswith("/namespaces/sales/policies/snap-expiry")

    def test_get_policy_returns_dict(self) -> None:
        sess = FakeSession()
        sess.register(
            "GET",
            "/policies/p",
            FakeResponse(json_body={"name": "p", "type": "system.snapshot-expiry"}),
        )
        result = self._client(sess).get_policy("ns", "p")
        assert result == {"name": "p", "type": "system.snapshot-expiry"}

    def test_get_policy_rejects_non_object_response(self) -> None:
        sess = FakeSession()
        sess.register("GET", "/policies/p", FakeResponse(json_body=["array", "not", "object"]))
        with pytest.raises(PolarisError, match="non-object"):
            self._client(sess).get_policy("ns", "p")

    def test_applicable_policies_for_table_url(self) -> None:
        sess = FakeSession()
        sess.register(
            "GET",
            "/applicable-policies",
            FakeResponse(json_body={"applicable-policies": []}),
        )
        self._client(sess).list_applicable_policies_for_table("sales", "orders")
        url = sess.calls[0]["url"]
        assert url.endswith("/catalogs/mycat/namespaces/sales/tables/orders/applicable-policies")

    def test_applicable_policies_for_namespace_url(self) -> None:
        sess = FakeSession()
        sess.register(
            "GET",
            "/applicable-policies",
            FakeResponse(json_body={"applicable-policies": []}),
        )
        self._client(sess).list_applicable_policies_for_namespace("sales")
        url = sess.calls[0]["url"]
        assert url.endswith("/catalogs/mycat/namespaces/sales/applicable-policies")

    def test_authorization_header_is_set_on_management_calls(self) -> None:
        sess = FakeSession()
        sess.register("GET", "/policies", FakeResponse(json_body={"policies": []}))
        self._client(sess).list_policies("sales")
        assert sess.calls[0]["headers"]["Authorization"] == "Bearer t"
        assert sess.calls[0]["headers"]["Accept"] == "application/json"

    def test_204_no_content_returns_none(self) -> None:
        # Some management endpoints (delete-style) return 204. Force the
        # path through a list_policies stub by registering a 204 response.
        sess = FakeSession()
        sess.register("GET", "/policies", FakeResponse(status_code=204))
        # When the response is 204, _request returns None, so the
        # public method - which calls _extract_list - will raise.
        with pytest.raises(PolarisError, match="did not contain a list"):
            self._client(sess).list_policies("sales")


# ===========================================================================
# Response parsing
# ===========================================================================


class TestPolarisClientResponseShapes:
    @staticmethod
    def _client(session: FakeSession) -> PolarisClient:
        return PolarisClient("https://x", "cat", token="t", session=session)

    def test_envelope_with_policies_key(self) -> None:
        sess = FakeSession()
        sess.register(
            "GET",
            "/policies",
            FakeResponse(
                json_body={
                    "policies": [
                        {"name": "p1", "type": "system.snapshot-expiry"},
                        {"name": "p2", "type": "system.metadata-compaction"},
                    ]
                }
            ),
        )
        result = self._client(sess).list_policies("ns")
        assert len(result) == 2
        assert result[0]["name"] == "p1"

    def test_bare_array_response(self) -> None:
        sess = FakeSession()
        sess.register(
            "GET",
            "/policies",
            FakeResponse(json_body=[{"name": "p1", "type": "system.snapshot-expiry"}]),
        )
        result = self._client(sess).list_policies("ns")
        assert len(result) == 1
        assert result[0]["name"] == "p1"

    def test_unrecognised_envelope_raises(self) -> None:
        sess = FakeSession()
        sess.register(
            "GET",
            "/policies",
            FakeResponse(json_body={"unexpected": "shape"}),
        )
        with pytest.raises(PolarisError, match="did not contain a list"):
            self._client(sess).list_policies("ns")

    def test_envelope_with_singular_key_fallback(self) -> None:
        # Some Polaris versions return "policy" (singular) as the
        # envelope key. The client falls back to it before giving up.
        sess = FakeSession()
        sess.register(
            "GET",
            "/policies",
            FakeResponse(
                json_body={"policie": [{"name": "p1", "type": "system.snapshot-expiry"}]}
            ),
        )
        result = self._client(sess).list_policies("ns")
        assert len(result) == 1
        assert result[0]["name"] == "p1"

    def test_envelope_items_fallback(self) -> None:
        sess = FakeSession()
        sess.register(
            "GET",
            "/policies",
            FakeResponse(json_body={"items": [{"name": "p1", "type": "system.snapshot-expiry"}]}),
        )
        result = self._client(sess).list_policies("ns")
        assert len(result) == 1

    def test_envelope_data_fallback(self) -> None:
        sess = FakeSession()
        sess.register(
            "GET",
            "/policies",
            FakeResponse(json_body={"data": [{"name": "p1", "type": "system.snapshot-expiry"}]}),
        )
        result = self._client(sess).list_policies("ns")
        assert len(result) == 1

    def test_non_dict_entries_filtered_from_list(self) -> None:
        # Defensive: a wonky API mixing dicts and bare strings should
        # not crash the client.
        sess = FakeSession()
        sess.register(
            "GET",
            "/policies",
            FakeResponse(
                json_body={
                    "policies": [
                        {"name": "good", "type": "system.snapshot-expiry"},
                        "garbage",
                        42,
                    ]
                }
            ),
        )
        result = self._client(sess).list_policies("ns")
        assert len(result) == 1
        assert result[0]["name"] == "good"


class TestPolarisClientErrorPaths:
    @staticmethod
    def _client(session: FakeSession) -> PolarisClient:
        return PolarisClient("https://x", "cat", token="t", session=session)

    def test_4xx_raises_with_status_and_body(self) -> None:
        sess = FakeSession()
        sess.register("GET", "/policies", FakeResponse(status_code=404, text="not found"))
        with pytest.raises(PolarisError, match="404") as excinfo:
            self._client(sess).list_policies("ns")
        assert excinfo.value.status_code == 404
        assert "not found" in (excinfo.value.body or "")

    def test_5xx_raises(self) -> None:
        sess = FakeSession()
        sess.register("GET", "/policies", FakeResponse(status_code=503, text="busy"))
        with pytest.raises(PolarisError) as excinfo:
            self._client(sess).list_policies("ns")
        assert excinfo.value.status_code == 503

    def test_non_json_response_raises(self) -> None:
        sess = FakeSession()
        # status 200 but body isn't JSON.
        sess.register("GET", "/policies", FakeResponse(text="<html>error</html>"))
        with pytest.raises(PolarisError, match="non-JSON"):
            self._client(sess).list_policies("ns")

    def test_long_body_truncated_in_error(self) -> None:
        sess = FakeSession()
        sess.register(
            "GET",
            "/policies",
            FakeResponse(status_code=500, text="x" * 5000),
        )
        with pytest.raises(PolarisError) as excinfo:
            self._client(sess).list_policies("ns")
        body = excinfo.value.body or ""
        # Truncation cap at 1000 chars + ellipsis suffix.
        assert len(body) <= 1010
        assert body.endswith("...")

    def test_connection_failure_wrapped(self) -> None:
        class _BoomSession(FakeSession):
            def request(self, *_a, **_kw):
                raise OSError("connection refused")

        sess = _BoomSession()
        with pytest.raises(PolarisError, match="failed before response"):
            self._client(sess).list_policies("ns")

    def test_connection_failure_never_echoes_token(self) -> None:
        """Exception text that quotes the Authorization header (raw or
        repr) is scrubbed, and the raw exception is not chained."""

        class _EchoSession(FakeSession):
            def request(self, method, url, *, headers=None, **_kw):
                value = (headers or {})["Authorization"]
                raise ValueError(f"bad header {value!r}; raw={value}")

        c = PolarisClient("https://x", "cat", token="SUPERSECRETTOKEN123", session=_EchoSession())
        with pytest.raises(PolarisError, match="failed before response") as excinfo:
            c.list_policies("ns")
        message = str(excinfo.value)
        assert "SUPERSECRETTOKEN123" not in message
        assert "ValueError" in message and "***" in message
        assert excinfo.value.__cause__ is None
        assert excinfo.value.__suppress_context__ is True

    def test_requests_invalid_header_text_is_scrubbed(self) -> None:
        """requests' InvalidHeader quotes the header value with repr();
        the scrubber removes that form too. requests raises while
        preparing the request, so nothing is sent."""
        requests = pytest.importorskip("requests")

        c = PolarisClient("http://127.0.0.1:9", "cat", token="SUPERSECRETTOKEN123")
        c._session = requests.Session()
        # Bypass the constructor check to reach the header validation.
        c._cached_token = "SUPERSECRETTOKEN123\r"
        with pytest.raises(PolarisError, match="InvalidHeader") as excinfo:
            c.list_policies("ns")
        assert "SUPERSECRETTOKEN123" not in str(excinfo.value)


class TestRequestsImportError:
    def test_lazy_import_error_message(self, monkeypatch) -> None:
        import builtins

        c = PolarisClient("https://x", "cat", token="t")
        # Force any `import requests` inside the client to fail.
        original_import = builtins.__import__

        def _fake_import(name, *args, **kwargs):
            if name == "requests":
                raise ImportError("No module named 'requests'")
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _fake_import)
        # _ensure_session triggers the import; trigger via list_policies.
        with pytest.raises(ImportError, match=r"iceberg-ops\[polaris\]"):
            c.list_policies("ns")
