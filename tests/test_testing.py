"""Tests for :mod:`iceberg_ops.testing` - the public FakeSession / FakeResponse."""

from __future__ import annotations

import pytest

from iceberg_ops.testing import FakeResponse, FakeSession


class TestFakeResponse:
    def test_defaults(self) -> None:
        r = FakeResponse()
        assert r.status_code == 200
        assert r.text == ""
        # No json body -> .json() raises.
        with pytest.raises(ValueError, match="no JSON body"):
            r.json()

    def test_json_body_round_trip(self) -> None:
        body = {"hello": "world", "n": 42}
        r = FakeResponse(json_body=body)
        assert r.json() == body
        # text auto-derived from json_body.
        assert "hello" in r.text
        assert "world" in r.text

    def test_explicit_text_overrides_json_serialisation(self) -> None:
        r = FakeResponse(json_body={"a": 1}, text="raw text")
        assert r.text == "raw text"
        # json() still works because json_body was given.
        assert r.json() == {"a": 1}

    def test_status_code_passes_through(self) -> None:
        r = FakeResponse(status_code=503, text="server gone")
        assert r.status_code == 503
        assert r.text == "server gone"

    def test_raise_for_status_is_noop(self) -> None:
        # Currently a no-op (matches old behaviour). The test
        # locks the contract in - if the helper grows real
        # behaviour, this test should be updated.
        FakeResponse(status_code=500).raise_for_status()  # does not raise


class TestFakeSession:
    def test_constructor_initialises_empty_state(self) -> None:
        sess = FakeSession()
        assert sess.calls == []

    def test_request_records_and_returns_canned_response(self) -> None:
        sess = FakeSession()
        sess.register("GET", "/policies", FakeResponse(json_body={"policies": []}))
        resp = sess.request("GET", "https://api.example.com/policies")
        assert resp.json() == {"policies": []}
        assert len(sess.calls) == 1
        call = sess.calls[0]
        assert call["method"] == "GET"
        assert call["url"] == "https://api.example.com/policies"
        assert call["headers"] == {}

    def test_request_records_kwargs(self) -> None:
        sess = FakeSession()
        sess.register("POST", "/items", FakeResponse(json_body={"id": 1}))
        sess.request(
            "POST",
            "https://api.example.com/items",
            json={"name": "x"},
            headers={"Authorization": "Bearer t"},
            timeout=5.0,
            params={"q": "hello"},
        )
        call = sess.calls[0]
        assert call["json"] == {"name": "x"}
        assert call["headers"] == {"Authorization": "Bearer t"}
        assert call["timeout"] == 5.0
        assert call["params"] == {"q": "hello"}

    def test_post_records_separately_from_request(self) -> None:
        sess = FakeSession()
        sess.register("POST", "/oauth/tokens", FakeResponse(json_body={"access_token": "t"}))
        sess.post(
            "https://api.example.com/oauth/tokens", data={"grant_type": "client_credentials"}
        )
        call = sess.calls[0]
        assert call["method"] == "POST"
        assert call["data"] == {"grant_type": "client_credentials"}

    def test_unmatched_request_raises(self) -> None:
        sess = FakeSession()
        # No registrations -> any call must raise.
        with pytest.raises(AssertionError, match="unexpected GET"):
            sess.request("GET", "https://api.example.com/anything")

    def test_unmatched_post_raises(self) -> None:
        sess = FakeSession()
        with pytest.raises(AssertionError, match="unexpected POST"):
            sess.post("https://api.example.com/oauth/tokens")

    def test_method_substring_match_is_case_insensitive(self) -> None:
        sess = FakeSession()
        sess.register("get", "/x", FakeResponse(json_body={}))
        sess.request("GET", "https://api.example.com/x")
        # Reverse: register upper-case, call lower-case.
        sess.register("POST", "/y", FakeResponse(json_body={}))
        sess.request("post", "https://api.example.com/y")

    def test_responses_consumed_in_order(self) -> None:
        sess = FakeSession()
        # Two registrations for the same endpoint - they should be
        # served in registration order.
        sess.register("GET", "/x", FakeResponse(json_body={"page": 1}))
        sess.register("GET", "/x", FakeResponse(json_body={"page": 2}))
        first = sess.request("GET", "https://api.example.com/x")
        second = sess.request("GET", "https://api.example.com/x")
        assert first.json() == {"page": 1}
        assert second.json() == {"page": 2}
        # A third call has no remaining response.
        with pytest.raises(AssertionError):
            sess.request("GET", "https://api.example.com/x")

    def test_path_substring_match_does_not_require_exact_url(self) -> None:
        # The substring matcher lets the client build a full URL
        # from base + path; tests register only the path fragment.
        sess = FakeSession()
        sess.register("GET", "/items/42", FakeResponse(json_body={"id": 42}))
        resp = sess.request("GET", "https://example.com/api/v1/items/42")
        assert resp.json() == {"id": 42}
