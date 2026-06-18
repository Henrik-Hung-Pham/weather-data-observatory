"""Unit tests for Slack alerting."""

from unittest.mock import MagicMock, patch

import pytest

from data_pipeline.alerting import SlackAlerter


@pytest.mark.unit
def test_inactive_when_disabled() -> None:
    alerter = SlackAlerter(webhook_url="https://hooks.slack.test/x", enabled=False)
    assert alerter.is_active() is False
    with patch("data_pipeline.alerting.requests.post") as mock_post:
        assert alerter.send("hi") is False
        mock_post.assert_not_called()


@pytest.mark.unit
def test_inactive_when_no_url() -> None:
    alerter = SlackAlerter(webhook_url="", enabled=True)
    assert alerter.is_active() is False
    with patch("data_pipeline.alerting.requests.post") as mock_post:
        assert alerter.send("hi") is False
        mock_post.assert_not_called()


@pytest.mark.unit
def test_send_posts_to_webhook() -> None:
    alerter = SlackAlerter(webhook_url="https://hooks.slack.test/x", enabled=True)
    mock_response = MagicMock(ok=True, status_code=200)
    with patch("data_pipeline.alerting.requests.post", return_value=mock_response) as mock_post:
        assert alerter.send("hello") is True
        mock_post.assert_called_once()
        assert mock_post.call_args.kwargs["json"] == {"text": "hello"}


@pytest.mark.unit
def test_send_handles_http_error() -> None:
    alerter = SlackAlerter(webhook_url="https://hooks.slack.test/x", enabled=True)
    mock_response = MagicMock(ok=False, status_code=500, text="oops")
    with patch("data_pipeline.alerting.requests.post", return_value=mock_response):
        assert alerter.send("hello") is False


@pytest.mark.unit
def test_send_handles_network_exception() -> None:
    import requests

    alerter = SlackAlerter(webhook_url="https://hooks.slack.test/x", enabled=True)
    with patch(
        "data_pipeline.alerting.requests.post",
        side_effect=requests.RequestException("down"),
    ):
        assert alerter.send("hello") is False  # must not raise


@pytest.mark.unit
def test_alert_pipeline_result_skips_success() -> None:
    alerter = SlackAlerter(webhook_url="https://hooks.slack.test/x", enabled=True)
    with patch("data_pipeline.alerting.requests.post") as mock_post:
        assert alerter.alert_pipeline_result(run_id="r1", status="success") is False
        mock_post.assert_not_called()


@pytest.mark.unit
@pytest.mark.parametrize("status", ["failed", "blocked"])
def test_alert_pipeline_result_sends_on_failure(status: str) -> None:
    alerter = SlackAlerter(webhook_url="https://hooks.slack.test/x", enabled=True)
    mock_response = MagicMock(ok=True, status_code=200)
    with patch("data_pipeline.alerting.requests.post", return_value=mock_response) as mock_post:
        ok = alerter.alert_pipeline_result(
            run_id="r1",
            status=status,
            reason="bronze gate blocked",
            stats={"ingested": 5},
        )
        assert ok is True
        text = mock_post.call_args.kwargs["json"]["text"]
        assert status.upper() in text
        assert "bronze gate blocked" in text
        assert "ingested=5" in text
