"""Alerting for pipeline failures and quality-gate blocks.

Posts a message to a Slack Incoming Webhook when a pipeline run fails or is
blocked by a quality gate. Alerting is best-effort: a delivery failure is
logged but never propagates, so notification problems can't take down the
pipeline itself.

Disabled by default — set ``ALERTS_ENABLED=true`` and ``SLACK_WEBHOOK_URL``
to turn it on.
"""

import logging

import requests

from data_pipeline.config import get_settings

logger = logging.getLogger(__name__)


class SlackAlerter:
    """Sends pipeline alerts to a Slack Incoming Webhook."""

    DEFAULT_TIMEOUT = 10

    def __init__(self, webhook_url: str | None = None, enabled: bool | None = None):
        """Initialize the alerter.

        Args:
            webhook_url: Slack Incoming Webhook URL. Falls back to settings.
            enabled: Master on/off switch. Falls back to settings.
        """
        settings = get_settings()
        self.webhook_url = webhook_url if webhook_url is not None else settings.slack_webhook_url
        self.enabled = enabled if enabled is not None else settings.alerts_enabled

    def is_active(self) -> bool:
        """Whether alerts will actually be sent (enabled and configured)."""
        return self.enabled and bool(self.webhook_url)

    def send(self, text: str) -> bool:
        """Post a plain-text message to Slack.

        Returns:
            True if the message was delivered, False otherwise (including when
            alerting is inactive). Never raises.
        """
        if not self.is_active():
            logger.debug("Alerting inactive; skipping Slack notification")
            return False

        try:
            response = requests.post(
                self.webhook_url,
                json={"text": text},
                timeout=self.DEFAULT_TIMEOUT,
            )
            if response.ok:
                return True
            logger.warning(
                "Slack alert failed: HTTP %s %s", response.status_code, response.text[:200]
            )
            return False
        except requests.RequestException as e:
            logger.warning("Slack alert failed: %s", e)
            return False

    def alert_pipeline_result(
        self,
        *,
        run_id: str,
        status: str,
        reason: str = "",
        stats: dict[str, int] | None = None,
    ) -> bool:
        """Send an alert for a non-successful pipeline run.

        Only ``failed`` and ``blocked`` statuses are alerted on; anything else
        (e.g. ``success``) is a no-op.

        Args:
            run_id: Pipeline run identifier.
            status: Run status (failed/blocked/success).
            reason: Human-readable failure/block reason.
            stats: Optional record counts to include for context.

        Returns:
            True if an alert was delivered.
        """
        if status not in ("failed", "blocked"):
            return False

        emoji = "🛑" if status == "blocked" else "❌"
        lines = [f"{emoji} *Pipeline {status.upper()}* — run `{run_id}`"]
        if reason:
            lines.append(reason)
        if stats:
            stat_str = ", ".join(f"{k}={v}" for k, v in stats.items())
            lines.append(f"_{stat_str}_")

        return self.send("\n".join(lines))
