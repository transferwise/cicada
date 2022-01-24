"""Send a test message to Slack."""

from cicada.lib import utils


def main(text: str):
    """Send a test message to Slack."""
    utils.send_slack_message(":dart:  *_Ping_* from Cicada :dart:", text, "good")
