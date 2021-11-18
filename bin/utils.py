import sys
import traceback
import backoff
import os
import yaml

from typing import Dict
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from functools import wraps


@backoff.on_exception(backoff.expo, SlackApiError)
def send_slack_message(message: str, text: str, color: str):
    """
    Sends message to a slack channel
    """
    config = load_config()
    client = WebClient(token=config['slack']['token'])
    client.chat_postMessage(channel=config['slack']['channel'],
                            text=message,
                            attachments=[
                                {
                                    "fallback": message,
                                    "color": color,
                                    "text": text,
                                }
                            ],
                            mrkdwn=True
                            )


def named_exception_handler(command_name):
    """
    Decorator to handle exceptions and send alerts via slack
    """
    def exception_handler(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            try:
                v = function(*args, **kwargs)
            except Exception:
                exc_type, exc_value, exc_traceback = sys.exc_info()

                send_slack_message(
                    message=':alarmamel: *Cicada is failing!* :alarmamel:',
                    text=f'`{command_name}` failed with `{exc_type.__name__}: {exc_value}`\n\nFull traceback:\n'
                         f'```{traceback.format_exception(exc_type, exc_value, exc_traceback)}```',
                    color='danger')

                raise

            return v

        return wrapper

    return exception_handler


def load_config() -> Dict:
    """
    Loads the config files ar cicada/config/definitions
    """
    config_file = os.path.join(os.path.abspath(os.path.dirname(__file__)), '../config/definitions.yml')
    return yaml.safe_load(open(config_file, 'r').read())
