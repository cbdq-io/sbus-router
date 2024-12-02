#!/usr/bin/env python
"""Wait for the Service Bus emulator to be alive and responsive."""
import logging
import os
import sys
import time

from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container

logging.basicConfig()
logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.DEBUG)


class SimpleSender(MessagingHandler):
    """
    Send a message to Azure Service Bus.

    Parameters
    ----------
    url : str
        The URL to send the message to.
    target : str
        The queue or topic name to send to.
    message_body : Message
        The message to be sent.
    """

    def __init__(self, url: str, target: str, message_body: Message) -> None:
        super(SimpleSender, self).__init__()
        self.url = url
        self.target = target
        self.message_body = message_body

    def on_start(self, event):
        """Create a connection to a URL."""
        logger.debug(f'Creating a connection to "{self.url}".')
        conn = event.container.connect(self.url)
        self.sender = event.container.create_sender(conn, self.target)

    def on_sendable(self, event):
        """Send a message to a sender connection."""
        message = Message(body=self.message_body)
        event.sender.send(message)
        logger.debug(f'Message sent "{self.message_body}".')
        event.sender.close()
        event.connection.close()


if __name__ == '__main__':
    attempts = 0

    while True:
        try:
            Container(SimpleSender(os.environ['AQMP_URL'], 'connection.test', 'Hello, world!')).run()
            logger.info('The emulator is now running.')
            break
        except Exception as e:
            logger.warning(str(e))

            if attempts >= 45:
                raise TimeoutError(e)

            time.sleep(1)

        attempts += 1

    sys.exit(0)
