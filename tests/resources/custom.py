from logging import Logger

from azure.servicebus import ServiceBusMessage


def transform(msg: ServiceBusMessage, topic_name: str, logger: Logger) -> ServiceBusMessage:
    """Set a session ID if an Irish topic."""
    # seed = str(application_properties.get(_SEED_KEY))
    # session_id = _session_from_seed(seed)
    session_id = None

    if topic_name == 'ie.topic':
        session_id = '0'

    logger.debug(f'transform {topic_name} {session_id}')
    msg.session_id = session_id
    return msg
