"""An example of a custom sender for the Service Bus Router."""
from azure.servicebus import ServiceBusMessage
from azure.servicebus.aio import ServiceBusSender


async def custom_sender(sender: ServiceBusSender, topic_name: str, message_body: object, application_properties: dict):
    """
    Send messages to the ie.topic with a session ID.

    For all other topics, no session ID is required.

    Parameters
    ----------
    sender : ServiceBusSender
        The sender to use to send the message.
    message_body : str | bytes
        The message body that is to be sent to the topic.
    application_properties : dict | None
        An optional set of properties that may have been set on the source message.
    """
    if sender.entity_name == 'ie.topic':
        session_message = ServiceBusMessage(
            body=message_body,
            application_properties=application_properties,
            session_id='0'
        )
        await sender.send_messages(message=session_message)
    else:
        await sender.send_messages(ServiceBusMessage(body=message_body, application_properties=application_properties))
