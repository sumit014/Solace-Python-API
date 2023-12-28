import asyncio
import socket
from solace.messaging.receiver.inbound_message import InboundMessage
from solace.messaging.resources.queue import Queue
from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener, ServiceEvent
from solace.messaging.receiver.message_receiver import MessagingHandler
from solace.messaging.receiver.persistent_message_receiver import PersistentMessageReceiver
from solace.messaging.config.missing_resource_creation_configuration import MissingResourceCreationStrategy
from solace.messaging.config.retry_strategy import RetryStrategy
from solace.messaging.config.transport_security_strategy import TLS
from solace.messaging.config.authentication_strategy import ClientCertificateAuthentication

class ServiceEventHandler(ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener):
  def on_reconnected(self, e: ServiceEvent):
    e.get_cause()
    e.get_message()

  def on_reconnecting(self, e: ServiceEvent):
    e.get_cause()
    e.get_message()

  def on_service_interrupted(self, e: ServiceEvent):
    e.get_cause()
    e.get_message()


class CustomMessageHandler(MessageHandler):
  def __init__(self, receiver):
    self._receiver = receiver

  def on_message(self, message: 'InboundMessage'):
    #this function will be called after receiving the message
    self._receiver.ack(message)
    # write your logic after receiving the message


def establish_aem_connection():
  # there are other authentication strategies 
  broker_props = {
      "solace.messaging.transport.host": "<your-host-name>",
      "solace.messaging.service.vpn-name": "<your-vpn-name>"
  }

  # you can go with_certificate_validation to verify the trust-store 
  transport_security = TLS.create().without_certificate_validation()
  # build a message service with reconnection strategy of 20 retries over an interval of 3 seconds and authentication strategy as certificate based authentication
  messaging_service = MessagingService.builder().from_properties(broker_props)\
          .with_reconnection_retry_strategy(RetryStrategy.parametrized_retyr(20, 3))\
          .with_transport_security_strategy(transport_security)\
          .with_authentication_strategy(\
            ClientCertificateAuthentication.of(
              certificate_file = "<your-certificate-file-path>",
              key_file = "<your-key-file-path>",
              key_password = "<your password to key file>")).build()
  messaging_service.connect()
  return messaging_service


def broker_properties():
  messaging_service = establish_aem_connection()
  service_handler = ServiceEventHandler()
  messaging_service.add_reconnection_listener(service_handler)
  messaging_service.add_reconnection_attempt_listener(service_handler)
  messaging_service.add_service_interruption_listener(service_handler)
  # queue from messages will be read
  queue_name = "<your-queue-name>"
  durable_exclusive_queue = Queue.durable_non_exclusive_queue(queue_name)
  # persistent message receiver is build
  persistent_receiver: PersistentMessageReceiver = messaging_service.create_persistent_message_receiver_builder()\
                          .with_missing_resource_creation_strategy(MissingResourceCreationStrategy.CREATE_ON_START)\
                          .build(durable_exclusive_queue)
  persistent_receiver.start()
  return persistent_receiver
  
async def run_consumer():
  receiver = broker_properties()
  message_handler = CustomMessageHandler(receiver)
  receiver.receive_async(message_handler)


def consumer():
  loop = asyncio.get_event_loop()
  try:
    asyncio.ensure_future(run_consumer())
    loop.run_forever()
  except:
    print(f"Function failed on host: {socket.hethostname()}")
    loop.close()


if __name__ == "main":
  consumer()
