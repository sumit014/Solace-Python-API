from solace.messaging.publish.outbound_message import OutboundMessageBuilder
from solace.messaging.resources.topic import Topic
from solace.messaging.utils.manageable import Metric
from solace.messaging.messaging_service import MessagingService, RetryStrategy
from solace.messaging.receiver.message_receiver import MessagingHandler
from solace.messaging.publisher.persistent_message_publisher import PersistentMessagePublisher, MessagePublishReceiptListener
from solace.messaging.config.transport_security_strategy import TLS
from solace.messaging.config.authentication_strategy import ClientCertificateAuthentication

class MessagePublishReceiptListenerImpl(MessagePublishReceiptListener):
  def __init__(self):
    self.publish_count = 0
  @property
  def get_publish_count(self):
    return self._publish_count
  def on_publish_receipt(self, publish_receipt: 'PublishReceipt'):
    self._publish_count += 1
    if publish_receipt.user_context:
      print(f'User context received: {publish_receipt.user_context.get_custom_message}')


class ClsMessagePublisher:
  def create_persistent_message_publisher(service: MessageService) -> 'PersistentMessagePublisher':
    #method to create, build and start persistent publisher
    publisher: PersistentMessagePublisher = service.create_persistent_message_publisher_builder().build()
    publisher.start_async()
    return publisher

  def publish_byte_message_blocking_waiting_for_publisher_confirmation(message_builder: OutboundMessageBuilder,
                                                                       messaging_service: MessageService,
                                                                       message_publisher: PersistentMessagePublisher,
                                                                       destination: Topic, message, time_out,
                                                                       message_priority):
    publish_receipt_listener = MessagePublishReceiptListenerImpl()
    message_publisher.set_message_publish_receipt_listener(publish_receipt_listener)
    message = message_builder.with_priority(message_priority).build(payload = message)
    message_publisher.publish_await_acknowledgement(message, destination, time_out)
    metrics = messaging_service.metrics()
    print(f'Publish message count: {metrics.get_value(Metric.PERSISTENT_MESSAGES_SENT)}')


def connect_to_aem_and_publish_message(msg):

  TOPIC_ENDPOINT_DEFAULT = "<your-topic-endpoint>"
  MESSAGE_TO_SEND = msg
  APPLICATION_MESSAGE_ID = "Solace-App"
  MESSAGE_PRIORITY = 5

  broker_props = {
        "solace.messaging.transport.host": "<your-host-url>",
        "solace.messaging.service.vpn-name": "<your-vpn-name"
      }
  transport_security = TLS.create().without_certificate_validation()

  messaging_service = MessagingService.builder().from_properties(broker_props)\
            .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20, 3))\
            .with_transport_security_strategy(transport_security)\
            .with_authentication_strategy(\
            ClientCertificateAuthentication.of(
              certificate_file = "<your-certificate-file-path>",
              key_file = "<your-key-file-path>",
              key_password = "<your-password-to-key-file>")).build()
  messaging_service.connect()

  try:
    topic = Topic.of(TOPIC_ENDPOINT_DEFAULT)
    message = MESSAGE_TO_SEND
    outbound_msg = messaging_service.message_builder().with_application_message_id(APPLICATION_MESSAGE_ID)
    msg_priority = MESSAGE_PRIORITY

    publisher = ClsMessagePublisher.create_persistent_message_publisher(messaging_service)

    ClsMessagePublisher.publish_byte_message_blocking_waiting_for_publisher_confirmation(outbound_msg,
                                                                                        messaging_service = messaging_service,
                                                                                        message_publisher = publisher,
                                                                                        destination = topic,
                                                                                        message = message,
                                                                                        time_out = 2000,
                                                                                        message_priority = msg_priority)
    print(f'message: {MESSAGE_TO_SEND} published to topic {TOPIC_ENDPOINT_DEFAULT} successfully')
  except Exception as e:
    print("Error occured:", e)
  finally:
    try:
      publisher.terminate()
      messaging_service.disconnect()
    except Exception as e:
      print("Error occured while closing", e)


connect_to_aem_and_publish_message("Hello World:)
