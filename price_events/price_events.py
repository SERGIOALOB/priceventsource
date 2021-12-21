import json
import logging

from nameko.dependency_providers import Config
from nameko.rpc import rpc
from nameko_kafka import KafkaProducer


class PriceEventsService:
    """   Price events  """
    name = "priceevents"
    config = Config()
    producer = KafkaProducer()
    
    def _on_send_error(self, excp):
        logging.error('Error sending message', exc_info=excp)
        # handle exception
        # Note: implement a retry logic, or a dead letter queue, ...


    def serialise_message(self, message: dict) -> bytes:
        return json.dumps(message).encode('utf-8')


    def send_message(self, message: dict):
        future = self.producer.send("price_event", value=self.serialise_message(message))
        future.add_errback(self._on_send_error)


    @rpc
    def send(self, price_events):
        for price_event in price_events:
            self.send_message(message=price_event)
            logging.info(f'New price event {price_event}')
        self.producer.flush()
