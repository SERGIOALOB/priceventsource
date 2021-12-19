""" Example of service unit testing best practice. """

from nameko.testing.services import worker_factory
from user_events import PriceEventsService
import json

def test_send():
    # create worker with mock dependencies
    service = worker_factory(PriceEventsService)

    new_user_event = {
            'id': 'id',
            'weight' : 'weight',
            'timestamp': 'tx',
            'price': 'Free'
        }

    # test send service rpc method
    result = service.send(new_user_event)

    # Validate that we call kafka producer flush once
    service.producer.flush.assert_called_once()
