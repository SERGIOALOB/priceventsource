import datetime
import json
import logging

from nameko.dependency_providers import Config
from nameko.rpc import rpc
from nameko_kafka import consume
from nameko_sqlalchemy import Database
from sqlalchemy import Column, String, create_engine, func, desc, Integer
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base

DeclarativeBase = declarative_base()
MAX_ID = 10
    
class Price(DeclarativeBase):
    __tablename__ = "prices"
    seq_id = Column(Integer, primary_key=True, autoincrement=True)
    id = Column(String)
    weight = Column(String)
    price = Column(String)
    timestamp = Column(String)
    index_return = Column(String)
    price_index = Column(String)

    def __init__(self,
                 id,
                 weight,
                 price,
                 timestamp,
                 index_return,
                 price_index):
        self.id = id
        self.weight = weight
        self.price = price
        self.timestamp = timestamp
        self.index_return = index_return
        self.price_index = price_index


engine = create_engine('sqlite:///data.sqlite', echo = True)
DeclarativeBase.metadata.create_all(engine)

class PriceStats:
    """    Price stats  service  """

    name = "pricestats"
    config = Config()
    db = Database(DeclarativeBase)

    
    @consume("price_event", group_id="price_event")
    def consume_price_event(self, new_price_event: bytes):
        """ Subcribe price_event topic and update database """
        
        message = self._deserialise_message(message=new_price_event.value)
        logging.info(f'Message received: {message}')
        self._calculate_index(message) 

    @rpc
    def report(self):
        """ Return price count by city """
        return self._get_report()

    def _get_report(self):
        record = self._get_last_record()
        if not record:
            report = {
                'price_timestamp': 't0',
                'price_index': '100',
            }
            return report
        
        report = {
            'price_timestamp': str(record.timestamp),
            'price_index': str(record.price_index),
        }

        return report

    def _calculate_index(self, message: dict):
        for price in message['prices']:
            record = {}
            record.update(price)
            record['timestamp']=message['timestamp']
            if message['timestamp'] == 't0':
                record['index_return']=0
                record['price_index']=100
            else:
                if last_record['id'] == record['id']:
                    security_return = (record['price']/last_record['price'] - 1)
                    record['index_return'] = last_record['index_return'] + record['weight']*security_return
                    record['price_index'] = last_record['price_index'] * (1 + record['index_return'])
            last_record=record
            self._insert_new_price(record)

        

    @staticmethod
    def _deserialise_message(message: bytes) -> dict:
        return json.loads(message.decode('utf-8'))

    def _insert_new_price(self, price: dict):
        
        price_db_record = Price(
            id=price['id'],
            price=str(price['price']),
            timestamp=price['timestamp'],
            weight=str(price['weight']),
            price_index= str(price['price_index']),
            index_return=str(price['index_return']))

        with self.db.get_session() as session:
            try:
                session.add(price_db_record)
                session.commit()
            except IntegrityError:
                logging.info('Duplicated event')

        logging.info(f'New price inserted')

    
