import datetime
import json
import logging
from typing import Type

import yagmail
from nameko.dependency_providers import Config
from nameko.rpc import rpc
from nameko.timer import timer
from nameko_kafka import consume
from nameko_sqlalchemy import Database
from sqlalchemy import Column, String, create_engine, func, desc, Integer
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base

DeclarativeBase = declarative_base()
MAX_ID = 400
    
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
        
        price = self._deserialise_message(message=new_price_event.value)
        logging.info(f'Message recieved,{price}')
        record_id = f'{new_price_event.topic}-{new_price_event.offset}-{new_price_event.timestamp}'
        self._insert_new_price(record_id, price) 

    @rpc
    def report(self):
        """ Return price count by city """
        return self._get_report()


    def _calculate_security_return(self, price: dict):
        last = self._get_last_record_id(price)
        if last is None:
            return str(0)
        security_return = str(float(price['price'])/float(last.price) - 1)
        return security_return

    def _calculate_index_return(self, price: dict):
        last = self._get_last_record_time(price)
        security_return = self._calculate_security_return(price)

        if last is None:
            return {'value': str(float(price['weight'])*float(security_return))}
            
        index_return = {'value': str(float(last.index_return) + (float(price['weight'])*float(security_return)))}
        return index_return

    def _calculate_index(self, price: dict):
        last = self._get_last_record_id(price)
        if last is None:
            return {'value': 100}
        
        index_return = self._calculate_index_return(price)
        price_index = {'value': str(float(last.price_index) + float(last.price_index)*float(index_return['value']))}
        return price_index

    def _deserialise_message(self, message: bytes) -> dict:
        return json.loads(message.decode('utf-8'))

    def _insert_new_price(self, record_id: str, price: dict):
        price_index = self._calculate_index(price)
        index_return = self._calculate_index_return(price)
        price_db_record = Price(
            id=price['id'],
            price=str(price['price']),
            timestamp=price['timestamp'],
            weight=str(price['weight']),
            price_index= price_index['value'],
            index_return=index_return['value'])

        with self.db.get_session() as session:
            try:
                session.add(price_db_record)
                session.commit()
            except IntegrityError:
                logging.info('Duplicated event')

        logging.info(f'New price inserted')


    def _get_last_record_id(self, price):
        with self.db.get_session() as session:
            last = session.query(Price).filter(Price.id == price['id']).order_by(Price.seq_id.desc()).first()
            return last

    def _get_last_record_time(self, price):
        with self.db.get_session() as session:
            last = session.query(Price).filter(Price.timestamp == price['timestamp']).order_by(Price.seq_id.desc()).first()
            return last

    def _get_last_record(self):
        with self.db.get_session() as session:
            last = session.query(Price).filter(Price.id == MAX_ID).order_by(Price.seq_id.desc()).first()
            return last

        

    def _get_report(self):
        report_datetime = datetime.datetime.now()
        try:
            price_index = str(self._get_last_record().price_index)
        except:
            price_index = '100'
        report = {
            'timestamp': str(report_datetime),
            'price_index': price_index
        }

        return report
