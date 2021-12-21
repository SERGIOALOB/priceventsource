import datetime
import json
import logging
from typing import Dict, Optional
from decimal import Decimal

from nameko.dependency_providers import Config
from nameko.rpc import rpc
from nameko_kafka import consume
from nameko_sqlalchemy import Database
from sqlalchemy import Column, String, create_engine, func, desc, Integer
from sqlalchemy.ext.declarative import declarative_base

DeclarativeBase = declarative_base()


class Price(DeclarativeBase):
    __tablename__ = "prices"
    index = Column(String, primary_key=True)
    weight = Column(String)
    price = Column(String)
    timestamp = Column(Integer, primary_key=True)

    def __init__(
        self,
        index,
        weight,
        price,
        timestamp,
    ):
        self.index = index
        self.weight = weight
        self.price = price
        self.timestamp = timestamp


class PriceIndex(DeclarativeBase):
    __tablename__ = "price_index"
    price = Column(String)
    index_return = Column(String)
    timestamp = Column(Integer, primary_key=True)

    def __init__(self, price, timestamp, index_return):
        self.price = price
        self.timestamp = timestamp
        self.index_return = index_return


engine = create_engine("sqlite:///data.sqlite", echo=True)
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
        if not message:
            return

        logging.info(f"Message received: {message}")
        prices = message.get("prices", {})
        timestamp = message["timestamp"]
        for index, record in prices.items():
            price = {
                "index": index,
                "weight": record["weight"],
                "price": record["price"],
                "timestamp": timestamp,
            }
            self._insert_new_price(price)

        if timestamp == 0:
            logging.info("Timestamp is 0. Inserting index price = 100")
            self._insert_index_return(timestamp=0, index_price=100, index_return=0)
            return

        previous_timestamp = timestamp - 1
        record_previous_t = self._get_prices_at_timestamp(previous_timestamp)
        previous_index_price = self._get_index_at_timestamp(previous_timestamp)

        if not record_previous_t:
            logging.info(f"Unable to find previous prices for timetamp = {timestamp}")
            return

        if not previous_index_price:
            logging.info(
                f"Unable to find previous index price for timetamp = {timestamp}"
            )
            return

        index_return_t = self._calculate_return_of_the_index_on_date_t(
            record_previous_t, message
        )
        if not index_return_t:
            logging.info(
                f"Unable to compute index price at timestamp {timestamp}. Previous price: {record_previous_t}. Current price: {message}"
            )
            return

        index_price_t = previous_index_price["price"] * (1 + index_return_t)
        self._insert_index_return(
            timestamp=timestamp, index_price=index_price_t, index_return=index_return_t
        )

    @rpc
    def report(self, timestamp):
        """ Return price index """
        return self._get_report(timestamp)

    def _get_report(self, timestamp: int):
        record = self._get_index_at_timestamp(timestamp)
        if not record:
            return {}

        report = {
            "timestamp": str(record["timestamp"]),
            "price": str(record["price"]),
        }

        return report

    def _get_prices_at_timestamp(self, timestamp: int) -> Optional[Dict]:
        logging.info(f"Retrieving prices for timestamp {timestamp}")
        with self.db.get_session() as session:
            prices_at_t = (
                session.query(Price).filter(Price.timestamp == timestamp).all()
            )

        prices_at_t_dict = {
            x.index: {"price": Decimal(x.price), "weight": Decimal(x.weight)}
            for x in prices_at_t
        }
        record_at_t = {
            "prices": prices_at_t_dict,
            "timestamp": timestamp,
        }
        return record_at_t

    def _get_index_at_timestamp(self, timestamp: int) -> Optional[Dict]:
        logging.info(f"Retrieving prices for timestamp {timestamp}")
        with self.db.get_session() as session:
            price_index_at_t = (
                session.query(PriceIndex)
                .filter(PriceIndex.timestamp == timestamp)
                .first()
            )

        if not price_index_at_t:
            logging.info(f"Not found index price at timestamp {timestamp}")
            return

        record_at_t = {
            "index_return": Decimal(price_index_at_t.index_return),
            "price": Decimal(price_index_at_t.price),
            "timestamp": timestamp,
        }
        logging.info(f"Found index price {record_at_t}")
        return record_at_t

    @staticmethod
    def _calculate_return_of_the_index_on_date_t(
        record_t0: dict, record_t1: dict
    ) -> Optional[Decimal]:
        prices_t0 = record_t0.get("prices", {})
        prices_t1 = record_t1.get("prices", {})
        indexes_t0 = prices_t0.keys()
        indexes_t1 = prices_t1.keys()

        if indexes_t0 != indexes_t1:
            return

        index_return_t = 0
        for index in indexes_t0:
            price_t0_i = prices_t0[index]
            price_t1_i = prices_t1[index]
            r_i = price_t1_i["price"] / price_t0_i["price"] - 1
            index_return_t += index_return_t + price_t1_i["weight"] * r_i

        return Decimal(index_return_t)

    @staticmethod
    def _deserialise_message(message: bytes) -> Optional[Dict]:
        try:
            record = json.loads(message.decode("utf-8"))
            prices = record["prices"]
            timestamp = record["timestamp"]
            record_parsed = {
                "prices": {
                    index: {
                        "price": Decimal(price_weight["price"]),
                        "weight": Decimal(price_weight["weight"]),
                    }
                    for index, price_weight in prices.items()
                },
                "timestamp": timestamp,
            }
        except (KeyError, ValueError):
            logging.warning(f"Unable to parse message: {message}")
            return
        return record_parsed

    def _insert_index_return(self, timestamp, index_price, index_return):
        index_db_record = PriceIndex(
            timestamp=timestamp, price=str(index_price), index_return=str(index_return)
        )

        with self.db.get_session() as session:
            price_index_at_t = (
                session.query(PriceIndex)
                .filter(PriceIndex.timestamp == timestamp)
                .first()
            )
            if not price_index_at_t:
                session.add(index_db_record)
                session.commit()
                logging.info(f"New index price inserted Timestamp={timestamp}")
                return

            logging.info(f"Updating index price at timestamp {timestamp}")
            price_index_at_t.price = index_db_record.price
            price_index_at_t.index_return = index_db_record.index_return
            session.commit()

    def _insert_new_price(self, price: dict):
        price_db_record = Price(
            index=price["index"],
            price=str(price["price"]),
            timestamp=price["timestamp"],
            weight=str(price["weight"]),
        )

        with self.db.get_session() as session:
            price_i_t = (
                session.query(Price)
                .filter(Price.timestamp == price_db_record.timestamp)
                .filter(Price.index == price_db_record.index)
                .first()
            )
            if not price_i_t:
                session.add(price_db_record)
                session.commit()
                logging.info(
                    f"New price record inserted. (timestamp = {price_db_record.timestamp}, index: {price_db_record.index})"
                )
                return

            logging.info(
                f"Updating prices at index: (timestamp = {price_db_record.timestamp}, index: {price_db_record.index})"
            )

            price_i_t.price = price_db_record.price
            price_i_t.weight = price_db_record.weight
            session.commit()
