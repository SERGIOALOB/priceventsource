from typing import Dict, Iterator, List
from flask import Flask
from flask.wrappers import Response
from flask_nameko import FlaskPooledClusterRpcProxy
import json
import pandas as pd
import logging
from decimal import Decimal

rpc = FlaskPooledClusterRpcProxy()


def load_data() -> List[Dict]:
    logging.info("Loading input data")
    df = pd.read_excel("data/Data_small.xlsx")
    del df["Id"]
    records = df.to_dict("records")
    weights = records[0]
    prices = records[1:]
    output = []
    for timestamp, record in enumerate(prices):
        indexes = weights.keys()
        prices = {
            key: {"price": Decimal(record[key]), "weight": Decimal(weights[key])}
            for key in indexes
        }
        price_record = {
            "prices": prices,
            "timestamp": timestamp,
        }
        output.append(price_record)
    logging.info("Input data loaded")
    return output


def create_app():
    app = Flask(__name__)
    app.config.update(dict(NAMEKO_AMQP_URI="amqp://rabbit"))
    rpc.init_app(app)
    return app


app = create_app()
input_data_iterator = load_data()


@app.route("/healthcheck")
def healthcheck():
    return "All good from api!"


@app.route("/report/<int:timestamp>", methods=["GET"])
def get_report(timestamp):
    result = rpc.pricestats.report(timestamp)
    return Response(json.dumps(result), status=200)


@app.route("/submit/<int:num_prices>", methods=["GET"])
def submit(num_prices):
    """ Entry point to submit prices """
    n = 0
    batch_size = 100
    batch = []
    for row in input_data_iterator:
        if n >= num_prices:
            break
        logging.warning(row)
        batch.append(row)
        if not n % batch_size:
            rpc.priceevents.send(batch)
            batch = []
        n += 1

    if batch:
        rpc.priceevents.send(batch)

    return Response(f"Sent {n} new events", status=200)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
