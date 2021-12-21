from typing import Dict, Iterator
from flask import Flask
from flask.wrappers import Response
from flask_nameko import FlaskPooledClusterRpcProxy
import json
import pandas as pd
import logging

df = pd.read_csv('Data.csv', header = 1)

rpc = FlaskPooledClusterRpcProxy()

def load_data(df) -> Iterator[Dict]:
    df = df.rename(columns={'Unnamed: 0': 'id'})
    df = df.rename(columns={'Prices' : 'Unnamed: 2'})
    prices_df = df[df.columns[2:]].rename(columns=lambda x: "t" + str(int(str(x[8:]))-2)).transpose()
    weights = df[df.columns[1]]
    prices_dictionary = prices_df.to_dict(orient='index')
    time=0.0
    for k, v in prices_dictionary.items():
        prices = []
        for i in range(len(weights)):
            
            prices.append({'price': v[i], 'id': i+1, 'weight': weights[i]})
        record = {
            'prices': prices,
            'timestamp': 't' + str(int(time))
        }
        time+=0.5
        logging.info(f'record received: {record}')
        yield record


def create_app():
    app = Flask(__name__)
    app.config.update(dict(NAMEKO_AMQP_URI='amqp://rabbit'))
    rpc.init_app(app)
    return app

app = create_app()
input_data_iterator = load_data(df)

@app.route('/healthcheck')
def healthcheck():
    return 'All good from api!'

@app.route('/report', methods=["GET"])
def get_report():
    result = rpc.pricestats.report()
    return Response(json.dumps(result), status=200)

@app.route('/submit/<int:num_prices>', methods=["GET"])
def submit(num_prices):
    """ Entry point to submit prices """
    n = 0
    batch_size = 100
    batch = []
    for row in input_data_iterator:
        if n >= num_prices:
            break
        message = {
            'prices': row.get('prices',''),
            'timestamp': row.get('timestamp','')
        }
        batch.append(message)
        if not n % batch_size:
            rpc.priceevents.send(batch)
            batch = []
        n += 1

    if batch:
        rpc.priceevents.send(batch)
    
    return Response(f'Sent {n} new events', status=200)

    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)