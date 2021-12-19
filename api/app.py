import logging
from flask import Flask, Response
from flask_nameko import FlaskPooledClusterRpcProxy
import csv
import json
import ast
import atexit
import pandas as pd

df = pd.read_csv('Data.csv', header = 1)


rpc = FlaskPooledClusterRpcProxy()
sample_dict = [{
    'Id': 1,
    'weights': 1,
    'price': 1,
    'timestamp': 't0'
},
{
    'Id': 2,
    'weights': 2,
    'price': 2,
    'timestamp': 't0'
},
{
    'Id': 3,
    'weights': 2,
    'price': 3,
    'timestamp': 't0'
},
{
    'Id': 4,
    'weights': 1,
    'price': 4,
    'timestamp': 't0'
},
{
    'Id': 1,
    'weights': 1,
    'price': 5,
    'timestamp': 't1'
},
{
    'Id': 2,
    'weights': 2,
    'price': 6,
    'timestamp': 't1'
},
{
    'Id': 3,
    'weights': 2,
    'price': 9,
    'timestamp': 't1'
},
{
    'Id': 4,
    'weights': 1,
    'price': 8,
    'timestamp': 't1'
},
{
    'Id': 1,
    'weights': 1,
    'price': 10,
    'timestamp': 't2'
},
{
    'Id': 2,
    'weights': 2,
    'price': 18,
    'timestamp': 't2'
},
{
    'Id': 3,
    'weights': 2,
    'price': 18,
    'timestamp': 't2'
},
{
    'Id': 4,
    'weights': 1,
    'price': 8,
    'timestamp': 't2'
}]

def load_data(df):
    df = df.rename(columns={'Unnamed: 0': 'id'})
    df = df.rename(columns={'Prices' : 'Unnamed: 2'})
    prices_df = df[df.columns[2:]].rename(columns=lambda x: "t" + str(int(str(x[8:]))-2)).transpose()
    weights = df[df.columns[1]]
    price_dictionary = prices_df.to_dict(orient='index')
    result = []
    for index, value in price_dictionary.items():
        for k, v in value.items():
            record = {
                'id' : k+1,
                'price': value[k],
                'weight': weights[k],
                'timestamp': index
            }
            result.append(record)
                        
    return result



def create_app():
    app = Flask(__name__)
    app.config.update(dict(
        NAMEKO_AMQP_URI='amqp://rabbit'
        )
    )

    rpc.init_app(app)
    
    return app

app = create_app()
input_data_reader= load_data(df)

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
    n=0
    for row in input_data_reader:
        if n >= num_prices:
            break
        message = {
            'id': row.get('id',''),
            'weight':row.get('weight',''),
            'price': row.get('price',''),
            'timestamp': row.get('timestamp','')
        }
        rpc.priceevents.send(message)
        n += 1
    return Response(f'Sent {n} new events', status=200)

    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)