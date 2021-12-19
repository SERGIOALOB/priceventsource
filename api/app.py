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

def load_data(df):
    df = df.rename(columns={'Prices' : 'Unnamed 2'})
    prices_df=df[df.columns[2:]].rename(columns=lambda x: "t" + str(int(str(x[8:]))-2))
    pricesdict = prices_df.to_dict(orient='records') 
    weights = df.drop(df.columns[2:], axis=1).to_dict(orient='records')
    prices = [] 
    for index in range(len(pricesdict)): 
        for key, value in pricesdict[index].items(): 
            record = { 
                'timestamp': key, 
                'price': value
            
    }
            prices.append(record)
    result = []
    for i in range(len(weights)): 
        for j in range(len(pricesdict)):
            record ={**prices[j],**weights[i]}
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
            'id': row.get('Id',''),
            'weight':row.get('weights',''),
            'price': row.get('price',''),
            'timestamp': row.get('timestamp','')
        }
        rpc.priceevents.send(message)
        n += 1
    return Response(f'Sent {n} new events', status=200)

    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)