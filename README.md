# PoC of events based application with Nameko, Kafka and Flask

## Architecture

### Services

- API (flask):
  - Make RPC calls to nameko services `priceevents` and `pricestats`.
  - Endpoints are synchronous. 
  TODO: They should be asynchronous and return a 202. 
- `priceevents`(nameko service):
  - exposes a RPC endpoint to submit event messages to kafka.
- `pricestats` (nameko service):
  - consumes messages from kafka
  - insert a new record into a sqlite database for each new price event

### Brokers

- Zookeeper: Required by kafka
- Kafka: Messages
- Rabbit: Async communication between services

## Run demo with docker compose

1. Run brokers with `docker-compose up --build broker rabbit zookeper`. Wait until they are ready (around 30 s).
2. In a separate terminal, run the services: `docker-compose up --build priceevents pricestats api`
3. Request a new report with `curl localhost:8000/report`
4. Submit new 10 prices: `curl localhost:8000/submit/10`. Number of new prices can be any integer.


## Automatic tests

Created one example of test for price event service. Run `docker-compose run --entrypoint=bash priceevents /var/price_events/run-tests.sh`
  
## Local development env

Developement enviroment can be created with `pipenv install`

