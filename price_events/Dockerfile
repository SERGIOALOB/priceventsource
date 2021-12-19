FROM python:3.8



WORKDIR /var/price_events/
COPY requirements.txt requirements.txt

RUN pip install -U -q pip
RUN pip install -q -r requirements.txt

COPY config.yml config.yml
COPY run.sh run.sh
COPY price_events.py price_events.py

EXPOSE 8000

CMD bash /var/price_events/run.sh