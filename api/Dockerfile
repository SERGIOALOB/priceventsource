FROM python:3.8

COPY requirements.txt /var/api/requirements.txt

WORKDIR /var/api/

RUN pip install -U -q pip
RUN pip install -q -r requirements.txt

COPY run.sh run.sh
COPY app.py app.py

EXPOSE 8000

CMD bash /var/api/run.sh