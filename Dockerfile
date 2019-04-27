
FROM python:2.7

COPY . /var/app/worker

WORKDIR /var/app/worker

RUN pip install -r requirements.txt