FROM continuumio/anaconda3:2019.10
LABEL author="Daniel Junior <danieljunior@id.uff.br>"
USER root

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN mkdir -p /app
WORKDIR /app

COPY requirements.txt .
RUN /opt/conda/bin/pip install -r requirements.txt

# RUN apt-get update && apt-get install postgresql-dev gcc musl-dev
RUN rm -rf /tmp/* /var/tmp/* && apt-get clean
