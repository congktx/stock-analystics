FROM ubuntu:24.04

RUN apt update &&\
    apt upgrade &&\
    apt install -y python3-pip python3-venv

RUN mkdir /workspace &&\
    mkdir /workspace/airflow
WORKDIR /workspace/airflow

ENV AIRFLOW_HOME=/workspace/airflow

ENTRYPOINT ["tail", "-f", "/dev/null"]
