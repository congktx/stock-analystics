FROM ubuntu:24.04

RUN apt update &&\
    apt upgrade &&\
    apt install -y python3-pip python3-venv

RUN mkdir /workspace &&\
    mkdir /workspace/airflow

COPY ./java /workspace/java

WORKDIR /workspace/airflow
RUN mkdir /workspace/hadoop
ENV HADOOP_CONF_DIR=/workspace/hadoop
ENV YARN_CONF_DIR=/workspace/hadoop

ENV JAVA_HOME=/workspace/java
ENV PATH=$PATH:$JAVA_HOME/bin
ENV AIRFLOW_HOME=/workspace/airflow

ENTRYPOINT ["tail", "-f", "/dev/null"]
