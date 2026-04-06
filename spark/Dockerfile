FROM spark:4.0.1-java21-python3

USER root

RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    lz4

RUN apt-get update && apt-get install -y netcat

ENV SPARK_HOME=/opt/spark \
    PYTHONHASHSEED=1

WORKDIR /opt/spark-apps

RUN mkdir -p /opt/spark/logs

COPY start-spark.sh /
RUN chmod +x /start-spark.sh

CMD ["/bin/bash", "/start-spark.sh"]