FROM apache/spark-py:latest

USER root

WORKDIR /app

COPY app/ /app/app/
COPY DataCoSupplyChainDataset.csv /app/data/DataCoSupplyChainDataset.csv
COPY fuel.csv /app/data/fuel.csv

RUN mkdir -p /app/output && chmod -R 777 /app

CMD ["/opt/spark/bin/spark-submit", "/app/app/spark_app.py"]
