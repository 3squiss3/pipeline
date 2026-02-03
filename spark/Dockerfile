FROM bitnami/spark:3.5.6
USER root
RUN pip install pymongo numpy pandas
WORKDIR /opt/spark-app
COPY jobs/ /opt/spark-app/jobs/
USER 1001
