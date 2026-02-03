FROM spark:3.5.6-python3
USER root
RUN pip install --no-cache-dir pymongo numpy pandas
WORKDIR /opt/spark-app
COPY jobs/ /opt/spark-app/jobs/
USER 1001
