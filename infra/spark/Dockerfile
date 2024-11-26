# Use the Bitnami Spark image as the base
FROM bitnami/spark:3.5.0

# Install Python dependencies
COPY ./etl/requirements.txt /opt/bitnami/spark/python/requirements.txt
RUN pip install --no-cache-dir -r /opt/bitnami/spark/python/requirements.txt

# Ensure `datalake` package is included
COPY ./etl/datalake /opt/bitnami/spark/python/datalake