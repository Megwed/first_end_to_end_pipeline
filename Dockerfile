FROM apache/airflow:2.6.3-python3.8
COPY requirements.txt  requirements.txt
USER airflow
# Declare the ARG for credentials content
ARG GOOGLE_APPLICATION_CREDENTIALS_CONTENT
# Set the ENV for the path where the credentials file will be located inside the container
ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/application_default_credentials.json
# Write the credentials content to the specified location
# Installing requirements
RUN pip install -r requirements.txt