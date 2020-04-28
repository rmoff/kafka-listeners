FROM python:3

# We'll add netcat cos it's a really useful
# network troubleshooting tool
RUN apt-get update
RUN apt-get install -y netcat

# Install the Confluent Kafka python library
RUN pip install confluent_kafka

# Add our script
ADD python_kafka_test_client.py /
ENTRYPOINT [ "python", "/python_kafka_test_client.py"]
