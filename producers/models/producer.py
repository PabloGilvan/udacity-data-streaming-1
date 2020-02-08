"""Producer base-class providing common utilites and functionality"""
import json
import logging
import time
from dataclasses import asdict

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)
BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.client = AdminClient({"bootstrap.servers": BROKER_URL})

        acks = (self.num_replicas - 1) if self.num_replicas > 2 else self.num_replicas
        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "client.id": f"producer_client_id_{self.topic_name.strip().replace(' ', '')}",
            "acks": acks
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

        self.producer = AvroProducer(
            self.broker_properties,
            schema_registry=schema_registry
        )

    def create_topic(self):
        topic_not_exists = self.client.list_topics(self.topic_name) is None
        if topic_not_exists:
            futures = self.client.create_topics(
                [
                    NewTopic(
                        topic_name=self.topic_name,
                        num_partitions=self.num_partitions,
                        replication_factor=self.num_replicas
                    )
                ]
            )

            for _, future in futures.items():
                try:
                    print(f"Topic created.: {self.topic_name}")
                    future.result()
                except Exception as e:
                    print(f"Error creating topcic: {self.topic_name}. Error.: {e}")
                    raise

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
