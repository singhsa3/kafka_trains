"""Producer base-class providing common utilites and functionality"""
import logging
import time
from dataclasses import dataclass, field


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic



from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient


logger = logging.getLogger(__name__)
SCHEMA_REGISTRY_URL = "http://localhost:8081"
BROKER_URL = "PLAINTEXT://localhost:9092"

@dataclass
class Producer:
    # Tracks existing topics across all Producer instances
    existing_topics = set([])
    """
    topic_name : str
    key_schema : str
    value_schema : str =field(default=None)
    num_partitions : int = field(default=1)
    num_replicas : int = field(default=1)
    broker_properties : dict = field(default_factory= {
            "bootstrap.servers": "PLAINTEXT://localhost:9092",
            "client.id": "ex4",
            "linger.ms": 1000,
            "compression.type": "lz4",
            "batch.num.messages": 100,
        })
        
    def __init__(self):
        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)
        # TODO: Configure the AvroProducer
        schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        self.producer = AvroProducer({"bootstrap.servers": BROKER_URL}, schema_registry=schema_registry)

    """
    """ Defines and provides common functionality amongst Producers"""


    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        #Initializes a Producer object with basic settings
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self. replication_factor = num_replicas
    
        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://localhost:9092",
            "client.id": "ex4",
            "linger.ms": 1000,
            "compression.type": "lz4",
            "batch.num.messages": 100,
        }
    

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        self.producer = AvroProducer({"bootstrap.servers": BROKER_URL}, schema_registry=schema_registry)


    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        client = AdminClient({"bootstrap.servers": self.broker_properties["bootstrap.servers"]})
        topic = NewTopic(self.topic_name, self.num_partitions, self.replication_factor)
        logger.info("topic creation kafka integration incomplete - skipping")
        client.create_topics([topic])

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        logger.info("producer close incomplete - skipping")
        print("producer close- skipping")
        self.producer.flush()
        #self.producer.close()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

    # test123