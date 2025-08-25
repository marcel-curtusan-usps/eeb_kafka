import os
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import azure.functions as func

def main(mytimer: func.TimerRequest) -> None:
    # Load config from environment variables
    bootstrap_servers = os.environ['KAFKA_BOOTSTRAP_SERVERS']
    schema_registry_url = os.environ['SCHEMA_REGISTRY_URL']
    topic = os.environ['KAFKA_TOPIC']
    group = os.environ['KAFKA_GROUP_ID']
    schema_path = os.path.join(os.path.dirname(__file__), '../avro/user_generic.avsc')
    with open(schema_path) as f:
        schema_str = f.read()

    sr_conf = {'url': schema_registry_url}
    schema_registry_client = SchemaRegistryClient(sr_conf)
    avro_deserializer = AvroDeserializer(
        schema_registry_client,
        schema_str,
        dict_to_user
    )

    client_id = os.environ['KAFKA_CLIENT_ID']
    client_secret = os.environ['KAFKA_CLIENT_SECRET']
    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group,
        'auto.offset.reset': "earliest",
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'OAUTHBEARER',
        'sasl.oauthbearer.client.id': client_id,
        'sasl.oauthbearer.client.secret': client_secret
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    msg = consumer.poll(1.0)
    if msg is not None:
        user = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        # Process user record here (e.g., log, store, etc.)
    consumer.close()

def dict_to_user(obj, ctx):
    if obj is None:
        return None
    return User(name=obj['name'],
                favorite_number=obj['favorite_number'],
                favorite_color=obj['favorite_color'])

class User(object):
    def __init__(self, name=None, favorite_number=None, favorite_color=None):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color
