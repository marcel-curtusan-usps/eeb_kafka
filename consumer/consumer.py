from confluent_kafka import KafkaException
from dotenv import load_dotenv
load_dotenv()
import uuid
import argparse
import os
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.avro import AvroDeserializer
from shared.oauth_schema_registry import get_client_credentials_schema_registry_client
from shared.shared_function import get_env_variable

    # Add Confluent Cloud security config
client_id = get_env_variable('KAFKA_CLIENT_ID')
client_secret = get_env_variable('KAFKA_CLIENT_SECRET')
sasl_mechanism = get_env_variable('BEARER_AUTH_CREDENTIALS_SOURCE')
bootstrap_servers = get_env_variable('KAFKA_BOOTSTRAP_SERVERS')
schema_registry_url = get_env_variable('SCHEMA_REGISTRY_URL')
connection_retry_seconds = get_env_variable('CONNECTION_RETRY_SECONDS')
topic = get_env_variable('KAFKA_TOPIC')

class User(object):
    def __init__(self, name=None, favorite_number=None, favorite_color=None):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color

def dict_to_user(obj, ctx):
    if obj is None:
        return None
    return User(name=obj['name'],
                favorite_number=obj['favorite_number'],
                favorite_color=obj['favorite_color'])


import logging
logging.basicConfig(level=logging.INFO)

# Kafka connection check function at module level
def check_kafka_connection(consumer_conf):
    from confluent_kafka import KafkaException
    logging.info("\n--- Kafka Connection Diagnostics ---")
    logging.info(f"bootstrap.servers: {consumer_conf['bootstrap.servers']}")
    logging.info(f"security.protocol: {consumer_conf['security.protocol']}")
    logging.info(f"sasl.mechanism: {consumer_conf['sasl.mechanism']}")
    logging.info(f"sasl.username: {consumer_conf.get('sasl.username', 'N/A')}")
    # Do not print password for security
    try:
        consumer = Consumer(consumer_conf)
        logging.info("Attempting to fetch Kafka metadata...")
        md = consumer.list_topics(timeout=10)
        logging.info(f"Kafka connection successful. Topics available: {list(md.topics.keys())}")
        consumer.close()
        return True
    except KafkaException as e:
        import traceback
        logging.error("Kafka connection failed!")
        logging.error(f"Exception type: {type(e).__name__}")
        logging.error(f"Exception message: {e}")
        logging.error("Traceback:")
        traceback.print_exc()
        logging.error("\nTROUBLESHOOTING TIPS:")
        logging.error("- Check that your bootstrap.servers is correct and reachable from this machine.")
        logging.error("- If using Confluent Cloud, ensure your SASL credentials are correct and have access.")
        logging.error("- If behind a firewall or proxy, ensure ports are open (default: 9092 for PLAINTEXT, 9093 for SASL_SSL).")
        logging.error("- If using OAUTHBEARER, check your token endpoint and credentials.")
        logging.error("- Try running 'ping <broker>' or 'telnet <broker> <port>' to verify network connectivity.")
        logging.error("- If error is SSL related, check your CA certificates and SSL config.")
        logging.error("- For more details, set 'debug': 'all' in the config and rerun.")
        return False
    except Exception as e:
        import traceback
        logging.error("Kafka connection failed (unexpected error)!")
        logging.error(f"Exception type: {type(e).__name__}")
        logging.error(f"Exception message: {e}")
        logging.error("Traceback:")
        traceback.print_exc()
        return False

def main():
    # Log OAUTHBEARER config values (do not log secrets in production)
    logging.info(f"OAUTHBEARER client_id: {client_id}")
    logging.info(f"OAUTHBEARER using SASL mechanism: {sasl_mechanism}")
    # Optionally, log identity pool and logical cluster if you use them
    # logging.info(f"OAUTHBEARER identity_pool_id: {identity_pool_id}")
    # logging.info(f"OAUTHBEARER logical_cluster: {logical_cluster}")


    group = str(uuid.uuid1())
    schema_path = os.path.join(os.path.dirname(__file__), '../avro/user_generic.avsc')
    with open(schema_path) as f:
        schema_str = f.read()


    # Retry schema registry client creation until successful
    import time
    # Default retry interval to 30 seconds if not set or empty
    retry_seconds = 30
    try:
        if connection_retry_seconds is not None and str(connection_retry_seconds).strip() != "":
            retry_seconds = float(connection_retry_seconds)
    except Exception:
        logging.warning("Invalid CONNECTION_RETRY_SECONDS value, defaulting to 30 seconds.")
        retry_seconds = 30

    schema_registry_client = None
    while schema_registry_client is None:
        schema_registry_client = get_client_credentials_schema_registry_client()
        if schema_registry_client is None:
            logging.error("Schema Registry connection failed. Retrying in %s seconds...", retry_seconds)
            time.sleep(retry_seconds)

    # AvroDeserializer signature: (schema_str, from_dict, schema_registry_client)
    avro_deserializer = AvroDeserializer(
        schema_str,
        dict_to_user,
        schema_registry_client
    )

    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group,
        'auto.offset.reset': "earliest",
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': get_env_variable('BEARER_AUTH_CREDENTIALS_SOURCE'),
        'sasl.oauthbearer.token.endpoint.url': get_env_variable('SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL'),
        'sasl.oauthbearer.client.id': client_id,
        'sasl.oauthbearer.client.secret': client_secret,
        'sasl.oauthbearer.scope': get_env_variable('BEARER_AUTH_SCOPE'),
        'bearer.auth.credentials.source': get_env_variable('BEARER_AUTH_CREDENTIALS_SOURCE'),
        'bearer.auth.issuer.endpoint.url': get_env_variable('BEARER_AUTH_ISSUER_ENDPOINT_URL'),
        'bearer.auth.client.id': client_id,
        'bearer.auth.client.secret': client_secret,
        'bearer.auth.scope': get_env_variable('BEARER_AUTH_SCOPE'),
        'bearer.auth.logical.cluster': get_env_variable('BEARER_AUTH_LOGICAL_CLUSTER'),
        'bearer.auth.identity.pool.id': get_env_variable('BEARER_AUTH_IDENTITY_POOL_ID'),
        'isolation.level': 'read_committed',
        'specific.avro.reader': True
    }
    import time
    # Default retry interval to 30 seconds if not set or empty
    retry_seconds = 30
    try:
        if connection_retry_seconds is not None and str(connection_retry_seconds).strip() != "":
            retry_seconds = float(connection_retry_seconds)
    except Exception:
        logging.warning("Invalid CONNECTION_RETRY_SECONDS value, defaulting to 30 seconds.")
        retry_seconds = 30

    while not check_kafka_connection(consumer_conf):
        logging.error(f"Kafka connection failed. Retrying in {retry_seconds} seconds...")
        time.sleep(retry_seconds)

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    logging.info("Listening for Avro messages...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            try:
                user = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                if user is not None:
                    # user may be a User object or dict depending on deserializer
                    if isinstance(user, dict):
                        logging.info(f"User record {msg.key()}: name: {user.get('name')}\n\tfavorite_number: {user.get('favorite_number')}\n\tfavorite_color: {user.get('favorite_color')}\n")
                    else:
                        logging.info(f"User record {msg.key()}: name: {getattr(user, 'name', None)}\n\tfavorite_number: {getattr(user, 'favorite_number', None)}\n\tfavorite_color: {getattr(user, 'favorite_color', None)}\n")
            except Exception as msg_exc:
                import traceback
                logging.error("Error while polling/processing message:")
                logging.error(f"Exception type: {type(msg_exc).__name__}")
                logging.error(f"Exception message: {msg_exc}")
                traceback.print_exc()
                logging.error("Continuing to next message...")
    except KeyboardInterrupt:
        logging.info("Aborted by user.")
    finally:
        consumer.close()
        logging.info("Kafka consumer closed.")

# Run main if this script is executed directly
if __name__ == "__main__":
    main()
