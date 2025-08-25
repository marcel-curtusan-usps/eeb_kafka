


import logging
logging.basicConfig(level=logging.INFO)
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from shared.shared_function import get_env_variable

def get_client_credentials_schema_registry_client():
    """
    Returns a SchemaRegistryClient using client credentials OAuth config.
    Logs success or failure to retrieve schema subjects.
    """
    client_credentials_oauth_config = {
        'url': get_env_variable('SCHEMA_REGISTRY_URL'),
        'bearer.auth.credentials.source': get_env_variable('BEARER_AUTH_CREDENTIALS_SOURCE'),
        'bearer.auth.client.id': get_env_variable('SCHEMA_REGISTRY_CLIENT_ID'),
        'bearer.auth.client.secret': get_env_variable('SCHEMA_REGISTRY_CLIENT_SECRET'),
        'bearer.auth.scope': get_env_variable('SCHEMA_REGISTRY_SCOPE'),
        'bearer.auth.issuer.endpoint.url': get_env_variable('SCHEMA_REGISTRY_ISSUER_ENDPOINT_URL'),
        'bearer.auth.logical.cluster': get_env_variable('BEARER_AUTH_LOGICAL_CLUSTER'),
        'bearer.auth.identity.pool.id': get_env_variable('BEARER_AUTH_IDENTITY_POOL_ID')
    }
    try:
        client = SchemaRegistryClient(client_credentials_oauth_config)
        subjects = client.get_subjects()
        logging.info(f"[CLIENT CREDENTIALS] Successfully retrieved schema subjects: {subjects}")
        return client
    except Exception as e:
        logging.error(f"[CLIENT CREDENTIALS] Failed to retrieve schema subjects: {e}")
        return None

def main():
    static_oauth_config = {
        'url': get_env_variable('SCHEMA_REGISTRY_URL'),
        'bearer.auth.credentials.source': get_env_variable('BEARER_AUTH_CREDENTIALS_SOURCE'),
        'bearer.auth.token': get_env_variable('BEARER_AUTH_TOKEN'),
        'bearer.auth.logical.cluster': get_env_variable('BEARER_AUTH_LOGICAL_CLUSTER'),
        'bearer.auth.identity.pool.id': get_env_variable('BEARER_AUTH_IDENTITY_POOL_ID')
    }
    static_oauth_sr_client = SchemaRegistryClient(static_oauth_config)
    try:
        subjects = static_oauth_sr_client.get_subjects()
        logging.info(f"[STATIC TOKEN] Successfully retrieved schema subjects: {subjects}")
    except Exception as e:
        logging.error(f"[STATIC TOKEN] Failed to retrieve schema subjects: {e}")

    client_credentials_oauth_config = {
        'url': get_env_variable('SCHEMA_REGISTRY_URL'),
        'bearer.auth.credentials.source': get_env_variable('BEARER_AUTH_CREDENTIALS_SOURCE'),
        'bearer.auth.client.id': get_env_variable('SCHEMA_REGISTRY_CLIENT_ID'),
        'bearer.auth.client.secret': get_env_variable('SCHEMA_REGISTRY_CLIENT_SECRET'),
        'bearer.auth.scope': get_env_variable('SCHEMA_REGISTRY_SCOPE'),
        'bearer.auth.issuer.endpoint.url': get_env_variable('SCHEMA_REGISTRY_ISSUER_ENDPOINT_URL'),
        'bearer.auth.logical.cluster': get_env_variable('BEARER_AUTH_LOGICAL_CLUSTER'),
        'bearer.auth.identity.pool.id': get_env_variable('BEARER_AUTH_IDENTITY_POOL_ID')
    }

    client_credentials_oauth_sr_client = SchemaRegistryClient(client_credentials_oauth_config)
    try:
        subjects = client_credentials_oauth_sr_client.get_subjects()
        logging.info(f"[CLIENT CREDENTIALS] Successfully retrieved schema subjects: {subjects}")
    except Exception as e:
        logging.error(f"[CLIENT CREDENTIALS] Failed to retrieve schema subjects: {e}")


    def custom_oauth_function(config):
        return config

    custom_config = {
        'bearer.auth.token': get_env_variable('SCHEMA_REGISTRY_CUSTOM_TOKEN'),
        'bearer.auth.logical.cluster': get_env_variable('BEARER_AUTH_LOGICAL_CLUSTER'),
        'bearer.auth.identity.pool.id': get_env_variable('BEARER_AUTH_IDENTITY_POOL_ID')
    }

    custom_sr_config = {
        'url': get_env_variable('SCHEMA_REGISTRY_URL'),
        'bearer.auth.credentials.source': 'CUSTOM',
        'bearer.auth.custom.provider.function': custom_oauth_function,
        'bearer.auth.custom.provider.config': custom_config
    }

    custom_sr_client = SchemaRegistryClient(custom_sr_config)
    try:
        subjects = custom_sr_client.get_subjects()
        logging.info(f"[CUSTOM TOKEN] Successfully retrieved schema subjects: {subjects}")
    except Exception as e:
        logging.error(f"[CUSTOM TOKEN] Failed to retrieve schema subjects: {e}")

if __name__ == '__main__':
    main()
