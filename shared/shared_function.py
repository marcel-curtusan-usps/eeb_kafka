import os
import logging

# Helper function for loading environment variables
def get_env_variable(key):
    value = os.getenv(key)
    if value is None:
        logging.error(f"Environment variable '{key}' not found.")
        raise ValueError(f"Environment variable '{key}' not found.")
    return value