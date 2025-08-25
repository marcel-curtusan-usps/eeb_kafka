# eeb_kafka

eeb kafka consumer and producer

A Python project with basic Apache Kafka producer and consumer examples for local development and testing.

## Requirements

- Python 3.8+
- Apache Kafka (local or remote)

## Setup

1. (Optional) Create and activate a virtual environment:

   ```powershell
   python -m venv venv
   .\venv\Scripts\activate
   ```

2. Install dependencies:

   ```powershell
   pip install -r requirements.txt
   ```

## Usage

- Run the producer:

  ```powershell
  python producer.py
  ```

- Run the consumer:

  ```powershell
  python consumer.py
  ```

## Notes

- Update Kafka broker and topic settings in the scripts as needed for your environment.
