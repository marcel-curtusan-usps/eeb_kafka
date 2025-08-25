import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic = 'test-topic'

for i in range(10):
    message = f"Message {i}".encode('utf-8')
    producer.send(topic, message)
    print(f"Sent: {message}")
    time.sleep(1)

producer.flush()
producer.close()
