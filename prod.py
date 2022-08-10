from logging import exception
from confluent_kafka import Producer
from confluent_kafka import KafkaException, KafkaError

def run_producer():
  producer = Producer({
  'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
  'partitioner': 'consistent_random',
  'security.protocol': 'plaintext'
  })

  print(producer)
  
  topic_info = producer.list_topics()
  
  print(topic_info.cluster_id)
  print(topic_info.brokers)
  print(topic_info.topics)
  print(topic_info.controller_id)
  

run_producer()