from logging import exception
from confluent_kafka import Producer
from confluent_kafka import KafkaException, KafkaError

def run_producer():
  p = Producer({
  'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
  # 'security_protocol': 'plaintext',
  # 'acks': '-1',
  # 'partitioner': 'consistent_random'
  })

  print(p)
  
  topic_info = p.list_topics()
  
  print(topic_info.cluster_id)
  print(topic_info.brokers)
  print(topic_info.topics)
  print(topic_info.controller_id)
  

run_producer()