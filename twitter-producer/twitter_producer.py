import tweepy
import configparser
import time
import json
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

servers = 'localhost:29092,localhost:29093,localhost:29094'
producer = Producer({
'bootstrap.servers': servers,
'partitioner': 'consistent_random',
'security.protocol': 'plaintext'
})

def topics_config(topics, servers):

  a = AdminClient({'bootstrap.servers': servers})

  topics = [NewTopic(topic, num_partitions=3, replication_factor=3) for topic in topics]

  fs = a.create_topics(topics, request_timeout=15.0)

  for topic, f in fs.items():
    try:
      f.result() # returns None
      print(f"Topic {topic} successfully created.")
    except Exception as e:
      print(f"Failed to create topic {topic} -- {e}.")

def send_message(data, name_topic, id):

  producer.produce(topic=name_topic, value=data, 
                   key=f"{name_topic[:2].upper()}{id}".encode('utf-8'))


# TWEEPY

def check_rules(bearer_token, rules, tags):

  def add_rules(client, rules, tags):
    for rule, tag in zip(rules, tags):
      client.add_rules(tweepy.StreamRule(value=rule, tag=tag))

  client = tweepy.StreamingClient(bearer_token, wait_on_rate_limit=False)
  if client.get_rules()[3]['result_count'] != 0:
    n_rules = client.get_rules()[0]
    ids = [n_rules[i_tuple[0]][2] for i_tuple in enumerate(n_rules)]
    client.delete_rules(ids)
    add_rules(client, rules, tags)
  else:
    add_rules(client, rules, tags)

class Listener(tweepy.StreamingClient):
  def on_connect(self):
    print("Connected")

  def on_data(self, data):
    print("\n")
    print(data)
    print("-------------------------------------------------")
    message = json.loads(data)
    if 'matching_rules' in message:
      for rule in message['matching_rules']:
        send_message(data, name_topic=rule['tag'], id=message['data']['id'])
    else:
      print("Operational error, reconnecting...")
    return True
   
  def on_error(self, status):
    print(status)
  