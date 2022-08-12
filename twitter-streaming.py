import tweepy
import configparser
import time
import json
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

servers = 'localhost:9092,localhost:9093,localhost:9094'
producer = Producer({
'bootstrap.servers': servers,
'partitioner': 'consistent_random',
'security.protocol': 'plaintext'
})

def topics_config(topics, servers):
  """Create and configure topics for the cluster."""
  # Instantiation 
  a = AdminClient({'bootstrap.servers': servers})

  # Setting topics config
  # I.e., each topic is to have three partitions and its data a replication factor of three
  topics = [NewTopic(topic, num_partitions=3, replication_factor=3) for topic in topics]

  # Use create_topics to create the topics
  # Returns a dict of topic:future
  fs = a.create_topics(topics, request_timeout=15.0)
  print(fs)

  #
  for topic, f in fs.items():
    try:
      print(f.result()) # returns None
      print(f"Topic {topic} successfully created.")
    except Exception as e:
      print(f"Failed to create topic {topic} -- {e}.")
#topics_config(['twitter'], servers)

# print(producer)
# topic_info = producer.list_topics()
# print(topic_info.cluster_id)
# print(topic_info.brokers)
# print(topic_info.topics)
# print(topic_info.controller_id)

def send_message(data, name_topic, id):
  """Begin sending messages and assign every message a key
     based on the tweet ID.
  """
  producer.produce(topic=name_topic, value=data, 
                   key=f"{name_topic[:2].upper()}{id}".encode('utf-8'))


# TWEEPY

config = configparser.ConfigParser()
config.read('config.ini')

api_key = config['twitter']['api_key']
api_key_secret = config['twitter']['api_key_secret']
bearer_token = config['twitter']['bearer_token']
access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']

client = tweepy.Client(bearer_token, api_key, api_key_secret, access_token, access_token_secret)
auth = tweepy.OAuth1UserHandler(api_key, api_key_secret, access_token, access_token_secret)
api = tweepy.API(auth)

search_terms = ["scala", "programming", "coding"]


class Listener(tweepy.StreamingClient):
  def on_connect(self):
    print("Connected")

  def on_data(self, data):
    print("\n")
    print(data)
    print("-------------------------------------------------")
    # if 'matching_rules' in message:
    #   for rule in message['matching_rules']:
    message = json.loads(data)
    send_message(data, name_topic='twitter', id=message['data']['id'])
    time.sleep(0.2)

stream = Listener(bearer_token=bearer_token)

# stream.add_rules(tweepy.StreamRule(search_terms))
# for term in search_terms:
#   stream.add_rules(tweepy.StreamRule(term))


stream.filter(tweet_fields=['referenced_tweets', 'author_id'])


# def get_all_rule_ids():
#   rules = str(stream.get_rules().data)
#   a_list = rules.split(", ")

#   subs = "id"
#   ids_str_list = [s for s in a_list if subs in s]

#   ids_list = []
#   for string in ids_str_list:
#     ids_list.append(int(''.join(filter(str.isdigit, string))))
#   return ids_list

# def delete_all_rules():
#   for id in get_all_rule_ids():
#     stream.delete_rules(ids=id)

# print(stream.get_rules())
# print("\n")
# delete_all_rules()
# print("\n")
# print(stream.get_rules())