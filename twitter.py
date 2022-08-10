import tweepy
import configparser
import time
import json
from pprint import pprint

config = configparser.ConfigParser()
config.read('config.ini')

api_key = config['twitter']['api_key']
api_key_secret = config['twitter']['api_key_secret']
bearer_token = config['twitter']['bearer_token']
access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']

client = tweepy.Client(bearer_token, api_key, api_key_secret, access_token, access_token_secret)
# auth
auth = tweepy.OAuth1UserHandler(api_key, api_key_secret, access_token, access_token_secret)
api = tweepy.API(auth)

search_terms = ["scala", "programming", "coding"]

class Listener(tweepy.StreamingClient):
  def on_connect(self):
    print("Connected")

  # def on_tweet(self, tweet):
  #   if tweet.referenced_tweets == None:
  #   #  print(tweet.author_id)
  #     user_data = client.get_user(id=tweet.data['author_id'])
  #    # print(user_data[])
  #     print(tweet.text)
  #     print(client.get_user(id=tweet.data['author_id']))
  #    # client.get_user(tweet.data['id'])
  #   #  print(tweet.created_at)
  #     print("-------------------------------------------------")
  #     time.sleep(0.2)
  def on_data(self, data):
    print("\n")
    msg = json.loads(data)
    pprint(msg)
    print(msg['data']['id'])
    print("-------------------------------------")



stream = Listener(bearer_token=bearer_token)

#stream.add_rules(tweepy.StreamRule(search_terms))
for term in search_terms:
  stream.add_rules(tweepy.StreamRule(term))

stream.filter(tweet_fields=['referenced_tweets', 'author_id'])


def get_all_rule_ids():
  rules = str(stream.get_rules().data)
  a_list = rules.split(", ")

  subs = "id"
  ids_str_list = [s for s in a_list if subs in s]

  ids_list = []
  for string in ids_str_list:
    ids_list.append(int(''.join(filter(str.isdigit, string))))
  return ids_list

def delete_all_rules():
  for id in get_all_rule_ids():
    stream.delete_rules(ids=id)

# print(stream.get_rules())
# print("\n")
# delete_all_rules()
# print("\n")
# print(stream.get_rules())
