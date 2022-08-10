from typing import List
import tweepy
import configparser
import time

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

search_terms = ['data engineer',  'remote',  'job']

class Listener(tweepy.StreamingClient):
  def on_connect(self):
    print("Connected")

  def on_tweet(self, tweet):
    if tweet.referenced_tweets == None:
      print("\n")       #pretty
      print(tweet.text)
      print("------------------------------------------------------------")
      time.sleep(0.2)


stream = Listener(bearer_token=bearer_token)

for term in search_terms:
  stream.add_rules(tweepy.StreamRule(term))

# stream.filter(tweet_fields=['referenced_tweets'])


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
