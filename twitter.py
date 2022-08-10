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

search_terms = ['"data engineer"', 'remote', 'UK']

class Listener(tweepy.StreamingClient):
  def on_connect(self):
    print("Connected")

  def on_tweet(self, tweet):
    if tweet.referenced_tweets == None:
      print(tweet.text)
      time.sleep(1)

stream = Listener(bearer_token=bearer_token)

for term in search_terms:
  stream.add_rules(tweepy.StreamRule(term))

stream.filter(tweet_fields=['referenced_tweets'])

  