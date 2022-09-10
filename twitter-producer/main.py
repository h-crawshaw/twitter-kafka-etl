from twitter_producer import check_rules, topics_config, Listener, servers
import configparser
import os


query = " -is:retweet -has:hashtags"
rules = [f"putin {query}",
    f"zelensky {query}",
    f"biden {query}",
    f"nato {query}"]
tags = ["putin", "zelensky", "biden", "nato"]


config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
config = configparser.ConfigParser()
config.read(config_file)
bearer_token = config['twitter']['bearer_token']


def main():
 check_rules(bearer_token, rules, tags)

 topics_config(topics=tags, servers=servers)
 
 Listener(bearer_token).filter(tweet_fields=['created_at'])


if __name__ == '__main__':
  main()

