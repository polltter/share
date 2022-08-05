import config

import tweepy
import json
import time

from datetime import datetime, timedelta


def get_client():
    """Returns Twitter API v2 Client object."""
    client = tweepy.Client(bearer_token=config.BEARER_TOKEN)
    return client


def get_tweets(query, start_time=None, end_time=None, max_results=100):
    """Saves Twitter data in json format based on a given query.

    :param query: Input query for matching tweets
    :type query: str
    :param start_time: Timestamp (inclusive) from which the tweets will be provided - 
    defaults to None, which returns tweets from up to seven days ago
    :type start_time: datetime.datetime | str, optional
    :param end_time: Timestamp (exclusive) until which the tweets will be provided - 
    defaults to None, which returns tweets until as recent as 30 seconds ago
    :type end_time: datetime.datetime | str, optional
    :param max_results: Maximum number of search results to be returned by a request - 
    defaults to 100, the maximum possible value (it accepts values between 10 and 100)
    :type max_results: int, optional
    """
    client = get_client()

    with open('data/tweets.txt', mode = 'w') as file:

        start = time.time()
        
        for tweet in tweepy.Paginator(
                client.search_recent_tweets, 
                query=query, 
                start_time=start_time, 
                end_time=end_time, 
                max_results=max_results, 
                tweet_fields=['lang', 'created_at', 'public_metrics']).flatten():

            print(tweet.id)
            end = time.time()
            print(str((end - start)/60) + " minutes")
            tweet_json = json.dumps(tweet.data) # the data attribute of each tweet is a dictionary
            file.write(tweet_json + '\n')


if __name__ == '__main__':

    # test for a given query and a time interval of 30 minutes
    query = "McDonalds -is:retweet lang:en"
    start_time=(datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d') + 'T23:00:00Z' # yesterday at 23:00:00
    end_time=(datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d') + 'T23:30:00Z' # yesterday at 23:30:00

    get_tweets(query=query, start_time=start_time, end_time=end_time)

    print('Success!')