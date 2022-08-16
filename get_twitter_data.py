#import config
import os

import pymongo

import tweepy
import json
import time

from datetime import datetime, timedelta


def get_client():
    """Returns Twitter API v2 Client object."""
    #client = tweepy.Client(bearer_token=config.BEARER_TOKEN)
    client = tweepy.Client(bearer_token=os.environ.get('TWITTER_TOKEN'))
    return client


mongo_client=pymongo.MongoClient('mongodb://localhost:27017/')

def get_search_words(company):
    """Gets list of words of interest, defined by each company,
    from a MongoDB database and returns this list.

    :param company: Name of company 
    :type company: str
    :return: A list of words
    :rtype: list
    """
    db = mongo_client['rep_analysis_test'] # database rep_analysis_test
    search_words = db['search_words'] # collection search_words

    my_query = {"company": company}
    for words in search_words.find(my_query, {"_id": 0, "company": 0}):
        return words['words']


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


def get_tweets_mongo(company, start_time=None, end_time=None, max_results=100):
    """Saves Twitter data in a MongoDB database.

    :param company: Name of company
    :type company: str
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

    db = mongo_client['rep_analysis_test'] # database rep_analysis_test
    keywords = db['keywords'] # collection keywords

    for word in get_search_words(company):
        query = word + " -is:retweet lang:en"

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
            keywords.insert_one(tweet.data) # the data attribute of each tweet is a dictionary


if __name__ == '__main__':

    # test for a given query and a time interval of 30 minutes
    """
    query = "McDonalds -is:retweet lang:en"
    start_time=(datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d') + 'T23:00:00Z' # yesterday at 23:00:00
    end_time=(datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d') + 'T23:30:00Z' # yesterday at 23:30:00

    get_tweets(query=query, start_time=start_time, end_time=end_time)

    print('Success!')
    """

    # test for Vodafone and a time interval of 10 minutes
    start_time=(datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d') + 'T23:00:00Z' # yesterday at 23:00:00
    end_time=(datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d') + 'T23:30:00Z' # yesterday at 23:10:00

    get_tweets_mongo(company="Vodafone", start_time=start_time, end_time=end_time)

    print('Success!')