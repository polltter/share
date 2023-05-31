import pymongo

import tweepy
import config

import time
from datetime import datetime, timedelta

import json
from bson.json_util import dumps
import requests

from get_client_info import get_analysis


mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')


def get_client():
    """Returns Twitter API v2 Client object."""
    client = tweepy.Client(bearer_token=config.BEARER_TOKEN)
    return client


ymd = '%Y-%m-%d'
language = 'en' # this will be defined by the client and will come from somewhere else

def get_tweets_mongo(start_time=None, end_time=None, max_results=100, lang=language):
    
    client = get_client()

    terms = []

    for word in search_words:
        if len(word.split()) > 1:
            terms.append('"' + word + '"') # to account for phrases (example: "artificial intelligence")
        else:
            terms.append(word)

    query_terms = ' '.join(terms)

    query = f"({query_terms}) -is:retweet lang:{lang}"

    source_dict = {'source': 'Twitter'}
    date = datetime.utcnow().strftime(ymd) # check if we define this here or leave it as a global variable
    extracted_at_dict = {'extracted_at': date}

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
        data = {**tweet.data, **source_dict, **extracted_at_dict} # the data attribute of each tweet is a dictionary
        data_twitter.insert_one(data)


today = datetime.today().strftime(ymd)
raw_url = "https://esg-maturity.com/api/v1/reputational/analysis-info-raw"

def post_data(tenant, collection):

    headers = {'Accept': 'application/json', 
               'Content-Type': 'application/json', 
               'Authorization': 'Bearer 2|IIvhcPW0VLmm11NXAuEVOxQMI1GLdyJ8cUntGzBB', 
               'X-Tenant': tenant}
    
    my_query = {"extracted_at": {"$eq": today}}
    cursor = collection.find(my_query, projection={'_id': False})
    data = json.dumps({"ainfo_id": analysis[0], "data": dumps(cursor), "extracted_at": today})
    # should I define extracted_at or is it automatically set?
    
    response = requests.post(raw_url, headers=headers, data=data)
    
    print(response.status_code)
    #print(response.json())


analysis_per_tenant = get_analysis()
### START TEST ###
del analysis_per_tenant['3201246a-67d0-4062-a387-39bc4558b3e1'][0:6] # to use only the MY_TEST_3 analysis
### END TEST ###

for tenant in analysis_per_tenant.keys():
    
    for analysis in analysis_per_tenant[tenant]:
        
        analysis_name = analysis[1]
        db = mongo_client[analysis_name]
        
        search_words = analysis[2]
        
        ### TWITTER ###
        #db['data_twitter'].drop() # in case we need to delete this collection
        data_twitter = db['data_twitter'] # collection data_twitter

        # get desired tweets for yesterday
        start_time=(datetime.today()-timedelta(days=1)).strftime(ymd) + 'T00:00:00Z' # yesterday at 00:00:00
        end_time=(datetime.today()-timedelta(days=1)).strftime(ymd) + 'T23:59:59Z' # yesterday at 23:59:59
        
        get_tweets_mongo(start_time=start_time, end_time=end_time)

        post_data(tenant, data_twitter)

        ### OTHER SOURCES ###
        # ...


if __name__ == '__main__':

    #print(analysis_per_tenant)
    #print(analysis_name)
    #print(search_words)

    #print(mongo_client.list_database_names())
    #print(db.list_collection_names())
    
    print(data_twitter.count_documents({}))