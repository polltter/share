import pymongo

from datetime import datetime

import requests
import pandas as pd
import json
from bson.json_util import dumps

from get_client_info import get_analysis
from kw_extraction_api import clean_kw, get_keywords, agg_kw_daily
from sentiment_analysis_api import ml_sent, get_sentiment, agg_sentiment_daily
from emotion_analysis_api import ml_emotions, get_emotions, agg_emotions_daily


mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')


ymd = '%Y-%m-%d'
today = datetime.today().strftime(ymd)

search_raw_url = "https://esg-maturity.com/api/v1/reputational/analysis-info-raw/search"

def load_data(tenant):

    path = "/" + today

    headers = {'Accept': 'application/json',
               'Authorization': 'Bearer 2|IIvhcPW0VLmm11NXAuEVOxQMI1GLdyJ8cUntGzBB', 
               'X-Tenant': tenant}
        
    response = requests.get(search_raw_url + path, headers=headers)
    
    #print(response.status_code)
    #print(response.json())
    
    data_for_analysis = response.json()['data'][0]['data']
    
    df = pd.DataFrame(json.loads(data_for_analysis))

    return df


def load_tweets(df, language='en'):
   
    df_tweets = df[(df['source'] == 'Twitter') & (df['lang'] == language)].copy()

    df_tweets['retweets'] = df_tweets['public_metrics'].map(lambda x: x['retweet_count'])
    df_tweets['replies'] = df_tweets['public_metrics'].map(lambda x: x['reply_count'])
    df_tweets['likes'] = df_tweets['public_metrics'].map(lambda x: x['like_count'])
    df_tweets['quotes'] = df_tweets['public_metrics'].map(lambda x: x['quote_count'])

    df_tweets.drop(['public_metrics', 'edit_history_tweet_ids'], axis=1, inplace=True)

    return df_tweets


my_query = {"extracted_at": {"$eq": today}}

kw_url = "https://esg-maturity.com/api/v1/reputational/keywords-frequency"

def post_kw(tenant, collection):

    headers = {'Accept': 'application/json', 
               'Content-Type': 'application/json', 
               'Authorization': 'Bearer 2|IIvhcPW0VLmm11NXAuEVOxQMI1GLdyJ8cUntGzBB', 
               'X-Tenant': tenant}
    
    cursor = collection.find(my_query, projection={'_id': False})
    data = json.dumps({"ainfo_id": analysis[0], "data": dumps(cursor), "extracted_at": today})
    # should I define extracted_at or is it automatically set?
    
    response = requests.post(kw_url, headers=headers, data=data)
    
    print(response.status_code)
    #print(response.json())


kw_daily_url = "https://esg-maturity.com/api/v1/reputational/keywords-frequency-daily"

def post_kw_daily(tenant, collection):

    headers = {'Accept': 'application/json', 
               'Content-Type': 'application/json', 
               'Authorization': 'Bearer 2|IIvhcPW0VLmm11NXAuEVOxQMI1GLdyJ8cUntGzBB', 
               'X-Tenant': tenant}
    
    cursor = collection.find(my_query, projection={'_id': False, 'kw_weights': True})
    year = json.loads(dumps(collection.find(my_query)[0]))['year']
    month = json.loads(dumps(collection.find(my_query)[0]))['month']
    week = json.loads(dumps(collection.find(my_query)[0]))['week_of_year']
    
    data = json.dumps({"ainfo_id": analysis[0], "data": json.dumps(json.loads(dumps(cursor))[0]), 
                        "year": year, "month": month, "week_of_year": week, "extracted_at": today})
    
    response = requests.post(kw_daily_url, headers=headers, data=data)

    print(response.status_code)
    #print(response.json())


sent_url = "https://esg-maturity.com/api/v1/reputational/sentiments"

def post_sent(tenant, collection):

    headers = {'Accept': 'application/json', 
               'Content-Type': 'application/json', 
               'Authorization': 'Bearer 2|IIvhcPW0VLmm11NXAuEVOxQMI1GLdyJ8cUntGzBB', 
               'X-Tenant': tenant}
    
    cursor = collection.find(my_query, projection={'_id': False})
    data = json.dumps({"ainfo_id": analysis[0], "data": dumps(cursor), "extracted_at": today})
    # should I define extracted_at or is it automatically set?
    
    response = requests.post(sent_url, headers=headers, data=data)
    
    print(response.status_code)
    #print(response.json())


sent_daily_url = "https://esg-maturity.com/api/v1/reputational/sentiments-daily"

def post_sent_daily(tenant, collection):

    headers = {'Accept': 'application/json', 
               'Content-Type': 'application/json', 
               'Authorization': 'Bearer 2|IIvhcPW0VLmm11NXAuEVOxQMI1GLdyJ8cUntGzBB', 
               'X-Tenant': tenant}
    
    cursor = collection.find(my_query, projection={'_id': False, 
                                                        'year': False, 
                                                        'month': False, 
                                                        'week_of_year': False})
    year = json.loads(dumps(collection.find(my_query)[0]))['year']
    month = json.loads(dumps(collection.find(my_query)[0]))['month']
    week = json.loads(dumps(collection.find(my_query)[0]))['week_of_year']
    
    data = json.dumps({"ainfo_id": analysis[0], "data": json.dumps(json.loads(dumps(cursor))[0]), 
                        "year": year, "month": month, "week_of_year": week, "extracted_at": today})
    
    response = requests.post(sent_daily_url, headers=headers, data=data)

    print(response.status_code)
    #print(response.json())


emo_url = "https://esg-maturity.com/api/v1/reputational/emotions"

def post_emo(tenant, collection):

    headers = {'Accept': 'application/json', 
               'Content-Type': 'application/json', 
               'Authorization': 'Bearer 2|IIvhcPW0VLmm11NXAuEVOxQMI1GLdyJ8cUntGzBB', 
               'X-Tenant': tenant}
    
    cursor = collection.find(my_query, projection={'_id': False})
    data = json.dumps({"ainfo_id": analysis[0], "data": dumps(cursor), "extracted_at": today})
    # should I define extracted_at or is it automatically set?
    
    response = requests.post(emo_url, headers=headers, data=data)
    
    print(response.status_code)
    #print(response.json())


emo_daily_url = "https://esg-maturity.com/api/v1/reputational/emotions-daily"

def post_emo_daily(tenant, collection):

    headers = {'Accept': 'application/json', 
               'Content-Type': 'application/json',
               'Authorization': 'Bearer 2|IIvhcPW0VLmm11NXAuEVOxQMI1GLdyJ8cUntGzBB', 
               'X-Tenant': tenant}
        
    cursor = collection.find(my_query, projection={'_id': False, 
                                                        'year': False, 
                                                        'month': False, 
                                                        'week_of_year': False})
    year = json.loads(dumps(collection.find(my_query)[0]))['year']
    month = json.loads(dumps(collection.find(my_query)[0]))['month']
    week = json.loads(dumps(collection.find(my_query)[0]))['week_of_year']
    
    data = json.dumps({"ainfo_id": analysis[0], "data": json.dumps(json.loads(dumps(cursor))[0]), 
                        "year": year, "month": month, "week_of_year": week, "extracted_at": today})
    
    response = requests.post(emo_daily_url, headers=headers, data=data)

    print(response.status_code)
    #print(response.json())


analysis_per_tenant = get_analysis()
### START TEST ###
del analysis_per_tenant['3201246a-67d0-4062-a387-39bc4558b3e1'][0:6] # to use only the MY_TEST_3 analysis
language = 'en' # this will be defined by the client and will come from somewhere else
### END TEST ###

for tenant in analysis_per_tenant.keys():
    
    for analysis in analysis_per_tenant[tenant]:

        analysis_name = analysis[1]
        db = mongo_client[analysis_name]

        ### GET DATA TO ANALYSE ###
        df_data = load_data(tenant)

        df = load_tweets(df_data, language)
        """
        ### EXTRACT KEYWORDS ###
        print("EXTRACTING KEYWORDS...")
        kw = clean_kw(df, language)
        get_keywords(db, kw)

        kw_freq_weight = db['kw_freq_weight'] # collection kw_freq_weight
        post_kw(tenant, kw_freq_weight)

        ### KEYWORDS DAILY ###
        agg_kw_daily(db, today)

        kw_daily = db['kw_daily'] # collection kw_daily
        post_kw_daily(tenant, kw_daily)
        print("KEYWORD EXTRACTION COMPLETED!")
        
        ### SENTIMENT ANALYSIS ###
        print("RUNNING SENTIMENT ANALYSIS...")
        ml_sent_results = ml_sent(df, language)
        get_sentiment(db, ml_sent_results)

        sentiment = db['sentiment'] # collection sentiment
        post_sent(tenant, sentiment)

        ### SENTIMENT DAILY ###
        agg_sentiment_daily(db, today)

        sentiment_daily = db['sentiment_daily'] # collection sentiment_daily
        post_sent_daily(tenant, sentiment_daily)
        print("SENTIMENT ANALYSIS COMPLETED!")
        """
        ### EMOTION ANALYSIS ###
        print("RUNNING EMOTION ANALYSIS...")
        ml_emotions_results = ml_emotions(df, language)
        get_emotions(db, ml_emotions_results)

        emotions = db['emotions'] # collection emotions
        post_emo(tenant, emotions)

        ### EMOTIONS DAILY ###
        agg_emotions_daily(db, today)

        emotions_daily = db['emotions_daily'] # collection emotions_daily
        post_emo_daily(tenant, emotions_daily)
        print("EMOTION ANALYSIS COMPLETED!")


if __name__ == '__main__':

    #print(df)
    #print(kw)

    #print(db.list_collection_names())

    print("Success!")