import pymongo

import json
import pandas as pd

from nltk.sentiment.vader import SentimentIntensityAnalyzer

import textacy
from textblob import TextBlob

from transformers import pipeline


def load_tweets(language='en'):
    """Returns data frame with tweets and additional information like tweet id, language, date of creation, 
    number of retweets, number of replies, number of likes and number of quotes.

    :param language: Language, defaults to 'en' (english)
    :type language: str, optional
    :return: A data frame with tweets and related info
    :rtype: DataFrame
    """
    list_json = []

    with open('data/tweets.txt') as file:
        for line in file:
            data = json.loads(line)
            list_json.append(data)

    df = pd.DataFrame(list_json, columns = ['id', 'text', 'lang', 'created_at', 'public_metrics'])

    df['retweets'] = df['public_metrics'].map(lambda x: x['retweet_count'])
    df['replies'] = df['public_metrics'].map(lambda x: x['reply_count'])
    df['likes'] = df['public_metrics'].map(lambda x: x['like_count'])
    df['quotes'] = df['public_metrics'].map(lambda x: x['quote_count'])

    df.drop('public_metrics', axis=1, inplace=True)

    df_final = df[df['lang'] == language].copy()

    return df_final

#print(load_tweets().head())
#print(load_tweets()['lang'].value_counts())


mongo_client=pymongo.MongoClient('mongodb://localhost:27017/')
#mongo_client=pymongo.MongoClient('mongodb://root:root@mongo:27017/')

def load_tweets_mongo(language='en'):
    """Returns data frame with tweets and additional information like tweet id, language, date of creation, 
    number of retweets, number of replies, number of likes and number of quotes.

    :param language: Language, defaults to 'en' (english)
    :type language: str, optional
    :return: A data frame with tweets and related info
    :rtype: DataFrame
    """
    db = mongo_client['rep_analysis_new'] # database rep_analysis_new
    #db = mongo_client['central'] # database central
    data_twitter = db['data_twitter'] # collection data_twitter

    #my_query = {"created_at": {"$gt": "2022-09-26T23:29:00.000Z"}}
    #cursor = data_twitter.find(my_query)
    cursor = data_twitter.find() # we will use all the tweets for now
    df = pd.DataFrame(list(cursor))

    df['retweets'] = df['public_metrics'].map(lambda x: x['retweet_count'])
    df['replies'] = df['public_metrics'].map(lambda x: x['reply_count'])
    df['likes'] = df['public_metrics'].map(lambda x: x['like_count'])
    df['quotes'] = df['public_metrics'].map(lambda x: x['quote_count'])

    df.drop('public_metrics', axis=1, inplace=True)

    df_final = df[df['lang'] == language].copy()

    return df_final

#print(load_tweets_mongo().head())
#print(load_tweets_mongo()['lang'].value_counts())

  
def vader_sent(df, threshold_pos=0.05, threshold_neg=-0.05):
    """Returns data frame with scores, compound score
    and sentiment label ('pos', 'neu' or 'neg') for each text.

    :param df: Data frame to pass as input
    :type df: DataFrame
    :param threshold_pos: Value above which the sentiment is considered to be positive, defaults to 0.05
    :type threshold_pos: float, optional
    :param threshold_neg: Value below which the sentiment is considered to be negative, defaults to -0.05
    :type threshold_neg: float, optional
    :return: A data frame with sentiment analysis results
    :rtype: DataFrame
    """
    sid = SentimentIntensityAnalyzer()

    df_vader = df.copy()
    df_vader['scores'] = df_vader['text'].map(lambda tweet: sid.polarity_scores(tweet))
    df_vader['compound']  = df_vader['scores'].map(lambda score_dict: score_dict['compound'])
    df_vader['label'] = df_vader['compound'].map(lambda comp: 'pos' if comp >= threshold_pos else ('neg' if comp <= threshold_neg else 'neu'))

    return(df_vader)

#print(vader_sent(load_tweets()))
#print(vader_sent(load_tweets())['label'].value_counts())


def tblob_sent(df, threshold_pos=0.05, threshold_neg=-0.05):
    """Returns data frame with polarity, subjectivity
    and sentiment label ('pos', 'neu' or 'neg') for each text.

    :param df: Data frame to pass as input
    :type df: DataFrame
    :param threshold_pos: Value above which the sentiment is considered to be positive, defaults to 0.05
    :type threshold_pos: float, optional
    :param threshold_neg: Value below which the sentiment is considered to be negative, defaults to -0.05
    :type threshold_neg: float, optional
    :return: A data frame with sentiment analysis results
    :rtype: DataFrame
    """
    df_tblob = df.copy()
    corpus = textacy.Corpus("en_core_web_sm", df_tblob['text'])

    pol = []
    subj = []

    for doc in corpus:
        tblob = TextBlob(doc.text)
    
        pol.append({'polarity': tblob.sentiment.polarity})
        subj.append({'subjectivity': tblob.sentiment.subjectivity})

    df_tblob['polarity'] = pd.DataFrame(pol)
    df_tblob['subjectivity'] = pd.DataFrame(subj)

    df_tblob['label'] = df_tblob['polarity'].map(lambda pol: 'pos' if pol >= threshold_pos else ('neg' if pol <= threshold_neg else 'neu'))

    return(df_tblob)

#print(tblob_sent(load_tweets()))
#print(tblob_sent(load_tweets())['label'].value_counts())


def ml_sent(df, model="cardiffnlp/twitter-roberta-base-sentiment-latest"):
    """Returns data frame with sentiment, top_sentiment, score
    and sentiment label ('pos', 'neu' or 'neg') for each text.

    :param df: Data frame to pass as input
    :type df: DataFrame
    :param model: model for sentiment analysis, defaults to "cardiffnlp/twitter-roberta-base-sentiment-latest"
    :type model: str, optional
    :return: A data frame with sentiment analysis results
    :rtype: DataFrame
    """
    df_ml = df.copy()

    classifier = pipeline("sentiment-analysis", model)
    sentiment_analysis = classifier(df_ml['text'].tolist(), top_k=3) # top_k=3 to get the scores for all the possible labels

    df_ml['sentiment'] = pd.Series(sentiment_analysis)
    df_ml['top_sentiment'] = df_ml['sentiment'].map(lambda sentiment: max(sentiment, key=lambda x: x['score'])) # top sentiment
    df_ml['score'] = df_ml['top_sentiment'].map(lambda x: x['score']) # top score
    df_ml['label'] = df_ml['top_sentiment'].map(lambda x: x['label']) # top label
    df_ml['label'] = df_ml['label'].map(lambda x: 'pos' if x == 'Positive' else ('neg' if x == 'Negative' else 'neu'))

    return df_ml

#print(ml_sent(load_tweets()))
#print(ml_sent(load_tweets())['label'].value_counts())


def get_sentiment(df):

    # convert dataframe into a list of dictionaries
    df_dict = df.to_dict(orient='records') # we get a list where each element corresponds to a row of the dataframe

    db = mongo_client['rep_analysis_new'] # database rep_analysis_new
    #db = mongo_client['central'] # database central
    sentiment = db['sentiment'] # collection sentiment

    sentiment.insert_many(df_dict)


if __name__ == '__main__':

    # sentiment analysis with data from MongoDB
    #print(vader_sent(load_tweets_mongo())['label'].value_counts()) # vader
    #print(tblob_sent(load_tweets_mongo())['label'].value_counts()) # textblob
    #print(ml_sent(load_tweets_mongo())['label'].value_counts()) # ml model

    # save sentiment analysis (VADER) results to a MongoDB database
    get_sentiment(vader_sent(load_tweets_mongo()))

    print('Success!')