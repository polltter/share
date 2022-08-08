import json
import pandas as pd

from nltk.sentiment.vader import SentimentIntensityAnalyzer

import textacy
from textblob import TextBlob


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

    df_en = df[df['lang'] == language].copy()

    return df_en

#print(load_tweets().head())
#print(load_tweets()['lang'].value_counts())


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
    df_vader['comp_label'] = df_vader['compound'].map(lambda comp: 'pos' if comp >= threshold_pos else ('neg' if comp <= threshold_neg else 'neu'))

    return(df_vader)

#print(vader_sent(load_tweets()))


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