from transformers import pipeline

import pandas as pd

from datetime import datetime


def ml_sent(df, lang, source):

    if lang == 'pt':
        model="cardiffnlp/twitter-xlm-roberta-base-sentiment"
    else:
        model="cardiffnlp/twitter-roberta-base-sentiment-latest"
    
    df_ml = df.copy()

    classifier = pipeline("sentiment-analysis", model)

    if source == 'Twitter':
        sentiment_analysis = classifier(df_ml['text'].tolist(), top_k=3) # top_k=3 to get the scores for all the possible labels

    if source == 'news':
        sentiment_analysis = classifier(df_ml['title'].tolist(), top_k=3) # top_k=3 to get the scores for all the possible labels

    df_ml['sentiment'] = pd.Series(sentiment_analysis)
    df_ml['top_sentiment'] = df_ml['sentiment'].map(lambda sentiment: max(sentiment, key=lambda x: x['score'])) # top sentiment
    df_ml['score'] = df_ml['top_sentiment'].map(lambda x: x['score']) # top score
    df_ml['label'] = df_ml['top_sentiment'].map(lambda x: x['label']) # top label
    df_ml['label'] = df_ml['label'].map(lambda x: 'pos' if x == 'positive' else ('neg' if x == 'negative' else 'neu'))

    return df_ml


def get_sentiment(db, df):

    # convert dataframe into a list of dictionaries
    df_dict = df.to_dict(orient='records') # we get a list where each element corresponds to a row of the dataframe

    sentiment = db['sentiment'] # collection sentiment

    sentiment.insert_many(df_dict)


def agg_sentiment_daily(db, date):
    
    sentiment = db['sentiment'] # collection sentiment
    sentiment_daily = db['sentiment_daily'] # collection sentiment_daily

    # number of positive tweets
    my_query = {"$and": [{"extracted_at": {"$eq": date}}, {"label": "pos"}]}
    n_pos = sentiment.count_documents(my_query)

    # number of negative tweets
    my_query = {"$and": [{"extracted_at": {"$eq": date}}, {"label": "neg"}]}
    n_neg = sentiment.count_documents(my_query)

    # number of neutral tweets
    my_query = {"$and": [{"extracted_at": {"$eq": date}}, {"label": "neu"}]}
    n_neu = sentiment.count_documents(my_query)

    dt = datetime.strptime(date, "%Y-%m-%d")
    # date_ymw is a tuple with the form (date, year, month, week)
    date_ymw = (date, dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%U"))

    doc = {"extracted_at": date_ymw[0], "year": date_ymw[1], "month": date_ymw[2], "week_of_year": date_ymw[3], 
           "positive_count": n_pos, "negative_count": n_neg, "neutral_count": n_neu}
    
    sentiment_daily.insert_one(doc)