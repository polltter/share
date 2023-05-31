from transformers import pipeline

import pandas as pd

from datetime import datetime


def tweets_translation(df, model="Helsinki-NLP/opus-mt-mul-en"):
    
    df_tr = df.copy()
    
    tweets_translation = []
    classifier = pipeline("translation", model)

    for tweet in df_tr['text']:
        translation_result = classifier(tweet)
        tweets_translation.append({'translation': translation_result[0]['translation_text']})
        
    df_tr['translation'] = pd.DataFrame(tweets_translation)
    
    return df_tr


def ml_emotions(df, lang, model="j-hartmann/emotion-english-distilroberta-base"):

    classifier = pipeline("text-classification", model)

    if lang =='pt':
        df_ml = tweets_translation(df)
        emotion_analysis = classifier(df_ml['translation'].tolist(), top_k=3) # top_k=3 to get the scores for the top-3 emotions
    else:
        df_ml = df.copy()
        emotion_analysis = classifier(df_ml['text'].tolist(), top_k=3) # top_k=3 to get the scores for the top-3 emotions

    df_ml['emotion'] = pd.Series(emotion_analysis)
    df_ml['top1_emotion'] = df_ml['emotion'].map(lambda x: x[0]['label'])
    df_ml['top2_emotion'] = df_ml['emotion'].map(lambda x: x[1]['label'])
    df_ml['top3_emotion'] = df_ml['emotion'].map(lambda x: x[2]['label'])
    
    return df_ml


def get_emotions(db, df):
    
    # convert dataframe into a list of dictionaries
    df_dict = df.to_dict(orient='records') # we get a list where each element corresponds to a row of the dataframe

    emotions = db['emotions'] # collection emotions

    emotions.insert_many(df_dict)


def agg_emotions_daily(db, date):
    
    emotions = db['emotions'] # collection emotions
    emotions_daily = db['emotions_daily'] # collection emotions_daily

    # total number of tweets with emotions extracted for a specific date
    n_total = emotions.count_documents({"extracted_at": {"$eq": date}})

    # number of tweets where emotion anger is the top1, top2 or top3 emotion
    my_query = {"$and": [{"extracted_at": {"$eq": date}}, {"$or": [{"top1_emotion": "anger"}, {"top2_emotion": "anger"}, {"top3_emotion": "anger"}]}]}
    n_anger = emotions.count_documents(my_query)
    percent_anger = round(n_anger/n_total, 2)

    # number of tweets where emotion disgust is the top1, top2 or top3 emotion
    my_query = {"$and": [{"extracted_at": {"$eq": date}}, {"$or": [{"top1_emotion": "disgust"}, {"top2_emotion": "disgust"}, {"top3_emotion": "disgust"}]}]}
    n_disgust = emotions.count_documents(my_query)
    percent_disgust = round(n_disgust/n_total, 2)

    # number of tweets where emotion fear is the top1, top2 or top3 emotion
    my_query = {"$and": [{"extracted_at": {"$eq": date}}, {"$or": [{"top1_emotion": "fear"}, {"top2_emotion": "fear"}, {"top3_emotion": "fear"}]}]}
    n_fear = emotions.count_documents(my_query)
    percent_fear = round(n_fear/n_total, 2)

    # number of tweets where emotion joy is the top1, top2 or top3 emotion
    my_query = {"$and": [{"extracted_at": {"$eq": date}}, {"$or": [{"top1_emotion": "joy"}, {"top2_emotion": "joy"}, {"top3_emotion": "joy"}]}]}
    n_joy = emotions.count_documents(my_query)
    percent_joy = round(n_joy/n_total, 2)

    # number of tweets where emotion neutral is the top1, top2 or top3 emotion
    my_query = {"$and": [{"extracted_at": {"$eq": date}}, {"$or": [{"top1_emotion": "neutral"}, {"top2_emotion": "neutral"}, {"top3_emotion": "neutral"}]}]}
    n_neutral = emotions.count_documents(my_query)
    percent_neutral = round(n_neutral/n_total, 2)

    # number of tweets where emotion sadness is the top1, top2 or top3 emotion
    my_query = {"$and": [{"extracted_at": {"$eq": date}}, {"$or": [{"top1_emotion": "sadness"}, {"top2_emotion": "sadness"}, {"top3_emotion": "sadness"}]}]}
    n_sadness = emotions.count_documents(my_query)
    percent_sadness = round(n_sadness/n_total, 2)

    # number of tweets where emotion surprise is the top1, top2 or top3 emotion
    my_query = {"$and": [{"extracted_at": {"$eq": date}}, {"$or": [{"top1_emotion": "surprise"}, {"top2_emotion": "surprise"}, {"top3_emotion": "surprise"}]}]}
    n_surprise = emotions.count_documents(my_query)
    percent_surprise = round(n_surprise/n_total, 2)

    dt = datetime.strptime(date, "%Y-%m-%d")
    # date_ymw is a tuple with the form (date, year, month, week)
    date_ymw = (date, dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%U"))

    doc = {"extracted_at": date_ymw[0], "year": date_ymw[1], "month": date_ymw[2], "week_of_year": date_ymw[3], "n_tweets": n_total,
           "anger_count": n_anger, "anger_percent": percent_anger, 
           "disgust_count": n_disgust, "disgust_percent": percent_disgust,
           "fear_count": n_fear, "fear_percent": percent_fear,
           "joy_count": n_joy, "joy_percent": percent_joy,
           "neutral_count": n_neutral, "neutral_percent": percent_neutral,
           "sadness_count": n_sadness, "sadness_percent": percent_sadness,
           "surprise_count": n_surprise, "surprise_percent": percent_surprise}
    
    emotions_daily.insert_one(doc)