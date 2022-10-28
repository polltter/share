import pymongo

import pandas as pd

#import textacy
import textacy.resources

from datetime import datetime


mongo_client=pymongo.MongoClient('mongodb://localhost:27017/')
#mongo_client=pymongo.MongoClient('mongodb://root:root@mongo:27017/')

def load_tweets_mongo(date, language='en'):
    """Returns data frame with tweets and additional information like tweet id, language, date of creation, 
    number of retweets, number of replies, number of likes, number of quotes and search word.

    :param date: Date ("yyyy-mm-dd") when the tweets were extracted
    :type date: str
    :param language: Language, defaults to 'en' (english)
    :type language: str, optional
    :return: A data frame with tweets and related info
    :rtype: DataFrame
    """
    db = mongo_client['rep_analysis_main'] # database rep_analysis_main
    #db = mongo_client['central'] # database central
    #data_twitter = db['data_twitter'] # collection data_twitter
    data_twitter = db['data_test'] # collection data_test

    my_query = {"extracted_at": {"$eq": date}} # this date will be the day before the one we are currently in because we are doing this everyday
    cursor = data_twitter.find(my_query)
    df = pd.DataFrame(list(cursor))

    df['retweets'] = df['public_metrics'].map(lambda x: x['retweet_count'])
    df['replies'] = df['public_metrics'].map(lambda x: x['reply_count'])
    df['likes'] = df['public_metrics'].map(lambda x: x['like_count'])
    df['quotes'] = df['public_metrics'].map(lambda x: x['quote_count'])

    df.drop('public_metrics', axis=1, inplace=True)

    df_final = df[df['lang'] == language].copy()

    return df_final


def dmood_emotions(df):
    
    df_dmood = df.copy()

    corpus = textacy.Corpus("en_core_web_sm", df_dmood['text'])

    rs = textacy.resources.DepecheMood(lang="en", word_rep="lemma", min_freq=2)

    moods = []

    for doc in corpus:
        mood = sorted(rs.get_emotional_valence(doc).items(), key=lambda x: x[1], reverse=True)
        moods.append({'mood': mood})

    df_dmood['mood'] = pd.DataFrame(moods)
    # drop rows where it was not possible to extract the moods
    #print(df_dmood[df_dmood['mood'].str.len() == 0])
    #print(len(df_dmood[df_dmood['mood'].str.len() == 0]))
    df_dmood.drop(df_dmood[df_dmood['mood'].str.len() == 0].index, inplace=True)

    df_dmood['top1_mood'] = df_dmood['mood'].map(lambda x: x[0][0])
    df_dmood['top2_mood'] = df_dmood['mood'].map(lambda x: x[1][0])
    df_dmood['top3_mood'] = df_dmood['mood'].map(lambda x: x[2][0])
    
    return(df_dmood)

#print(dmood_emotions(load_tweets_mongo("2022-10-02")).head())


def get_emotions(df):

    # convert dataframe into a list of dictionaries
    df_dict = df.to_dict(orient='records') # we get a list where each element corresponds to a row of the dataframe

    db = mongo_client['rep_analysis_main'] # database rep_analysis_main
    #db = mongo_client['central'] # database central
    #emotions = db['emotions'] # collection emotions
    emotions = db['emotions_test'] # collection emotions_test

    emotions.insert_many(df_dict)


def agg_emotions_daily(date):

    db = mongo_client['rep_analysis_main'] # database rep_analysis_main
    emotions = db['emotions_test'] # collection emotions_test
    emotions_daily_main = db['emotions_daily_main'] # collection emotions_daily_main

    # total number of tweets with emotions extracted for a specific date
    n_total = emotions.count_documents({"extracted_at": {"$eq": date}})

    # number of tweets where emotion AMUSED is the top1, top2 or top3 emotion
    my_query = {"$and": [{"extracted_at": {"$eq": date}}, {"$or": [{"top1_mood": "AMUSED"}, {"top2_mood": "AMUSED"}, {"top3_mood": "AMUSED"}]}]}
    n_amused = emotions.count_documents(my_query)
    percent_amused = round(n_amused/n_total, 2)

    # number of tweets where emotion AFRAID is the top1, top2 or top3 emotion
    my_query = {"$and": [{"extracted_at": {"$eq": date}}, {"$or": [{"top1_mood": "AFRAID"}, {"top2_mood": "AFRAID"}, {"top3_mood": "AFRAID"}]}]}
    n_afraid = emotions.count_documents(my_query)
    percent_afraid = round(n_afraid/n_total, 2)

    # number of tweets where emotion ANGRY is the top1, top2 or top3 emotion
    my_query = {"$and": [{"extracted_at": {"$eq": date}}, {"$or": [{"top1_mood": "ANGRY"}, {"top2_mood": "ANGRY"}, {"top3_mood": "ANGRY"}]}]}
    n_angry = emotions.count_documents(my_query)
    percent_angry = round(n_angry/n_total, 2)

    # number of tweets where emotion ANNOYED is the top1, top2 or top3 emotion
    my_query = {"$and": [{"extracted_at": {"$eq": date}}, {"$or": [{"top1_mood": "ANNOYED"}, {"top2_mood": "ANNOYED"}, {"top3_mood": "ANNOYED"}]}]}
    n_annoyed = emotions.count_documents(my_query)
    percent_annoyed = round(n_annoyed/n_total, 2)

    # number of tweets where emotion DONT_CARE is the top1, top2 or top3 emotion
    my_query = {"$and": [{"extracted_at": {"$eq": date}}, {"$or": [{"top1_mood": "DONT_CARE"}, {"top2_mood": "DONT_CARE"}, {"top3_mood": "DONT_CARE"}]}]}
    n_dontcare = emotions.count_documents(my_query)
    percent_dontcare = round(n_dontcare/n_total, 2)

    # number of tweets where emotion HAPPY is the top1, top2 or top3 emotion
    my_query = {"$and": [{"extracted_at": {"$eq": date}}, {"$or": [{"top1_mood": "HAPPY"}, {"top2_mood": "HAPPY"}, {"top3_mood": "HAPPY"}]}]}
    n_happy = emotions.count_documents(my_query)
    percent_happy = round(n_happy/n_total, 2)

    # number of tweets where emotion INSPIRED is the top1, top2 or top3 emotion
    my_query = {"$and": [{"extracted_at": {"$eq": date}}, {"$or": [{"top1_mood": "INSPIRED"}, {"top2_mood": "INSPIRED"}, {"top3_mood": "INSPIRED"}]}]}
    n_inspired = emotions.count_documents(my_query)
    percent_inspired = round(n_inspired/n_total, 2)

    # number of tweets where emotion SAD is the top1, top2 or top3 emotion
    my_query = {"$and": [{"extracted_at": {"$eq": date}}, {"$or": [{"top1_mood": "SAD"}, {"top2_mood": "SAD"}, {"top3_mood": "SAD"}]}]}
    n_sad = emotions.count_documents(my_query)
    percent_sad = round(n_sad/n_total, 2)

    dt = datetime.strptime(date, "%Y-%m-%d")
    # date_ymw is a tuple with the form (date, year, month, week)
    date_ymw = (date, dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%U"))

    doc = {"extracted_at": date_ymw[0], "year": date_ymw[1], "month": date_ymw[2], "week_of_year": date_ymw[3],
           "amused_count": n_amused, "amused_percent": percent_amused, 
           "afraid_count": n_afraid, "afraid_percent": percent_afraid,
           "angry_count": n_angry, "angry_percent": percent_angry,
           "annoyed_count": n_annoyed, "annoyed_percent": percent_annoyed,
           "dontcare_count": n_dontcare, "dontcare_percent": percent_dontcare,
           "happy_count": n_happy, "happy_percent": percent_happy,
           "inspired_count": n_inspired, "inspired_percent": percent_inspired,
           "sad_count": n_sad, "sad_percent": percent_sad}
    
    emotions_daily_main.insert_one(doc)


def agg_emotions_weekly(week):

    db = mongo_client['rep_analysis_main'] # database rep_analysis_main
    emotions_daily_main = db['emotions_daily_main'] # collection emotions_daily_main
    emotions_weekly_main = db['emotions_weekly_main'] # collection emotions_weekly_main

    cursor = emotions_daily_main.aggregate([
        {"$match": {"week_of_year": week}}, 
        {"$group": {"_id": {"year_week": ["$year", "$week_of_year"]}, 
                    "total_amused_count": {"$sum": "$amused_count"}, 
                    "total_afraid_count": {"$sum": "$afraid_count"}, 
                    "total_angry_count": {"$sum": "$angry_count"},
                    "total_annoyed_count": {"$sum": "$annoyed_count"},
                    "total_dontcare_count": {"$sum": "$dontcare_count"},
                    "total_happy_count": {"$sum": "$happy_count"},
                    "total_inspired_count": {"$sum": "$inspired_count"},
                    "total_sad_count": {"$sum": "$sad_count"},
                    "total_number_of_days": {"$sum": 1}}},
        {"$addFields": {"amused_percent": {"$round": [{"$divide": ["$total_amused_count", "$total_number_of_days"]}, 2]}, 
                        "afraid_percent": {"$round": [{"$divide": ["$total_afraid_count", "$total_number_of_days"]}, 2]}, 
                        "angry_percent": {"$round": [{"$divide": ["$total_angry_count", "$total_number_of_days"]}, 2]},
                        "annoyed_percent": {"$round": [{"$divide": ["$total_annoyed_count", "$total_number_of_days"]}, 2]},
                        "dontcare_percent": {"$round": [{"$divide": ["$total_dontcare_count", "$total_number_of_days"]}, 2]},
                        "happy_percent": {"$round": [{"$divide": ["$total_happy_count", "$total_number_of_days"]}, 2]},
                        "inspired_percent": {"$round": [{"$divide": ["$total_inspired_count", "$total_number_of_days"]}, 2]},
                        "sad_percent": {"$round": [{"$divide": ["$total_sad_count", "$total_number_of_days"]}, 2]}}
        }
    ])

    for x in cursor:
        emotions_weekly_main.insert_one(x)


def agg_emotions_monthly(month, year):

    db = mongo_client['rep_analysis_main'] # database rep_analysis_main
    emotions_daily_main = db['emotions_daily_main'] # collection emotions_daily_main
    emotions_monthly_main = db['emotions_monthly_main'] # collection emotions_monthly_main

    cursor = emotions_daily_main.aggregate([
        {"$match": {"month": month}}, 
        {"$group": {"_id": {"year_month": ["$year", "$month"]}, 
                    "total_amused_count": {"$sum": "$amused_count"}, 
                    "total_afraid_count": {"$sum": "$afraid_count"}, 
                    "total_angry_count": {"$sum": "$angry_count"},
                    "total_annoyed_count": {"$sum": "$annoyed_count"},
                    "total_dontcare_count": {"$sum": "$dontcare_count"},
                    "total_happy_count": {"$sum": "$happy_count"},
                    "total_inspired_count": {"$sum": "$inspired_count"},
                    "total_sad_count": {"$sum": "$sad_count"},
                    "total_number_of_days": {"$sum": 1}}},
        {"$addFields": {"amused_percent": {"$round": [{"$divide": ["$total_amused_count", "$total_number_of_days"]}, 2]}, 
                        "afraid_percent": {"$round": [{"$divide": ["$total_afraid_count", "$total_number_of_days"]}, 2]}, 
                        "angry_percent": {"$round": [{"$divide": ["$total_angry_count", "$total_number_of_days"]}, 2]},
                        "annoyed_percent": {"$round": [{"$divide": ["$total_annoyed_count", "$total_number_of_days"]}, 2]},
                        "dontcare_percent": {"$round": [{"$divide": ["$total_dontcare_count", "$total_number_of_days"]}, 2]},
                        "happy_percent": {"$round": [{"$divide": ["$total_happy_count", "$total_number_of_days"]}, 2]},
                        "inspired_percent": {"$round": [{"$divide": ["$total_inspired_count", "$total_number_of_days"]}, 2]},
                        "sad_percent": {"$round": [{"$divide": ["$total_sad_count", "$total_number_of_days"]}, 2]}, 
                        "year": year} # field added to make the yearly aggregation easier
        }
    ])

    for x in cursor:
        emotions_monthly_main.insert_one(x)


if __name__ == '__main__':

    # save emotions analysis results to a MongoDB database
    #get_emotions(dmood_emotions(load_tweets_mongo("2022-10-19")))

    # aggregate emotion analysis results (daily)
    #agg_emotions_daily("2022-10-19")

    # aggregate emotion analysis results (weekly)
    #agg_emotions_weekly("42")

    # aggregate emotion analysis results (monthly)
    agg_emotions_monthly("10", "2022")

    print('Success!')