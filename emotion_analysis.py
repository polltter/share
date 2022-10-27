import pymongo

import pandas as pd

#import textacy
import textacy.resources


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
    #emotions = db['sentiment'] # collection emotion
    emotions = db['emotions_test'] # collection emotions_test

    emotions.insert_many(df_dict)


if __name__ == '__main__':

    # save emotions analysis results to a MongoDB database
    get_emotions(dmood_emotions(load_tweets_mongo("2022-10-19")))

    print('Success!')