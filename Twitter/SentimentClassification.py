import nltk
import GetTweets as gt
from textblob import TextBlob
import pandas as pd
import numpy as np
from textblob import translate
import textblob
import datetime
from itertools import repeat
import time

######################### GET POLARITY INGLESA  ##############################

def get_polarity_tweet(tweet, lang):
    
    analysis = TextBlob(tweet)

    if lang == 'en':
        analysis_ready = analysis
    else:
        analysis_ready = analysis.translate(from_lang = lang, to='en')
        
    return analysis_ready.sentiment.polarity

######################### GET SUBJECTIVITY INGLESA ##############################

def get_subjectivity_tweet(tweet, lang):
    
    analysis = TextBlob(tweet)

    if lang == 'en':
        analysis_ready = analysis
    else:
        analysis_ready = analysis.translate(from_lang = lang, to='en')
        
    return analysis_ready.sentiment.subjectivity

####################### SENTIMENT COMBINED  ###############################

def get_tweet_sentiment_combined(company, previous_in_hours = 6, count = 10, language = 'en', exclude_retweets = 'S', time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    time_start = time.time()
    
    ### Get Data, tanto para empresa como para competicao
    
    data_full = gt.get_tweets_last_day_combined(company, previous_in_hours = previous_in_hours, count = count, language = language, exclude_retweets = exclude_retweets, time_block = time_block)
    
    ### Select colunas relevantes

    data_text = data_full[['Time_Block','Bloco','SelfOrCompetitor', 'Empresa', 'Present_Date','Tweet_ID','Tweet_Date','Tweet_Text',
                         'Tweet_Lang', 'Tweet_Likes', 'Tweet_Retweets', 'Tweet_Comments',
                         'User_Verified', 'User_Followers', 'User_Friends']]
    
    ### Obter os tweets em formato de lista
    
    list_tweet_text = data_text['Tweet_Text'].to_list()
    list_tweet_lang = data_full['Tweet_Lang'].to_list()

    ### Adicionar Polaridade + Subjectivity dos tweets à análise
    
    polarity_list = []
    subjectivity_list = []

    for i in range(len(list_tweet_text)):
        
        polarity_list.append(get_polarity_tweet(list_tweet_text[i], list_tweet_lang[i]))
        subjectivity_list.append(get_subjectivity_tweet(list_tweet_text[i], list_tweet_lang[i]))
    
    data_text['Tweet_Polarity'] = polarity_list
    data_text['Tweet_Subjectivity'] =  subjectivity_list
    
    list_std_pol = []
    list_std_sub = []
    
    for i in data_text['Empresa'].unique():
        
        data_section = data_text[data_text['Empresa'] == i]
        
        stdev_pol = data_section['Tweet_Polarity'].std()
        stdev_sub = data_section['Tweet_Subjectivity'].std()
        
        list_std_pol.extend(repeat(stdev_pol,len(data_section)))
        list_std_sub.extend(repeat(stdev_sub,len(data_section)))
        
    
    data_text['stdev_polarity'] = list_std_pol
    data_text['stdev_subjectivity'] =  list_std_sub
    
    time_end= time.time()
    
    print('Tempo de execução: ' , time_end-time_start)
    
    return data_text

#######################SENTIMENT COMPETITOR ###############################

def get_tweet_sentiment_competitor(company, previous_in_hours = 6, count = 10, language = 'en', exclude_retweets = 'S', time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    time_start = time.time()
    
    ### Get tweets
    
    data_full = gt.get_tweets_last_day_competitor(company, previous_in_hours = previous_in_hours, count = count, language = language, exclude_retweets = exclude_retweets, time_block = time_block)
    
    ### Select colunas relevantes

    data_text = data_full[['Time_Block','Bloco','SelfOrCompetitor', 'Empresa', 'Present_Date','Tweet_ID','Tweet_Date', 'Tweet_Text',
                         'Tweet_Lang', 'Tweet_Likes', 'Tweet_Retweets', 'Tweet_Comments',
                         'User_Verified', 'User_Followers', 'User_Friends']]
    
    ### Obter os tweets em formato de lista
    
    list_tweet_text = data_text['Tweet_Text'].to_list()
    list_tweet_lang = data_full['Tweet_Lang'].to_list()

    ### Adicionar Polaridade + Subjectivity dos tweets à análise
    
    polarity_list = []
    subjectivity_list = []

    for i in range(len(list_tweet_text)):
        
        polarity_list.append(get_polarity_tweet(list_tweet_text[i], list_tweet_lang[i]))
        subjectivity_list.append(get_subjectivity_tweet(list_tweet_text[i], list_tweet_lang[i]))
    
    data_text['Tweet_Polarity'] = polarity_list
    data_text['Tweet_Subjectivity'] =  subjectivity_list
    
    list_std_pol = []
    list_std_sub = []
    
    for i in data_text['Empresa'].unique():
        
        data_section = data_text[data_text['Empresa'] == i]
        
        stdev_pol = data_section['Tweet_Polarity'].std()
        stdev_sub = data_section['Tweet_Subjectivity'].std()
        
        list_std_pol.extend(repeat(stdev_pol,len(data_section)))
        list_std_sub.extend(repeat(stdev_sub,len(data_section)))
        
    
    data_text['stdev_polarity'] = list_std_pol
    data_text['stdev_subjectivity'] =  list_std_sub
    
    time_end= time.time()
    
    print('Tempo de execução: ' , time_end-time_start)
    
    return data_text

####################### SENTIMENT EMPRESA ###############################

def get_tweet_sentiment_company(company, previous_in_hours = 6, count = 10, language = 'en', exclude_retweets = 'S', time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    time_start = time.time()
    
    ### Get Tweets
    
    data_full = gt.get_tweets_last_day_company(company, previous_in_hours = previous_in_hours, count = count, language = language, exclude_retweets = exclude_retweets, time_block= time_block)
    
    ### Select colunas relevantes

    data_text = data_full[['Time_Block','Bloco','SelfOrCompetitor', 'Empresa','Present_Date', 'Tweet_ID', 'Tweet_Date', 'Tweet_Text',
                         'Tweet_Lang', 'Tweet_Likes', 'Tweet_Retweets', 'Tweet_Comments',
                         'User_Verified', 'User_Followers', 'User_Friends']]

    ### Obter os tweets + lang em formato de lista

    list_tweet_text = data_text['Tweet_Text'].to_list()
    list_tweet_lang = data_full['Tweet_Lang'].to_list()

    ### Adicionar Polaridade + Subjectivity dos tweets à análise
    
    polarity_list = []
    subjectivity_list = []

    for i in range(len(list_tweet_text)):
        
        polarity_list.append(get_polarity_tweet(list_tweet_text[i], list_tweet_lang[i]))
        subjectivity_list.append(get_subjectivity_tweet(list_tweet_text[i], list_tweet_lang[i]))
    
    data_text['Tweet_Polarity'] = polarity_list
    data_text['Tweet_Subjectivity'] =  subjectivity_list
    data_text['stdev_polarity'] = np.std(polarity_list)
    data_text['stdev_subjectivity'] =  np.std(subjectivity_list)
    
    time_end= time.time()
    
    print('Tempo de execução: ' , time_end-time_start)
    
    return data_text

############################# MAIN #########3###############################

if __name__ == '__main__':
    
    a= get_tweet_sentiment_combined('Netflix')
    #data_full = gt.get_tweets_last_day_company('Netflix')
    #data_sentiment = get_tweet_sentiment_combined_no_retweets("Netflix")
    #data_sentiment = get_tweet_sentiment_combined("Netflix") 