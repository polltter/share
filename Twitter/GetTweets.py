    # -*- coding: utf-8 -*-

import sys
import tweepy
import logging
import json
import time
import pandas as pd
import numpy as np
import datetime
import openpyxl
import re
from collections import Counter
from gensim.parsing.preprocessing import remove_stopwords
import time
import pytz

    


###############################   Authentication #######################################



def tweepy_authenticate_user():
     USER_KEY = ''
     USER_SECRET = ''
     ACCESS_TOKEN = ''
     ACCESS_SECRET = ''
     auth = tweepy.OAuthHandler(USER_KEY, USER_SECRET)
     auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
     api = tweepy.API(auth, wait_on_rate_limit=True)
     if api.verify_credentials() == False:
         print("The user credentials are invalid.")
     else:
        print("The user credentials are valid.")
     return api

##############################  Remove URL's #######################################

def remove_URL(txt):
    
    return " ".join(re.sub("([^0-9A-Za-z \t])|(\w+:\/\/\S+)", "", txt).split())
    
############################## Obter idiomas suportados pelo API ######################

def get_langs_twitter():
  
    dict_lang = {
                    'French': 'fr',
                    'English' : 'en',
                    'Arabic' : 'ar',
                    'Spanish' : 'es',
                    'Japanese' : 'ja',
                    'Italian' : 'it',
                    'German' : 'de',
                    'Indonesian' : 'id',
                    'Portuguese' : 'pt',
                    'Korean' : 'ko',
                    'Turkish': 'tr',
                    'Russian': 'ru',
                    'Dutch' : 'nl',
                    'Filipino': 'fil',
                    'Malay' : 'msa',
                    'Traditional Chinese': 'zh-tw',
                    'Simplified Chinese' : 'zh-cn',
                    'Hindi': 'hi',
                    'Norwegian': 'no',
                    'Swedish': 'sv',
                    'Finnish':'fi',
                    'Danish': 'da',
                    'Polish':'pl',
                    'Hungarian':'hu',
                    'Farsi': 'fa',
                    'Urdu': 'ur',
                    'Hebrew': 'he',
                    'Thai': 'th',
                    'English UK' : 'en-gb'
                 }
    
    return dict_lang 
    
##############################  Obter competiçao #######################################

def get_competitors(company):
    
    List_Stream = ['Netflix', 'Hulu', 'AmazonPrime', 'Showtime', 'HBO', 'Disney+'
                   , 'YoutubeTV', 'Youtube']
    
    List_Phone = ['Apple', 'Samsung', 'Huawei', 'Nokia', 'Sony', 'LG', 'Motorola']
    
    List_Computer = ['Apple', 'HP', 'Dell', 'Lenovo', 'Asus', 'Acer', 'Microsoft', 'Samsung']
    
    List_FastFood = ['McDonald\'s', 'Burger King', 'Wendy\'s', 'KFC', 'Subway', 'Taco Bell']
    
    
    
    List_Competitors = []
    Flat_List_Of_Competitors = []
    
    for i in range(0,len(List_Stream)):
        
        if List_Stream[i] == company:
            List_Stream.remove(List_Stream[i])
            List_Competitors.append(List_Stream)
            break
        else:
            continue
            
    for i in range(0,len(List_Phone)):
        
        if List_Phone[i] == company:
            List_Phone.remove(List_Phone[i])
            List_Competitors.append(List_Phone)
            break
        else:
            continue
            
    for i in range(0,len(List_Computer)):
        
        if List_Computer[i] == company:
            List_Computer.remove(List_Computer[i])
            List_Competitors.append(List_Computer)
            break
        else:
            continue
            
    for i in range(0,len(List_FastFood)):
        
        if List_FastFood[i] == company:
            List_FastFood.remove(List_FastFood[i])
            List_Competitors.append(List_FastFood)
            break
        else:
            continue
     
    ### Transformar o que pode ser uma lista de listas numa só lista
    
    Flat_List_Of_Competitors = [x for l in List_Competitors for x in l]   
    
    ### Remover duplicados
    Distinct_Competitors=[]
    
    for i in Flat_List_Of_Competitors:
        if i in Distinct_Competitors:
            continue
        else:
            Distinct_Competitors.append(i)
        
    
    
    
    return Distinct_Competitors

############################ INCLUIR ERROS ORTOGRAFICOS NA PESQUISA POR KEYWORD - NAO ESTA EM USO  ###############
 
def company_variations(company):
   
    company_variations = [company,company.upper, company.lower()]
    
    ### 1.  Uma letra a menos, exemplo: etflix, Ntflix,.....
    
    
    for i in range(0, len(company)):
        variation = company.replace(company[i],'')
        company_variations.append(variation)
        company_variations.append(variation.upper())
        company_variations.append(variation.lower())
        
        
    ### 2. Letras Trocadas com a seguinte ou anterior, exemplo: Entflix, Netflxi,......
    
    for i in range(0, len(company)):
        if i == len(company)-1 :
            continue
        else:
            switch1 = company[i]+company[i+1]
            switch2 = company[i+1] + company[i]
            
            variation = company.replace(switch1, switch2)
        
        company_variations.append(variation)
        company_variations.append(variation.upper())
        company_variations.append(variation.lower())      
        
    
    return company_variations

####################### GET COMPANY TWEETS RECENTES ##############################
    
def get_tweets_last_day_company(company, previous_in_hours = 6,  count = 10, language = "en", exclude_retweets = 'S', time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    
    time_start = time.time()
    
    ######## Authenticate ################
    
    api = tweepy_authenticate_user()
    
    ######## Incluir erros ortográficos #####
    
    #company_total = company_variations(company)# Comentado pois nao estava a fazer diferença
                                                # Se descomentar trocar "i" por "company" na variaver "filtro"
                                                # Se descomentar dar + 1 indent desde o cursor ate ao continue (inclusive)
    
    ####### Include retweets? Avaliar #######
    
    if exclude_retweets == 'S':
        filtro = 'company -filter:retweets'
        condicao_if = "(not tweet.retweeted) and ('RT @' not in tweet.text)"
    else:
        filtro = 'company'
        condicao_if = '1 == 1'
        
    
    ######## Create DataFrame ###############
    
    tweets_df = pd.DataFrame(data = None, 
                             columns = ['Time_Block','Bloco','SelfOrCompetitor','Empresa','Present_Date','Exclude_Retweets', 'Tweet_Date', 'Tweet_Year', 'Tweet_Month','Tweet_Day', 'Tweet_Hour',
                                        'Tweet_Minute', 'Tweet_Second', 'Tweet_ID', 'Tweet_Text', 'Tweet_Lang','Tweet_Likes', 'Tweet_Retweets', 
                                        'Tweet_Comments',
                                        'User_Screenname' , 'User_Name', 'User_ID', 'User_Verified', 'User_Location', 'User_Followers',
                                        'User_Friends', 'User_Account_Creation'])
    
    
    ####### Cursor : Percorrer Twitter ########
    
    #for i in company_total: # Comentado pois nao estava a fazer diferença
                             # Se descomentar trocar "company" por "i" na variaver "filtro"
                             # Se descomentar dar + 1 indent desde o cursor ate ao continue (inclusive)
    
    tweets = tweepy.Cursor(api.search_tweets, q = filtro  , lang = language,
                           tweet_mode="Extended", result_type = "recent",
                           until = ((datetime.datetime.now()).astimezone(pytz.timezone('UTC')) + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
    
    for tweet in tweets.items(count):
        
        ### Condiçao temporal - timezone do API é UTC
        
        if tweet.created_at >= (datetime.datetime.now()).astimezone(pytz.timezone('UTC')) - datetime.timedelta(hours = previous_in_hours):
        
            ### Excluir retweets
           
            if condicao_if: 
            
            ### Inicializar Lista ###
            
                tweets_list = []
            
            ### Acrescentar dados à lista ###
            
                tweets_list.append(time_block)
                tweets_list.append(company)
                tweets_list.append('Self')
                tweets_list.append(company)
                tweets_list.append((datetime.datetime.now()).strftime("%Y-%m-%d"))
                tweets_list.append(exclude_retweets)
                tweets_list.append(tweet.created_at)
                tweets_list.append(tweet.created_at.year)
                tweets_list.append(tweet.created_at.month)
                tweets_list.append(tweet.created_at.day)
                tweets_list.append(tweet.created_at.hour)
                tweets_list.append(tweet.created_at.minute)
                tweets_list.append(tweet.created_at.second)
                tweets_list.append(tweet.id)
                tweets_list.append(remove_URL(tweet.text))
                tweets_list.append(tweet.lang)
                tweets_list.append(tweet.favorite_count)
                tweets_list.append(tweet.retweet_count)
                tweets_list.append(len(tweet._json['entities']['hashtags']))
                tweets_list.append(tweet.user.screen_name)
                tweets_list.append(tweet.user.name)
                tweets_list.append(tweet.user.id_str)        
                tweets_list.append(tweet.user.verified)
                tweets_list.append(tweet.user.location)
                tweets_list.append(tweet.user.followers_count)
                tweets_list.append(tweet.user.friends_count)
                tweets_list.append(tweet.user.created_at)
            
            ### Dump no DataFrame ###
            
                tweets_df.loc[len(tweets_df)]=tweets_list
            
            else:
                continue
        else:
            continue
    
    time_end= time.time()
    
    print('Tempo de execução: ' , time_end-time_start)
    
    return  tweets_df
 
####################### GET COMPETIÇAO TWEETS RECENTES ##############################   

def get_tweets_last_day_competitor(company, previous_in_hours = 6, count = 10, language = "en", exclude_retweets = 'S', time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    
    time_start = time.time()
    
    ######## Authenticate ################
    
    api = tweepy_authenticate_user()
    
    ####### Obter Competiçao
    
    competition = get_competitors(company)
    
    ####### Include retweets? Avaliar #######
    
    if exclude_retweets == 'S':
        filtro = 'i -filter:retweets'
        condicao_if = "(not tweet.retweeted) and ('RT @' not in tweet.text)"
    else:
        filtro = 'i'
        condicao_if = '1 == 1'
    
    ######## Create DataFrame ###############
    
    tweets_df = pd.DataFrame(data = None, 
                             columns = ['Time_Block','Bloco','SelfOrCompetitor','Empresa', 'Present_Date','Exclude_Retweets', 'Tweet_Date', 'Tweet_Year', 'Tweet_Month','Tweet_Day', 'Tweet_Hour',
                                        'Tweet_Minute', 'Tweet_Second', 'Tweet_ID', 'Tweet_Text', 'Tweet_Lang','Tweet_Likes', 'Tweet_Retweets', 
                                        'Tweet_Comments',
                                        'User_Screenname' , 'User_Name', 'User_ID', 'User_Verified', 'User_Location', 'User_Followers',
                                        'User_Friends', 'User_Account_Creation'])
        
    ####### Iterar por competidor
    
    for i in competition:
    
    ######## Incluir erros ortográficos #####
    
        #company_total = company_variations(i) # Comentado pois nao estava a fazer diferença
                                               # Se descomentar trocar "i" por "j" na variaver "filtro"
                                               # Se descomentar dar + 1 indent desde o cursor ate ao continue (inclusive)
    
    
    ####### Cursor : Percorrer Twitter ########
    
        #for j in company_total: # Comentado pois nao estava a fazer diferença
                                 # Se descomentar trocar "i" por "j" na variaver "filtro"
                                 # Se descomentar dar + 1 indent desde o cursor ate ao continue (inclusive)
    
        tweets = tweepy.Cursor(api.search_tweets, q = filtro, lang = language,
                           tweet_mode="Extended", result_type = "recent",
                           until = ((datetime.datetime.now()).astimezone(pytz.timezone('UTC')) + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
    
        for tweet in tweets.items(count):
            
            ### Condiçao temporal - timezonde do API é UTC
            
            if tweet.created_at  >= (datetime.datetime.now()).astimezone(pytz.timezone('UTC')) - datetime.timedelta(hours = previous_in_hours):
            
                ### Excluir retweets
               
                if condicao_if: 
                
                ### Inicializar Lista ###
                
                    tweets_list = []
                
                ### Acrescentar dados à lista ###
                    
                    tweets_list.append(time_block)
                    tweets_list.append(company)
                    tweets_list.append('Competitor') 
                    tweets_list.append(i)
                    tweets_list.append((datetime.datetime.now()).strftime("%Y-%m-%d"))
                    tweets_list.append(exclude_retweets)
                    tweets_list.append(tweet.created_at)
                    tweets_list.append(tweet.created_at.year)
                    tweets_list.append(tweet.created_at.month)
                    tweets_list.append(tweet.created_at.day)
                    tweets_list.append(tweet.created_at.hour)
                    tweets_list.append(tweet.created_at.minute)
                    tweets_list.append(tweet.created_at.second)
                    tweets_list.append(tweet.id)
                    tweets_list.append(remove_URL(tweet.text))
                    tweets_list.append(tweet.lang)
                    tweets_list.append(tweet.favorite_count)
                    tweets_list.append(tweet.retweet_count)
                    tweets_list.append(len(tweet._json['entities']['hashtags']))
                    tweets_list.append(tweet.user.screen_name)
                    tweets_list.append(tweet.user.name)
                    tweets_list.append(tweet.user.id_str)        
                    tweets_list.append(tweet.user.verified)
                    tweets_list.append(tweet.user.location)
                    tweets_list.append(tweet.user.followers_count)
                    tweets_list.append(tweet.user.friends_count)
                    tweets_list.append(tweet.user.created_at)
                    
                    tweets_df.loc[len(tweets_df)]=tweets_list
                
                else:
                    continue
            else:
                continue
  
    time_end= time.time()
    
    print('Tempo de execução: ' , time_end-time_start)
    
    return  tweets_df

####################### GET TWEETS RECENTES  TUDO JUNTO##############################

def get_tweets_last_day_combined(company, previous_in_hours = 6, count = 10, language = "en", exclude_retweets = 'S', time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    time_start = time.time()
    
    Company_Tweets = get_tweets_last_day_company(company, previous_in_hours = previous_in_hours, count = count, language = language, exclude_retweets = exclude_retweets, time_block = time_block)
    Competitor_Tweets = get_tweets_last_day_competitor(company, previous_in_hours = previous_in_hours, count = round(count / 2), language = language, exclude_retweets = exclude_retweets, time_block = time_block)
    
    Combined_Tweets = Company_Tweets.append(Competitor_Tweets)
    
    time_end= time.time()
    
    print('Tempo de execução: ' , time_end-time_start)
    
    return Combined_Tweets  
          
############################ COUNT WORDS COMPETITOR ##################################################

def count_words_competitor(company, previous_in_hours = 6, count = 10, language = "en", exclude_retweets = "S", top_words = 30, remove_stop_words = 'S', time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    time_start = time.time()
        
    ### 1. Get Tweets
    
    tweets_competitor = get_tweets_last_day_competitor(company = company, previous_in_hours = previous_in_hours, count = count, language = language, exclude_retweets = exclude_retweets, time_block = time_block)
    
    #### 2. Criar dataframe final
    
    count_competitor = pd.DataFrame(data = None, 
                             columns = ['Time_Block','Bloco','SelfOrCompetitor','Empresa','Present_Date','Remove_Stopwords',
                                        'Exclude_Retweets', 'Language' ,'Word', 'Word_Count'])
    
    for j in tweets_competitor['Empresa'].unique():
        
        ### 3. Segmentar tweets_competitor por empresa para cada loop
        
        tweets_competitor_specific = tweets_competitor[tweets_competitor['Empresa'] == j]
        
        ### 4. List Tweet Text
        
        tweet_texts = tweets_competitor_specific['Tweet_Text'].tolist()
        
        ### 5. Remove stopwords
        tweet_texts_stopwords = []
        
        if 'S' == 'S':
            for i in tweet_texts:
                tweet_texts_stopwords.append(remove_stopwords(i))
        else:
            tweet_texts_stopwords = tweet_texts
        
        ### 6. Caps Lock, Join (juntar tudo num string) + regex.sub + split(separar todas as palavras)
        
        tweet_words_joined = ' '.join(tweet_texts_stopwords)
        tweet_words_lower= tweet_words_joined.lower()
        tweet_words_replace = tweet_words_lower.replace('\'', '')
        tweet_words_regex = re.sub(r'[^a-zA-Z0-9\- ]', '',tweet_words_replace)
        tweet_words_split = tweet_words_regex.split()
        
        ### 7. Contar as palavras mais comuns
        
        contador = Counter(tweet_words_split)
        
        most_occur = contador.most_common(top_words)
        
        ### 8. Excluir items que nao sao palavras + juntar a lista
        
        for i in range(len(most_occur)):
            
            most_occur_no_space = []
            
            if most_occur[i][0] == ' ' or most_occur[i][0] == '  ':
                continue
            else:
                most_occur_no_space.append(time_block)
                most_occur_no_space.append(company)
                most_occur_no_space.append("Competitor")
                most_occur_no_space.append(j)
                most_occur_no_space.append((datetime.datetime.now()).strftime("%Y-%m-%d"))
                most_occur_no_space.append(remove_stop_words)
                most_occur_no_space.append(exclude_retweets)
                most_occur_no_space.append(language)
                most_occur_no_space.append(most_occur[i][0])
                most_occur_no_space.append(most_occur[i][1])
                
                ### Dump no DataFrame ###
                
                count_competitor.loc[len(count_competitor)]=most_occur_no_space
            
    time_end= time.time()
    
    print('Tempo de execução: ' , time_end-time_start)
    
    return count_competitor

############################ COUNT WORDS COMPANY ##################################################

def count_words_company(company, previous_in_hours = 6, count = 10, language = "en", exclude_retweets = "S", top_words = 30, remove_stop_words = 'S' , time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    time_start = time.time()
    
    ### 1. Get Tweets
    
    tweets_company = get_tweets_last_day_company(company = company, previous_in_hours = previous_in_hours, count = count, language = language, exclude_retweets = exclude_retweets, time_block = time_block)
    
    ### 2. Criar dataframe final
    
    count_company = pd.DataFrame(data = None, 
                             columns = ['Time_Block','Bloco','SelfOrCompetitor','Empresa','Present_Date','Remove_Stopwords',
                                        'Exclude_Retweets', 'Language' ,'Word', 'Word_Count'])
    
    ### 3. List Tweet Text
    
    tweet_texts = tweets_company['Tweet_Text'].tolist()
    
    ### 4. Remove stopwords
    tweet_texts_stopwords = []
    
    if 'S' == 'S':
        for i in tweet_texts:
            tweet_texts_stopwords.append(remove_stopwords(i))
    else:
        tweet_texts_stopwords = tweet_texts
    
    ### 5. Caps Lock, Join (juntar tudo num string) + regex.sub + split(separar todas as palavras)
    
    tweet_words_joined = ' '.join(tweet_texts_stopwords)
    tweet_words_lower= tweet_words_joined.lower()
    tweet_words_replace = tweet_words_lower.replace('\'', '')
    tweet_words_regex = re.sub(r'[^a-zA-Z0-9\- ]', '',tweet_words_replace)
    tweet_words_split = tweet_words_regex.split()
    
    ### 6. Contar as palavras mais comuns
    
    contador = Counter(tweet_words_split)
    
    most_occur = contador.most_common(top_words)
    
    ### 7. Excluir items que nao sao palavras + juntar a lista
    
    for i in range(len(most_occur)):
        
        most_occur_no_space = []
        
        if most_occur[i][0] == ' ' or most_occur[i][0] == '  ':
            continue
        else:
            most_occur_no_space.append(time_block)
            most_occur_no_space.append(company)
            most_occur_no_space.append("Self")
            most_occur_no_space.append(company)
            most_occur_no_space.append((datetime.datetime.now()).strftime("%Y-%m-%d"))
            most_occur_no_space.append(remove_stop_words)
            most_occur_no_space.append(exclude_retweets)
            most_occur_no_space.append(language)
            most_occur_no_space.append(most_occur[i][0])
            most_occur_no_space.append(most_occur[i][1])
            
            ### Dump no DataFrame ###
            
            count_company.loc[len(count_company)]=most_occur_no_space
    
    time_end= time.time()
    
    print('Tempo de execução: ' , time_end-time_start)
    
    return count_company

####################### COUNT WORDS TUDO JUNTO ##############################

def count_words_combined(company, previous_in_hours = 6, count = 10, language = "en", exclude_retweets = 'S', top_words = 30, remove_stop_words = 'S', time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    time_start = time.time()
    
    Company_Words= count_words_company(company, previous_in_hours = previous_in_hours,count = count, language = language, exclude_retweets = exclude_retweets, top_words = top_words, remove_stop_words =remove_stop_words, time_block = time_block)
    Competitor_Words= count_words_competitor(company, previous_in_hours = previous_in_hours, count = round(count / 2), language = language, exclude_retweets = exclude_retweets, top_words = top_words, remove_stop_words = remove_stop_words , time_block = time_block)
    
    ### Juntar os datasets com a company em 1º lugar
    
    Combined_Words= Company_Words.append(Competitor_Words)
    
    time_end= time.time()
    
    print('Tempo de execução: ' , time_end-time_start)
    
    return Combined_Words


####################### GET TWEETS FEITOS PELA EMPRESA ######################

#### COLOQUEI O SCREENNAME DA EMPRESA COMO FILTRO, MAS ATENÇAO QUE PODE MUDAR

def get_tweets_last_day_tweeted_by_company(company, previous_in_hours = 6,  count = 10, language = "en", exclude_retweets = 'S', time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    
    time_start= time.time()
    
    ######## Authenticate ################
    
    api = tweepy_authenticate_user()
    
    ######## Incluir erros ortográficos #####
    
    #company_total = company_variations(company)# Comentado pois nao estava a fazer diferença
                                                # Se descomentar trocar "i" por "company" na variaver "filtro"
                                                # Se descomentar dar + 1 indent desde o cursor ate ao continue (inclusive)
    
    ####### Include retweets? Avaliar #######
    
    if exclude_retweets == 'S':
        filtro = 'company -filter:retweets'
        condicao_if = "(not tweet.retweeted) and ('RT @' not in tweet.text)"
    else:
        filtro = 'company'
        condicao_if = '1 == 1'
        
    
    ######## Create DataFrame ###############
    
    tweets_df = pd.DataFrame(data = None, 
                             columns = ['Time_Block','Bloco','SelfOrCompetitor','Empresa','Present_Date','Exclude_Retweets', 'Tweet_Date', 'Tweet_Year', 'Tweet_Month','Tweet_Day', 'Tweet_Hour',
                                        'Tweet_Minute', 'Tweet_Second', 'Tweet_ID', 'Tweet_Text', 'Tweet_Lang','Tweet_Likes', 'Tweet_Retweets', 
                                        'Tweet_Comments',
                                        'User_Screenname' , 'User_Name', 'User_ID', 'User_Verified', 'User_Location', 'User_Followers',
                                        'User_Friends', 'User_Account_Creation'])
    
    
    ####### Cursor : Percorrer Twitter ########
    
    #for i in company_total: # Comentado pois nao estava a fazer diferença
                             # Se descomentar trocar "company" por "i" na variaver "filtro"
                             # Se descomentar dar + 1 indent desde o cursor ate ao continue (inclusive)
    
    tweets = tweepy.Cursor(api.user_timeline, screen_name = company.lower()  , 
                           tweet_mode="Extended"
                           )
    
    for tweet in tweets.items(count):
        
        ### Condiçao temporal - timezone do API é UTC
        
        if tweet.created_at >= (datetime.datetime.now()).astimezone(pytz.timezone('UTC')) - datetime.timedelta(hours = previous_in_hours):
        
            ### Excluir retweets
           
            if condicao_if: 
            
            ### Inicializar Lista ###
            
                tweets_list = []
            
            ### Acrescentar dados à lista ###
            
                tweets_list.append(time_block)
                tweets_list.append(company)
                tweets_list.append('Self')
                tweets_list.append(company)
                tweets_list.append((datetime.datetime.now()).strftime("%Y-%m-%d"))
                tweets_list.append(exclude_retweets)
                tweets_list.append(tweet.created_at)
                tweets_list.append(tweet.created_at.year)
                tweets_list.append(tweet.created_at.month)
                tweets_list.append(tweet.created_at.day)
                tweets_list.append(tweet.created_at.hour)
                tweets_list.append(tweet.created_at.minute)
                tweets_list.append(tweet.created_at.second)
                tweets_list.append(tweet.id)
                tweets_list.append(remove_URL(tweet.text))
                tweets_list.append(tweet.lang)
                tweets_list.append(tweet.favorite_count)
                tweets_list.append(tweet.retweet_count)
                tweets_list.append(len(tweet._json['entities']['hashtags']))
                tweets_list.append(tweet.user.screen_name)
                tweets_list.append(tweet.user.name)
                tweets_list.append(tweet.user.id_str)        
                tweets_list.append(tweet.user.verified)
                tweets_list.append(tweet.user.location)
                tweets_list.append(tweet.user.followers_count)
                tweets_list.append(tweet.user.friends_count)
                tweets_list.append(tweet.user.created_at)
            
            ### Dump no DataFrame ###
            
                tweets_df.loc[len(tweets_df)]=tweets_list
            
            else:
                continue
        else:
            continue
    
    time_end= time.time()
    
    print('Tempo de execução: ' , time_end-time_start)
    
    return  tweets_df


####################### GET TWEETS FEITOS PELA COMPETICAO COM O NOME DA EMPRESA######################

#### COLOQUEI O SCREENNAME DA EMPRESA COMO FILTRO, MAS ATENÇAO QUE PODE MUDAR

def get_tweets_last_day_tweeted_by_competitor(company, previous_in_hours = 6, count = 10, language = "en", exclude_retweets = 'S', time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    time_start = time.time()
    
    ######## Authenticate ################
    
    api = tweepy_authenticate_user()
    
    ####### Obter Competiçao
    
    competition = get_competitors(company)
    
    ####### Include retweets? Avaliar #######
    
    if exclude_retweets == 'S':
        filtro = 'i -filter:retweets'
        condicao_if = "(not tweet.retweeted) and ('RT @' not in tweet.text)"
    else:
        filtro = 'i'
        condicao_if = '1 == 1'
    
    ######## Create DataFrame ###############
    
    tweets_df = pd.DataFrame(data = None, 
                             columns = ['Time_Block','Bloco','SelfOrCompetitor','Empresa', 'Present_Date','Exclude_Retweets', 'Tweet_Date', 'Tweet_Year', 'Tweet_Month','Tweet_Day', 'Tweet_Hour',
                                        'Tweet_Minute', 'Tweet_Second', 'Tweet_ID', 'Tweet_Text', 'Tweet_Lang','Tweet_Likes', 'Tweet_Retweets', 
                                        'Tweet_Comments',
                                        'User_Screenname' , 'User_Name', 'User_ID', 'User_Verified', 'User_Location', 'User_Followers',
                                        'User_Friends', 'User_Account_Creation'])
        
    ####### Iterar por competidor
    
    for i in competition:
    
    ######## Incluir erros ortográficos #####
    
        #company_total = company_variations(i) # Comentado pois nao estava a fazer diferença
                                               # Se descomentar trocar "i" por "j" na variaver "filtro"
                                               # Se descomentar dar + 1 indent desde o cursor ate ao continue (inclusive)
    
    
    ####### Cursor : Percorrer Twitter ########
    
        #for j in company_total: # Comentado pois nao estava a fazer diferença
                                 # Se descomentar trocar "i" por "j" na variaver "filtro"
                                 # Se descomentar dar + 1 indent desde o cursor ate ao continue (inclusive)
    
        tweets = tweepy.Cursor(api.user_timeline,screen_name = i, 
                               tweet_mode="Extended"
                               )
    
        for tweet in tweets.items(count):
            
            ### Condiçao temporal - timezonde do API é UTC
            
            if tweet.created_at  >= (datetime.datetime.now()).astimezone(pytz.timezone('UTC')) - datetime.timedelta(hours = previous_in_hours):
            
                ### Excluir retweets
               
                if condicao_if and company in tweet.text: 
                
                ### Inicializar Lista ###
                
                    tweets_list = []
                
                ### Acrescentar dados à lista ###
                    
                    tweets_list.append(time_block)
                    tweets_list.append(company)
                    tweets_list.append('Competitor') 
                    tweets_list.append(i)
                    tweets_list.append((datetime.datetime.now()).strftime("%Y-%m-%d"))
                    tweets_list.append(exclude_retweets)
                    tweets_list.append(tweet.created_at)
                    tweets_list.append(tweet.created_at.year)
                    tweets_list.append(tweet.created_at.month)
                    tweets_list.append(tweet.created_at.day)
                    tweets_list.append(tweet.created_at.hour)
                    tweets_list.append(tweet.created_at.minute)
                    tweets_list.append(tweet.created_at.second)
                    tweets_list.append(tweet.id)
                    tweets_list.append(remove_URL(tweet.text))
                    tweets_list.append(tweet.lang)
                    tweets_list.append(tweet.favorite_count)
                    tweets_list.append(tweet.retweet_count)
                    tweets_list.append(len(tweet._json['entities']['hashtags']))
                    tweets_list.append(tweet.user.screen_name)
                    tweets_list.append(tweet.user.name)
                    tweets_list.append(tweet.user.id_str)        
                    tweets_list.append(tweet.user.verified)
                    tweets_list.append(tweet.user.location)
                    tweets_list.append(tweet.user.followers_count)
                    tweets_list.append(tweet.user.friends_count)
                    tweets_list.append(tweet.user.created_at)
                    
                    tweets_df.loc[len(tweets_df)]=tweets_list
                
                else:
                    continue
            else:
                continue


    time_end= time.time()
    
    print('Tempo de execução: ' , time_end-time_start)
    
    return  tweets_df
    
############################# MAIN #########3###############################

if __name__ == '__main__':
    
    
    time_start = time.time()
    a = get_tweets_last_day_tweeted_by_company('Netflix')
    time_end = time.time()
    
    print('Tempo de execução: ' , time_end-time_start)
    