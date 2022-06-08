import SentimentClassification as sc
import GetTweets as gt
import RepresentacaoGrafica as rg
import os
import datetime
import pandas as pd


########################### FUNCTION: EXPORT TO EXCEL (NAO ESTA A SER USADO) #########################################

######### GET TWEETS

def to_excel_tweets_company(company, previous_in_hours = 6,count = 10, language = "en", exclude_retweets = 'S',time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    ### 1. Funcao
    
    dataframe = gt.get_tweets_last_day_company(company = company,previous_in_hours=previous_in_hours, count = count, language = language, exclude_retweets = exclude_retweets,time_block=time_block)       
    
    ### 2. filename + path
    
    filename = datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + "_" + company + "_Tweets_Company.xlsx"
    out_path = "C:/Users/gffar/TweepyProject/Spyder/Projeto/Twitter/Excel/tweets_company_last_day/"+filename
    
    ### 3. Retirar timezones
    
    date_columns = dataframe.select_dtypes(include=['datetime64[ns, UTC]']).columns
    for date_column in date_columns:
        dataframe[date_column] = dataframe[date_column].dt.date
    
    ### 4. Export
    
    dataframe.to_excel(out_path)

def to_excel_tweets_competitor(company, previous_in_hours = 6,count = 10, language = "en", exclude_retweets = 'S',time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    ### 1. Funcao
    
    dataframe = gt.get_tweets_last_day_competitor(company = company, previous_in_hours=previous_in_hours,count = count, language = language, exclude_retweets = exclude_retweets,time_block=time_block)       
    
    ### 2. filename + path
    
    filename = datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + "_" + company + "_Tweets_Competitor.xlsx"
    out_path = "C:/Users/gffar/TweepyProject/Spyder/Projeto/Twitter/Excel/tweets_company_last_day/"+filename
    
    ### 3. Retirar timezones
    
    date_columns = dataframe.select_dtypes(include=['datetime64[ns, UTC]']).columns
    for date_column in date_columns:
        dataframe[date_column] = dataframe[date_column].dt.date
    
    ### 4. Export
    
    dataframe.to_excel(out_path)   
    
def to_excel_tweets_combined(company, previous_in_hours = 6,count = 10, language = "en", exclude_retweets = 'S',time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    ### 1. Funcao
    
    dataframe = gt.get_tweets_last_day_combined(company = company,previous_in_hours=previous_in_hours, count = count, language = language, exclude_retweets = exclude_retweets,time_block=time_block)       
    
    ### 2. filename + path
    
    filename = datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + "_" + company + "_Tweets_Combined.xlsx"
    out_path = "C:/Users/gffar/TweepyProject/Spyder/Projeto/Twitter/Excel/tweets_company_last_day/"+filename
    
    ### 3. Retirar timezones
    
    date_columns = dataframe.select_dtypes(include=['datetime64[ns, UTC]']).columns
    for date_column in date_columns:
        dataframe[date_column] = dataframe[date_column].dt.date
    
    ### 4. Export
    
    dataframe.to_excel(out_path)
    
######## COUNT WORDS
    
def to_excel_count_word_company(company, previous_in_hours = 6,count = 10, language = "en", exclude_retweets = 'S', top_words = 30, remove_stop_words = 'S',time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    ### 1. Funcao
    
    dataframe = gt.count_words_company(company = company,previous_in_hours=previous_in_hours, count = count, language = language, exclude_retweets = exclude_retweets, top_words = top_words, remove_stop_words = remove_stop_words,time_block=time_block)
    
    ### 2. filename + path
    
    filename = datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + "_" + company + "_Count_Word_Company.xlsx"
    out_path = "C:/Users/gffar/TweepyProject/Spyder/Projeto/Twitter/Excel/count_tweet_words/"+filename
    
    ### 3. Retirar timezones
    
    date_columns = dataframe.select_dtypes(include=['datetime64[ns, UTC]']).columns
    for date_column in date_columns:
        dataframe[date_column] = dataframe[date_column].dt.date
    
    ### 4. Export
    
    dataframe.to_excel(out_path) 
    
def to_excel_count_word_competitor(company, previous_in_hours = 6,count = 10, language = "en", exclude_retweets = 'S', top_words = 30, remove_stop_words = 'S',time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    ### 1. Funcao
    
    dataframe = gt.count_words_competitor(company = company,previous_in_hours=previous_in_hours, count = count, language = language, exclude_retweets = exclude_retweets, top_words = top_words, remove_stop_words = remove_stop_words,time_block=time_block)
    
    ### 2. filename + path
    
    filename = datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + "_" + company + "_Count_Word_Competitor.xlsx"
    out_path = "C:/Users/gffar/TweepyProject/Spyder/Projeto/Twitter/Excel/count_tweet_words/"+filename
    
    ### 3. Retirar timezones
    
    date_columns = dataframe.select_dtypes(include=['datetime64[ns, UTC]']).columns
    for date_column in date_columns:
        dataframe[date_column] = dataframe[date_column].dt.date
    
    ### 4. Export
    
    dataframe.to_excel(out_path)
    
def to_excel_count_word_combined(company, previous_in_hours = 6,count = 10, language = "en", exclude_retweets = 'S', top_words = 30, remove_stop_words = 'S',time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    ### 1. Funcao
    
    dataframe = gt.count_words_combined(company = company, previous_in_hours=previous_in_hours,count = count, language = language, exclude_retweets = exclude_retweets, top_words = top_words, remove_stop_words = remove_stop_words,time_block=time_block)
    
    ### 2. filename + path
    
    filename = datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + "_" + company + "_Count_Word_Combined.xlsx"
    out_path = "C:/Users/gffar/TweepyProject/Spyder/Projeto/Twitter/Excel/count_tweet_words/"+filename
    
    ### 3. Retirar timezones
    
    date_columns = dataframe.select_dtypes(include=['datetime64[ns, UTC]']).columns
    for date_column in date_columns:
        dataframe[date_column] = dataframe[date_column].dt.date
    
    ### 4. Export
    
    dataframe.to_excel(out_path)

######## TWEET SENTIMENT

def to_excel_tweet_sentiment_company(company, previous_in_hours = 6,count = 10, language = "en", exclude_retweets = 'S',time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    ### 1. Funcao
    
    dataframe = sc.get_tweet_sentiment_company(company = company,previous_in_hours=previous_in_hours, count = count, language = language, exclude_retweets = exclude_retweets,time_block=time_block)
    
    ### 2. filename + path
    
    filename = datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + "_" + company + "_Tweet_Sentiment_Company.xlsx"
    out_path = "C:/Users/gffar/TweepyProject/Spyder/Projeto/Twitter/Excel/tweet_sentiment/"+filename
    
    ### 3. Retirar timezones
    
    date_columns = dataframe.select_dtypes(include=['datetime64[ns, UTC]']).columns
    for date_column in date_columns:
        dataframe[date_column] = dataframe[date_column].dt.date
    
    ### 4. Export
    
    dataframe.to_excel(out_path)
    
def to_excel_tweet_sentiment_competitor(company, previous_in_hours = 6,count = 10, language = "en", exclude_retweets = 'S',time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    ### 1. Funcao
    
    dataframe = sc.get_tweet_sentiment_competitor(company = company,previous_in_hours=previous_in_hours, count = count, language = language, exclude_retweets = exclude_retweets,time_block=time_block)
    
    ### 2. filename + path
    
    filename = datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + "_" + company + "_Tweet_Sentiment_Competitor.xlsx"
    out_path = "C:/Users/gffar/TweepyProject/Spyder/Projeto/Twitter/Excel/tweet_sentiment/"+filename
    
    ### 3. Retirar timezones
    
    date_columns = dataframe.select_dtypes(include=['datetime64[ns, UTC]']).columns
    for date_column in date_columns:
        dataframe[date_column] = dataframe[date_column].dt.date
    
    ### 4. Export
    
    dataframe.to_excel(out_path)
    
def to_excel_tweet_sentiment_combined(company, previous_in_hours = 6,count = 10, language = "en", exclude_retweets = 'S',time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    ### 1. Funcao
    
    dataframe = sc.get_tweet_sentiment_combined(company = company,previous_in_hours=previous_in_hours, count = count, language = language, exclude_retweets = exclude_retweets,time_block=time_block)
    
    ### 2. filename + path
    
    filename = datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + "_" + company + "_Tweet_Sentiment_Combined.xlsx"
    out_path = "C:/Users/gffar/TweepyProject/Spyder/Projeto/Twitter/Excel/tweet_sentiment/"+filename
    
    ### 3. Retirar timezones
    
    date_columns = dataframe.select_dtypes(include=['datetime64[ns, UTC]']).columns
    for date_column in date_columns:
        dataframe[date_column] = dataframe[date_column].dt.date
    
    ### 4. Export
    
    dataframe.to_excel(out_path)

######## RANKING

def to_excel_company_ranking_combined(company, previous_in_hours = 6,count = 10, language = "en", exclude_retweets = 'S',time_block = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")):
    
    ### 1. Funcao
    
    dataframe = rg.ranking_empresas_combined(company = company,previous_in_hours=previous_in_hours, count = count, language = language, exclude_retweets = exclude_retweets,time_block=time_block)
    
    ### 2. filename + path
    
    filename = datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + "_" + company + "_Company_Ranking_Combined.xlsx"
    out_path = "C:/Users/gffar/TweepyProject/Spyder/Projeto/Twitter/Excel/company_ranking/"+filename
    
    ### 3. Retirar timezones
    
    date_columns = dataframe.select_dtypes(include=['datetime64[ns, UTC]']).columns
    for date_column in date_columns:
        dataframe[date_column] = dataframe[date_column].dt.date
    
    ### 4. Export
    
    dataframe.to_excel(out_path)

############################# MAIN #########3###############################

if __name__ == '__main__':
    
    to_excel_company_ranking_combined('Netflix')
    #data_sentiment = get_tweet_sentiment_combined_no_retweets("Netflix")
    #data_sentiment = get_tweet_sentiment_combined("Netflix")
    