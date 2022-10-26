import pymongo

import json
import pandas as pd

import nltk
#nltk.download('stopwords')
from nltk.tokenize import TweetTokenizer
import string

from sklearn.feature_extraction.text import CountVectorizer

import textacy
from textacy.extract.kwic import keyword_in_context
textacy.set_doc_extensions("extract")

import re
import random
random.seed(42)

from collections import Counter

import matplotlib.pyplot as plt
from wordcloud import WordCloud

from datetime import datetime, timedelta
from collections import defaultdict


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

# this date will be the day before the one we are currently in because we are doing this everyday
#extracted_at = "2022-10-19"

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
    #cursor = data_twitter.find() # we will use all the tweets for now
    df = pd.DataFrame(list(cursor))

    df['retweets'] = df['public_metrics'].map(lambda x: x['retweet_count'])
    df['replies'] = df['public_metrics'].map(lambda x: x['reply_count'])
    df['likes'] = df['public_metrics'].map(lambda x: x['like_count'])
    df['quotes'] = df['public_metrics'].map(lambda x: x['quote_count'])

    df.drop('public_metrics', axis=1, inplace=True)

    df_final = df[df['lang'] == language].copy()

    return df_final

#print(load_tweets_mongo("2022-09-29").head())
#print(load_tweets_mongo("2022-09-29")['extracted_at'].head())
#print(load_tweets_mongo()['lang'].value_counts())


def tokens_nopunct(text):
    """Returns list of tokens replacing repeated character sequences of length 3 or greater with sequences of length 3
    and excluding Twitter handles and punctuation.

    :param text: Text to tokenize
    :type text: str
    :return: A list of tokens
    :rtype: list
    """
    punct = string.punctuation
    punct += "’" # add "’" to punct
    punct += "…" # add "…" to punct
    punct += "..." # add "..." to punct

    tokens = [token for token in TweetTokenizer(reduce_len=True, strip_handles=True).tokenize(text)]
    return [token for token in tokens if token not in punct]

#print(tokens_nopunct(load_tweets()['text'][3]))


def stopwords(language='english'):
    """Returns list of stop words.

    :param language: Language, defaults to 'english'
    :type language: str, optional
    :return: A list of stopwords
    :rtype: set
    """
    stopwords_list = set(nltk.corpus.stopwords.words(language))
    return stopwords_list

#print(len(stopwords()))


def compute_freq(df, tokenizer=tokens_nopunct, stop_words=stopwords(), n_min=1, n_max=1, min_docfreq=1):
    """Returns data frame with token frequency.

    :param df: Data frame to pass as input
    :type df: DataFrame
    :param tokenizer: Tokenizer to process text
    :type tokenizer: callable
    :param stop_words: List of stopwords to exclude
    :type stop_words: set
    :param n_min: Lower boundary of the range of n-values for different word n-grams to be extracted, defaults to 1
    :type n_min: int, optional
    :param n_max: Upper boundary of the range of n-values for different word n-grams to be extracted, defaults to 1
    :type n_max: int, optional
    :param min_docfreq: Minimum document frequency to consider when building the vocabulary - 
    ignore terms that have a document frequency strictly lower than the given threshold, defaults to 1
    :type min_docfreq: int, optional
    :return: A data frame with token frequency
    :rtype: DataFrame
    """
    bow_vectorizer = CountVectorizer(lowercase=True, 
                                     tokenizer=tokenizer, 
                                     stop_words=stop_words, 
                                     ngram_range=(n_min, n_max), 
                                     min_df=min_docfreq)
    
    cv_bow = bow_vectorizer.fit_transform(df['text'])
    df_cv_bow = pd.DataFrame(cv_bow.toarray(), columns=bow_vectorizer.get_feature_names_out())
    
    df_freq = pd.DataFrame(df_cv_bow.sum(axis = 0).index, columns=['token'])
    df_freq['freq'] = df_cv_bow.sum(axis = 0).values
    df_freq = df_freq.query('freq >= @min_docfreq') # we are using 1 as default
    
    df_freq['n-gram size'] = df_freq['token'].map(lambda x: len(str.split(x))) # size of n-gram
    df_freq.drop(df_freq[df_freq['n-gram size'] > n_max].index, inplace=True) # removes rows with n-gram size greater than expected

    df_freq['weighted_freq'] = df_freq['freq'] * df_freq['n-gram size']
    df_freq.sort_values('weighted_freq', ascending=False, inplace=True)

    df_freq.set_index('token', inplace=True)

    return df_freq

#print(compute_freq(load_tweets()))
#print(compute_freq(load_tweets_mongo()))


def kw_in_context(df, kw):
    """Prints chosen keyword in context.

    :param df: Data frame to pass as input
    :type df: DataFrame
    :param kw: Keyword to look for
    :type kw: str
    """
    kwic_list = []

    for doc in df['text']:
        if len(list(keyword_in_context(doc, kw))) > 0:
            kwic_list.append(list(keyword_in_context(doc, kw)))

    #print(len(kwic_list))

    if len(kwic_list) == 0:
        print('keyword not found!')

    elif len(kwic_list) < 5:
        for kwic in kwic_list:
            for tup in kwic:
                print(re.sub(r'[\n\t]', ' ', tup[0]) + ' [' + tup[1] + '] ' + re.sub(r'[\n\t]', ' ', tup[2]) + '\n')
    
    else:
        for sample in random.sample(kwic_list, 5):
            for tup in sample:
                print(re.sub(r'[\n\t]', ' ', tup[0]) + ' [' + tup[1] + '] ' + re.sub(r'[\n\t]', ' ', tup[2]) + '\n')

#kw_in_context(load_tweets(), "kw inexistente")
#kw_in_context(load_tweets(), "burger king")
#kw_in_context(load_tweets(), "mcdonalds")


# python -m spacy download en_core_web_sm
def extract_kw():
    """Returns dictionary with extracted keywords and their respective weights.

    :return: A dictionary with keyword-weight pairs
    :rtype: dict
    """
    yesterday = (datetime.utcnow()-timedelta(days=1)).strftime('%Y-%m-%d')
    #yesterday = "2022-09-29"
    corpus = textacy.Corpus("en_core_web_sm", load_tweets_mongo(yesterday)['text'])
    #print(corpus)

    kw_weights = Counter()

    for doc in corpus:
        keywords = doc._.extract_keyterms("textrank", normalize='lower', window_size=2, edge_weighting="binary", topn=10)
        kw_weights.update(dict(keywords))

    return kw_weights

#print(extract_kw())


def clean_kw():
    """Returns dictionary with extracted keywords and their respective weights,
    removing keywords containing Twitter handles, links or emojis.

    :return: A dictionary with keyword-weight pairs
    :rtype: dict
    """
    regex_pattern = re.compile(pattern = "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U0001F911-\U0001F99F"  # extra
                           "]+", flags = re.UNICODE)

    kw_weights_clean = {}

    for kw, weight in extract_kw().items():
        if not (kw.startswith('@') or 'http' in kw):
            if regex_pattern.sub(r'', kw) == kw:
                kw_weights_clean[kw] = weight

    return kw_weights_clean

#print(clean_kw())


def wordcloud(word_freq, title=None, max_words=50, additional_stopwords=None):
    """Plots wordcloud based on term frequency/weight.

    :param word_freq: Series/dict with term frequency/weight
    :type word_freq: Series | dict
    :param title: Plot title, defaults to None
    :type title: str, optional
    :param max_words: Maximum number of words to plot, defaults to 50
    :type max_words: int, optional
    :param additional_stopwords: Additional words to exclude, defaults to None
    :type additional_stopwords: list, optional
    """
    wc = WordCloud(width=800, height=400, 
                   background_color= "black", colormap="viridis", 
                   max_font_size=150, max_words=max_words)
    
    # convert series into dict
    if type(word_freq) == pd.Series:
        counter = Counter(word_freq.fillna(0).to_dict())
    else:
        counter = word_freq

    # remove additional stop words from frequency counter
    if additional_stopwords is not None:
        counter = {token: freq for (token, freq) in counter.items() 
                   if token not in additional_stopwords}
        
    wc.generate_from_frequencies(counter)
    
    plt.title(title, fontsize=16)

    plt.imshow(wc, interpolation='bilinear')
    plt.axis("off")
    plt.show()


def get_keywords(word_freq, additional_stopwords=None):
    """Saves terms and respective frequency or weight in a MongoDB database.

    :param word_freq: Series/dict with term frequency/weight
    :type word_freq: Series | dict
    :param additional_stopwords: Additional words to exclude, defaults to None
    :type additional_stopwords: list, optional
    """
    # convert series into dict
    if type(word_freq) == pd.Series:
        counter = Counter(word_freq.fillna(0).to_dict())
    else:
        #counter = word_freq
        counter = dict(Counter(word_freq).most_common()) # most_common(n) sorts the most common n elements by value in descending order

    # remove additional stop words from frequency counter
    if additional_stopwords is not None:
        counter = {token: freq for (token, freq) in counter.items() 
                   if token not in additional_stopwords}
    
    db = mongo_client['rep_analysis_main'] # database rep_analysis_main
    #db = mongo_client['central'] # database central
    #kw_freq_weight = db['kw_freq_weight'] # collection kw_freq_weight
    kw_freq_weight = db['kw_freq_weight_test'] # collection kw_freq_weight_test

    extracted_at = (datetime.utcnow()-timedelta(days=1)).strftime('%Y-%m-%d')
    kw_freq_weight.insert_one({"kw_weights": counter, "extracted_at": extracted_at})
    # extracted_at will be the day before the one we are currently in because we are doing this everyday


def agg_kw_daily(date):
    """Aggregates keyword extraction results on a daily basis.

    :param date: Date ("yyyy-mm-dd") of aggregation
    :type date: string
    """
    db = mongo_client['rep_analysis_main'] # database rep_analysis_main
    kw_freq_weight = db['kw_freq_weight_test'] # collection kw_freq_weight_test
    kw_daily_main = db['kw_daily_main'] # collection kw_daily_main

    my_query = {"extracted_at": {"$eq": date}}
    cursor = kw_freq_weight.find(my_query)

    kw_weights = list(cursor)[0]['kw_weights']

    dt = datetime.strptime(date, "%Y-%m-%d")
    # date_ymw is a tuple with the form (date, year, month, week)
    date_ymw = (date, dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%U"))

    doc = {"extracted_at": date_ymw[0], "year": date_ymw[1], "month": date_ymw[2], "week_of_year": date_ymw[3],
           "kw_weights": kw_weights}

    kw_daily_main.insert_one(doc)


def agg_kw_weekly(week):
    """Aggregates keyword extraction results on a weekly basis.

    :param week: Week of aggregation
    :type week: string
    """
    db = mongo_client['rep_analysis_main'] # database rep_analysis_main
    kw_daily_main = db['kw_daily_main'] # collection kw_daily_main
    kw_weekly_main = db['kw_weekly_main'] # collection kw_weekly_main
    
    my_query = {"week_of_year": {"$eq": week}}
    temp_dict = defaultdict(list)

    for doc in kw_daily_main.find(my_query):
        for k, v in doc['kw_weights'].items():
            temp_dict[k].append(v)
        
    mean_dict = {}

    for k, v in temp_dict.items():
        mean_dict[k] = sum(value for value in v) / len(v)
    
    year = list(kw_daily_main.find(my_query))[0]['year']
    id_dict = {'_id': {'year_week': [year, week]}}
    kw_dict = {'kw_weights': mean_dict}
    
    weekly_dict = {**id_dict, **kw_dict}
    
    kw_weekly_main.insert_one(weekly_dict)


def agg_kw_monthly(month, year):
    """Aggregates keyword extraction results on a monthly basis.

    :param month: Month of aggregation
    :type month: string
    :param year: Year of aggregation
    :type year: string
    """
    db = mongo_client['rep_analysis_main'] # database rep_analysis_main
    kw_daily_main = db['kw_daily_main'] # collection kw_daily_main
    kw_monthly_main = db['kw_monthly_main'] # collection kw_monthly_main
    
    my_query = {"month": {"$eq": month}}
    temp_dict = defaultdict(list)

    for doc in kw_daily_main.find(my_query):
        for k, v in doc['kw_weights'].items():
            temp_dict[k].append(v)
        
    mean_dict = {}

    for k, v in temp_dict.items():
        mean_dict[k] = sum(value for value in v) / len(v)
    
    id_dict = {'_id': {'year_month': [year, month]}}
    kw_dict = {'kw_weights': mean_dict, 'year': year}
    
    monthly_dict = {**id_dict, **kw_dict}
    
    kw_monthly_main.insert_one(monthly_dict)


def agg_kw_yearly(year):
    """Aggregates keyword extraction results on a yearly basis.

    :param year: Year of aggregation
    :type year: string
    """
    db = mongo_client['rep_analysis_main'] # database rep_analysis_main
    kw_monthly_main = db['kw_monthly_main'] # collection kw_monthly_main
    kw_yearly_main = db['kw_yearly_main'] # collection kw_yearly_main
    
    my_query = {"year": {"$eq": year}}
    temp_dict = defaultdict(list)

    for doc in kw_monthly_main.find(my_query):
        for k, v in doc['kw_weights'].items():
            temp_dict[k].append(v)
        
    mean_dict = {}

    for k, v in temp_dict.items():
        mean_dict[k] = sum(value for value in v) / len(v)
    
    id_dict = {'_id': {'year': year}}
    kw_dict = {'kw_weights': mean_dict}
    
    yearly_dict = {**id_dict, **kw_dict}
    
    kw_yearly_main.insert_one(yearly_dict)


if __name__ == '__main__':

    # wordcloud with term frequency
    #wordcloud(compute_freq(load_tweets())['freq'])
    
    # wordcloud with term weights (textrank)
    #wordcloud(clean_kw())

    # keywords with term frequency
    #get_keywords(compute_freq(load_tweets_mongo())['freq'])

    # keywords with term weights (textrank)
    get_keywords(clean_kw())

    # aggregate keyword extraction results (daily)
    #agg_kw_daily("2022-10-19")

    # aggregate keyword extraction results (weekly)
    #agg_kw_weekly("42")

    # aggregate keyword extraction results (monthly)
    #agg_kw_monthly("10", "2022")

    # aggregate keyword extraction results (yearly)
    #agg_kw_yearly("2022")

    print('Success!')