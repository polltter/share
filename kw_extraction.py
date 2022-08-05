import json
import pandas as pd

import nltk
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


# nltk.download('stopwords')
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
    corpus = textacy.Corpus("en_core_web_sm", load_tweets()['text'])
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