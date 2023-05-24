import textacy
textacy.set_doc_extensions("extract")

from collections import Counter

import re

from datetime import datetime

import pandas as pd


def extract_kw(df):
   
    corpus = textacy.Corpus("en_core_web_sm", df['text'])

    kw_weights = Counter()

    for doc in corpus:
        keywords = doc._.extract_keyterms("textrank", normalize="lower", window_size=2, edge_weighting="binary", topn=10)
        kw_weights.update(dict(keywords))

    # convert counter values to integers between 0 and 100
    maximum = max(kw_weights.values())
    minimum = min(kw_weights.values())
    max_min = maximum - minimum

    for k in kw_weights.keys():
        kw_weights[k] = round(((kw_weights[k] - minimum) / (max_min)) * 100)
    
    # exclude items with value=0
    kw_weights = {key: val for key, val in kw_weights.items() if val > 0}

    return kw_weights


def clean_kw(df):
    
    regex_pattern = re.compile(pattern = "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U0001F911-\U0001F99F"  # extra
                           "]+", flags = re.UNICODE)

    kw_weights_clean = {}

    for kw, weight in extract_kw(df).items():
        # exclude kw with handles, links and n-grams with n > 3
        if not (kw.startswith('@') or 'http' in kw) and len(kw.split()) < 4:
            # exclude kw with emojis
            if regex_pattern.sub(r'', kw) == kw:
                kw_weights_clean[kw] = weight

    return kw_weights_clean


ymd = '%Y-%m-%d'
today = datetime.today().strftime(ymd)

def get_keywords(db, word_freq, additional_stopwords=None):
  
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
    
    kw_freq_weight = db['kw_freq_weight'] # collection kw_freq_weight

    #extracted_at = datetime.utcnow().strftime('%Y-%m-%d') # confirmar se precisamos disto ou se usamos a variável global today
    kw_freq_weight.insert_one({"kw_weights": counter, "extracted_at": today})


def agg_kw_daily(db, date):
   
    kw_freq_weight = db['kw_freq_weight'] # collection kw_freq_weight
    kw_daily = db['kw_daily'] # collection kw_daily

    my_query = {"extracted_at": {"$eq": date}}
    cursor = kw_freq_weight.find(my_query)

    kw_weights = list(cursor)[0]['kw_weights']

    dt = datetime.strptime(date, "%Y-%m-%d")
    # date_ymw is a tuple with the form (date, year, month, week)
    date_ymw = (date, dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%U"))

    doc = {"extracted_at": date_ymw[0], "year": date_ymw[1], "month": date_ymw[2], "week_of_year": date_ymw[3],
           "kw_weights": kw_weights}

    kw_daily.insert_one(doc)