import os
import time
import spacy
import translators.server
from tqdm import tqdm

from get_client_info import *
import requests
import datetime
import translators as ts
import pandas as pd
import textacy
import textacy.resources
from textacy import preprocessing
from collections import Counter
from transformers import pipeline
from transformers import logging as transformers_logging

transformers_logging.set_verbosity_error()
textacy.set_doc_extensions("extract")
if os.getenv("API_URL") is not None:
    API_URL = os.getenv("API_URL")
    print('API_URL found ', API_URL)
else:
    API_URL = 'http://saas.test'

if os.getenv("BEARER_TOKEN") is not None:
    BEARER_TOKEN = os.getenv("BEARER_TOKEN")
    print('BEARER_TOKEN found ', BEARER_TOKEN)
else:
    exit('BEARER_TOKEN not found')

# API_URL = 'https://esg-maturity.com'
try:
    previous_day_dataframe = pd.read_json('scrapers/all/previous_day_data.json')
except ValueError:
    previous_day_dataframe = pd.DataFrame(columns=["raw_title", "raw_text", "date_news", "date_scrap", "url", "lang"])


def sort_dict_by_value(d: dict) -> dict:
    return {k: v for k, v in sorted(d.items(), key=lambda item: item[1], reverse=True)}


class ReputationalAnalysi:

    def __init__(self, tenant_id, clients, files: list, global_dataframe, max_articles=None):
        self.tenant = tenant_id
        self.clients = clients
        self.nlp = spacy.load('pt_core_news_sm')
        self.processed_dataframe = pd.DataFrame(columns=["raw_title", "raw_text", "date_news", "date_scrap", "url",
                                                         "lang", "clients", "text_en", "title_en", "text", "title",
                                                         "kw_weights", "sentiment", "emotion"])
        self.global_dataframe = global_dataframe
        self.load_file(files, max_articles)

    def remove_stopwords(self, text: str) -> str:
        doc = self.nlp(text)
        tokenized = [word.text for word in doc if not word.is_stop]
        return ' '.join(tokenized)

    def remove_noise(self, text: str) -> str:
        text = text.lower()
        text = preprocessing.remove.brackets(text)
        text = preprocessing.remove.punctuation(text)
        text = preprocessing.normalize.whitespace(text)
        text = preprocessing.normalize.unicode(text)
        text = preprocessing.remove.accents(text)
        text = preprocessing.replace.emojis(text, "")
        text = preprocessing.replace.hashtags(text, "")
        text = preprocessing.replace.user_handles(text, "")
        text = preprocessing.replace.urls(text, "")
        text = preprocessing.replace.numbers(text, "")
        return text

    def lemmatize_text(self, text: str) -> str:
        doc = self.nlp(text)
        lemma = [word.lemma_ for word in doc]
        return ' '.join(lemma)

    def prepocess_text(self, text: str, level: list) -> str:
        if 'all' in level:
            text = self.remove_stopwords(text)
            text = self.remove_noise(text)
            text = self.lemmatize_text(text)
        if 'stopwords' in level:
            text = self.remove_stopwords(text)
        if 'noise' in level:
            text = self.remove_noise(text)
        if 'lemmatize' in level:
            text = self.lemmatize_text(text)
        return text

    def create_text(self, text: str):
        doc = self.nlp(text)
        t = ''
        for sent in doc.sents:
            t += sent.text
            if len(t) > 4000:
                yield t
                t = ''
        if len(t):
            yield t

    def progress_bar(self, desc: str, func_to_apply, src_col, dest_col):
        total_articles = len(self.dataframe)
        pbar = tqdm(total=total_articles, desc=desc)

        def __apply_func(text):
            nonlocal pbar
            return_val = func_to_apply(text)
            pbar.update(1)
            pbar.set_postfix(analyzed=f"{pbar.n}/{total_articles}")
            return return_val

        self.dataframe[dest_col] = self.dataframe.apply(lambda x: __apply_func(x[src_col]), axis=1)
        pbar.close()

    def __translate(self, text: str, lang: str = "pt"):
        text_list = list(self.create_text(text))
        text_en = ''
        try:
            for t in text_list:
                text_en += ts.translate_text(query_text=t, from_language=lang, to_language='en', translator='google')
                time.sleep(0.5)
        except TypeError:
            return self.__translate(text)
        except translators.server.TranslatorError:
            print('TranslatorError')
            pass
        return text_en

    def __find_emotions(self, text):
        model = "j-hartmann/emotion-english-distilroberta-base"
        classifier = pipeline("text-classification", model, truncation=True, padding=True)
        return classifier(text, top_k=3)

    def __sentiment_analysis(self, text):
        classifier = pipeline(model="mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis",
                              truncation=True, padding=True)
        sentiment_analysis = classifier(text, top_k=3)
        return sentiment_analysis

    def __keywords(self, text, algo='yake', window_size=1, topn=15, lang='pt', ngram=(1,2 ,3)):
        if lang == 'pt':
            corpus = textacy.Corpus("pt_core_news_sm", text)
        else:
            corpus = textacy.Corpus("en_core_web_sm", text)

        kw_weights = Counter()

        keywords = corpus[0]._.extract_keyterms("yake", window_size=window_size, topn=topn, ngrams=ngram)
        kw_weights.update(dict(keywords))

        return dict(kw_weights)

    def __build_objs_sentiment(self):
        clients_data = {}
        self.dataframe.to_pickle('while_saving_dataframe.pickle')
        for c, s, d in zip(self.dataframe.clients, self.dataframe.sentiment, self.dataframe.date_news):
            for obj in c:
                try:
                    clients_data[obj['id']][d][s[0]['label']] += 1
                except KeyError:
                    try:
                        clients_data[obj['id']][d] = {}
                        clients_data[obj['id']][d]['neutral'] = 0
                        clients_data[obj['id']][d]['negative'] = 0
                        clients_data[obj['id']][d]['positive'] = 0
                        clients_data[obj['id']][d][s[0]['label']] += 1
                    except KeyError:
                        clients_data[obj['id']] = {}
                        clients_data[obj['id']][d] = {}
                        clients_data[obj['id']][d]['neutral'] = 0
                        clients_data[obj['id']][d]['negative'] = 0
                        clients_data[obj['id']][d]['positive'] = 0
                        clients_data[obj['id']][d][s[0]['label']] += 1
        return clients_data

    def __build_data_setiment(self):
        client_data = self.__build_objs_sentiment()
        for client in client_data:
            for date in client_data[client]:
                yield {
                    "ainfo_id": client,
                    "data": json.dumps({
                        "positive_count": client_data[client][date]['positive'],
                        "negative_count": client_data[client][date]['negative'],
                        "neutral_count": client_data[client][date]['neutral']
                    }),
                    "year": datetime.date.today().strftime("%Y"),
                    "month": datetime.date.today().strftime("%m"),
                    "week_of_year": datetime.date.today().strftime("%W"),
                    "extracted_at": datetime.datetime.strptime(date, '%d/%m/%y').strftime('%Y-%m-%d')
                }

    def top_moods(self, moods: list, top: int):
        return moods[top]['label']

    def __build_obj_emotions(self):
        emotions = [{'label': 'neutral'},
                    {'label': 'anger'},
                    {'label': 'disgust'},
                    {'label': 'joy'},
                    {'label': 'fear'},
                    {'label': 'sadness'},
                    {'label': 'surprise'}]
        clients_data = {}
        for c, d, e in zip(self.dataframe.clients, self.dataframe.date_news, self.dataframe.emotion):
            for obj in c:
                try:
                    clients_data[obj['id']][d]['n_texts'] += 1
                    for label in e:
                        clients_data[obj['id']][d][label['label']] += 1
                except KeyError:
                    try:
                        clients_data[obj['id']][d] = {}
                        clients_data[obj['id']][d]['n_texts'] = 1
                        clients_data[obj['id']][d]['neutral'] = 0
                        clients_data[obj['id']][d]['anger'] = 0
                        clients_data[obj['id']][d]['disgust'] = 0
                        clients_data[obj['id']][d]['joy'] = 0
                        clients_data[obj['id']][d]['fear'] = 0
                        clients_data[obj['id']][d]['sadness'] = 0
                        clients_data[obj['id']][d]['surprise'] = 0
                        for label in e:
                            clients_data[obj['id']][d][label['label']] += 1
                    except KeyError:
                        clients_data[obj['id']] = {}
                        clients_data[obj['id']][d] = {}
                        clients_data[obj['id']][d]['n_texts'] = 1
                        clients_data[obj['id']][d]['neutral'] = 0
                        clients_data[obj['id']][d]['anger'] = 0
                        clients_data[obj['id']][d]['disgust'] = 0
                        clients_data[obj['id']][d]['joy'] = 0
                        clients_data[obj['id']][d]['fear'] = 0
                        clients_data[obj['id']][d]['sadness'] = 0
                        clients_data[obj['id']][d]['surprise'] = 0
                        for label in e:
                            clients_data[obj['id']][d][label['label']] += 1
        return clients_data

    def __build_data_emotion(self):
        client_data = self.__build_obj_emotions()
        for client in client_data:
            for date in client_data[client]:
                yield {
                    'ainfo_id': client,
                    'data': json.dumps({
                        'n_tweets': client_data[client][date]['n_texts'],
                        'neutral_count': client_data[client][date]['neutral'],
                        'neutral_percent': round(
                            client_data[client][date]['neutral'] / client_data[client][date]['n_texts'], 2),
                        'anger_count': client_data[client][date]['anger'],
                        'anger_percent': round(
                            client_data[client][date]['anger'] / client_data[client][date]['n_texts'], 2),
                        'disgust_count': client_data[client][date]['disgust'],
                        'disgust_percent': round(
                            client_data[client][date]['disgust'] / client_data[client][date]['n_texts'], 2),
                        'joy_count': client_data[client][date]['joy'],
                        'joy_percent': round(client_data[client][date]['joy'] / client_data[client][date]['n_texts'],
                                             2),
                        'fear_count': client_data[client][date]['fear'],
                        'fear_percent': round(client_data[client][date]['fear'] / client_data[client][date]['n_texts'],
                                              2),
                        'sadness_count': client_data[client][date]['sadness'],
                        'sadness_percent': round(
                            client_data[client][date]['sadness'] / client_data[client][date]['n_texts'], 2),
                        'surprise_count': client_data[client][date]['surprise'],
                        'surprise_percent': round(
                            client_data[client][date]['surprise'] / client_data[client][date]['n_texts'], 2),
                    }),
                    "year": datetime.date.today().strftime("%Y"),
                    "month": datetime.date.today().strftime("%m"),
                    "week_of_year": datetime.date.today().strftime("%W"),
                    "extracted_at": datetime.datetime.strptime(date, '%d/%m/%y').strftime('%Y-%m-%d')
                }

    def __build_objs_kw(self):
        clients_data = {}
        for c, s, d in zip(self.dataframe.clients, self.dataframe.kw_weights, self.dataframe.date_news):
            for obj in c:
                try:
                    clients_data[obj['id']][d].append(s)
                except KeyError:
                    try:
                        clients_data[obj['id']][d] = [s]
                    except KeyError:
                        clients_data[obj['id']] = {}
                        clients_data[obj['id']][d] = [s]

        for client in clients_data:
            for date in clients_data[client]:
                final_dict = {}
                final_dict['n_analysis'] = 0
                final_dict['id'] = client
                final_dict['kw'] = {}
                final_dict['date'] = date
                for dictionary in clients_data[client][date]:
                    for kw in dictionary:
                        try:
                            final_dict['kw'][kw] *= dictionary[kw]
                        except KeyError:
                            final_dict['kw'][kw] = dictionary[kw]
                    final_dict['n_analysis'] += 1
                yield final_dict

    def __build_data_kw(self):
        client_data = list(self.__build_objs_kw())
        for d in client_data:
            try:
                to_del = []
                maximum = max(d['kw'].values())
                minimum = min(d['kw'].values())
                max_min = maximum - minimum
                for k in d['kw']:
                    d['kw'][k] = round((1 - ((d['kw'][k] - minimum) / max_min)) * 100)
                    if d['kw'][k] < 5:
                        to_del.append(k)
                for k in to_del:
                    del d['kw'][k]
            except ValueError:
                continue
        for client in client_data:
            yield {
                "ainfo_id": client['id'],
                "data": json.dumps({"kw_weights": client['kw']}),
                "year": datetime.date.today().strftime("%Y"),
                "month": datetime.date.today().strftime("%m"),
                "week_of_year": datetime.date.today().strftime("%W"),
                "extracted_at": datetime.datetime.strptime(client['date'], '%d/%m/%y').strftime(
                    '%Y-%m-%d')
            }

    def save(self):
        if not len(self.dataframe):
            print('No data to save')
            return
        analysis = [
            list(self.__build_data_setiment()),
            list(self.__build_data_emotion()),
            list(self.__build_data_kw())
        ]
        post_links = [
            f'{API_URL}/api/v1/reputational/sentiments-daily',
            f'{API_URL}/api/v1/reputational/emotions-daily',
            f'{API_URL}/api/v1/reputational/keywords-frequency-daily'
        ]
        headers = {'Accept': 'application/json',
                   'Content-Type': 'application/json',
                   'Authorization': BEARER_TOKEN,
                   # 'Authorization': 'Bearer 3|7XOfemJabZDyJCCtOzZgpomqU8JMRl4gRADZ1HZp',
                   'X-Tenant': self.tenant}

        for raw_url, analysi in zip(post_links, analysis):
            for result in analysi:
                data = json.dumps(result)
                print(data)
                response = requests.post(raw_url, headers=headers, data=data)
                print(response.status_code, response.content)
                time.sleep(1.25)

    def filter_search_terms(self, text):
        clients = []
        for client in self.clients:
            if all(term.lower() in text.lower() for term in client["search_terms"]):
                clients.append(client)
        return clients

    def check_if_already_analysed(self):
        rows_to_drop = []
        if not len(self.dataframe) or not len(self.global_dataframe):
            return
        for index, row in self.dataframe.iterrows():
            if row['raw_text'] in self.global_dataframe['raw_text'].values:
                analyzed_row = self.global_dataframe[self.global_dataframe['raw_text'] == row['raw_text']].copy()
                analyzed_row['clients'] = [row['clients']]
                self.processed_dataframe = pd.concat([self.processed_dataframe, analyzed_row], axis=0)
                rows_to_drop.append(index)
        for index in rows_to_drop:
            self.dataframe.drop(index, inplace=True)

    def load_file(self, files, max_articles=None):
        self.dataframe = pd.DataFrame(
            columns=["raw_title", "raw_text", "date_news", "date_scrap", "url", "lang", "clients"])
        for file in files:
            print('reading file')
            df = pd.read_json(file)
            df['clients'] = df['raw_text'].apply(self.filter_search_terms)
            self.dataframe = pd.concat([self.dataframe, df[df['clients'].map(lambda x: len(x) > 0)]], axis=0)
            if max_articles is not None and len(self.dataframe) >= max_articles:
                self.dataframe = self.dataframe[:max_articles]
                break
        self.check_if_already_analysed()
        print('preprocessing text', len(self.dataframe))
        if len(self.dataframe) > 0:
            self.progress_bar('Translating text.', self.__translate, 'raw_text', 'text_en')
            self.progress_bar('Translating title.', self.__translate, 'raw_title', 'title_en')
            self.dataframe['text'] = self.dataframe['raw_text'].apply(lambda x: self.prepocess_text(x, ['noise']))
            self.dataframe['title'] = self.dataframe['raw_title'].apply(lambda x: self.prepocess_text(x, ['all']))
            self.dataframe['text_en'] = self.dataframe['text_en'].apply(lambda x: self.prepocess_text(x, ['all']))
            self.dataframe['title_en'] = self.dataframe['title_en'].apply(lambda x: self.prepocess_text(x, ['all']))
            self.dataframe.to_pickle("processado_latest.pickle")

    def do_analysis(self):
        if len(self.dataframe) > 0:
            self.progress_bar('Extracting keywords', self.__keywords, 'raw_text', 'kw_weights')
            self.progress_bar('Analyzing Sentiments', self.__sentiment_analysis, 'text_en', 'sentiment')
            self.progress_bar('Analyzing Emotions', self.__find_emotions, 'text_en', 'emotion')
            self.dataframe.to_pickle(f"analise_feita_latest__{self.tenant}.pickle")
        new_global_dataframe = pd.concat([self.global_dataframe, self.dataframe], axis=0) if pd.concat(
            [self.global_dataframe, self.dataframe], axis=0) is not None else pd.DataFrame(
            columns=["raw_title", "raw_text", "date_news", "date_scrap", "url", "lang", "clients",
                     "text_en", "title_en", "text", "title", "kw_weights", "sentiment", "emotion"])
        self.dataframe = pd.concat([self.processed_dataframe, self.dataframe], axis=0)
        self.save()
        return new_global_dataframe


if __name__ == '__main__':
    tenants = get_analysis()
    global_dataframe = pd.DataFrame(
        columns=["raw_title", "raw_text", "date_news", "date_scrap", "url", "lang", "clients",
                 "text_en", "title_en", "text", "title", "kw_weights", "sentiment", "emotion"])
    for tenant in tenants:
        analysi = ReputationalAnalysi(tenant, tenants[tenant], ["scrapers/all/data.json"], global_dataframe)
        global_dataframe = analysi.do_analysis()
    os.rename("scrapers/all/data.json", "scrapers/all/previous_day_data.json")
