import spacy
import re
import emoji
from get_client_info import *
import requests
import datetime
import translators as ts
import pandas as pd
import textacy
import textacy.resources
from collections import Counter
from transformers import pipeline

textacy.set_doc_extensions("extract")
API_URL = 'http://saas.test'


class ReputationalAnalysi:

    def __init__(self, tenant_id, clients, collection, analysis, files: list):
        self.tenant = tenant_id
        self.clients = clients
        self.collection = collection
        self.analysis = analysis
        self.nlp = spacy.load('pt_core_news_sm')
        self.load_file(files)

    def clean_text(self, text: str):
        # Remove emojis
        text = emoji.demojize(text)
        # Remove emoji text representations
        text = re.sub(r":[\w_]+:", "", text)
        # Remove non-alphanumeric characters
        text = re.sub(r"[^\w\s]", "", text)
        # Remove excess whitespace
        text = re.sub(r"\s+", " ", text)
        # Convert to lowercase
        return text.lower()

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

    def translate(self, text: str, lang: str = "pt"):
        print('Translating...')
        text_list = list(self.create_text(text))
        text_en = ''
        try:
            for t in text_list:
                print(len(t))
                text_en += ts.translate_text(query_text=t, from_language=lang, to_language='en', translator='google')
        except TypeError:
            return self.translate(text)
        return text_en

    def __find_emotions(self, text):
        print("Finding emotions")
        # corpus = textacy.Corpus("en_core_web_sm", text)
        # rs = textacy.resources.DepecheMood(lang="en", word_rep="lemma", min_freq=2)
        model = "j-hartmann/emotion-english-distilroberta-base"
        classifier = pipeline("text-classification", model, truncation=True, padding=True)
        return classifier(text, top_k=3)
        # return rs.get_emotional_valence(corpus.docs[0])

    def emotion(self):
        self.dataframe['emotion'] = self.dataframe.apply(lambda x: self.__find_emotions(x['text_en']), axis=1)

    def __sentiment_analysis(self, text):
        print('Analysing sentiment...')
        classifier = pipeline(model="mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis",
                              truncation=True, padding=True)
        sentiment_analysis = classifier(text, top_k=3)
        return sentiment_analysis

    def sentiment(self):
        self.dataframe['sentiment'] = self.dataframe.apply(lambda x: self.__sentiment_analysis(x['text_en']), axis=1)

    def __keywords(self, text, lang='pt'):
        print('finding key_words...')
        if lang == 'pt':
            corpus = textacy.Corpus("pt_core_news_sm", text)
        else:
            corpus = textacy.Corpus("en_core_web_sm", text)
        kw_weights = Counter()
        for doc in corpus:
            keywords = doc._.extract_keyterms("textrank", normalize="lower", window_size=2, edge_weighting="binary",
                                              topn=10)
            kw_weights.update(dict(keywords))

        # convert counter values to integers between 0 and 100
        maximum = max(kw_weights.values())
        minimum = min(kw_weights.values())
        max_min = maximum - minimum

        for k in kw_weights.keys():
            kw_weights[k] = round(((kw_weights[k] - minimum) / (max_min)) * 100)

        # exclude items with value=0
        return {key: val for key, val in kw_weights.items() if val > 0}

    def extract_keywords(self):
        self.dataframe['kw_weights'] = self.dataframe.apply(lambda x: self.__keywords(x['text'], 'pt'), axis=1)

    def __build_objs_sentiment(self):
        clients_data = {}
        for c, s, d in zip(self.dataframe.clients, self.dataframe.sentiment, self.dataframe.date_scrap):
            for obj in c:
                try:
                    clients_data[obj['id']][s[0]['label']] += 1
                except KeyError:
                    clients_data[obj['id']] = {}
                    clients_data[obj['id']]['neutral'] = 0
                    clients_data[obj['id']]['negative'] = 0
                    clients_data[obj['id']]['positive'] = 0
                    clients_data[obj['id']][s[0]['label']] += 1
                    clients_data[obj['id']]['date'] = d
        return clients_data

    def __build_data_setiment(self):
        client_data = self.__build_objs_sentiment()
        for client in client_data:
            yield {
                "ainfo_id": client,
                "data": json.dumps({
                    "positive_count": client_data[client]['positive'],
                    "negative_count": client_data[client]['negative'],
                    "neutral_count": client_data[client]['neutral']
                }),
                "year": datetime.date.today().strftime("%Y"),
                "month": datetime.date.today().strftime("%m"),
                "week_of_year": datetime.date.today().strftime("%W"),
                "extracted_at": datetime.datetime.strptime(client_data[client]['date'], '%d/%m/%y').strftime('%Y-%m-%d')
            }

    def top_moods(self, moods: list, top: int):
        return moods[top]['label']

    def __build_obj_emotions(self):
        self.dataframe['top1'] = self.dataframe.emotion.apply(lambda x: self.top_moods(x, 0))
        self.dataframe['top2'] = self.dataframe.emotion.apply(lambda x: self.top_moods(x, 1))
        self.dataframe['top3'] = self.dataframe.emotion.apply(lambda x: self.top_moods(x, 2))
        emotions = [{'label': 'neutral'},
                    {'label': 'anger'},
                    {'label': 'disgust'},
                    {'label': 'joy'},
                    {'label': 'fear'},
                    {'label': 'sadness'},
                    {'label': 'surprise'}]
        clients_data = {}
        for c, d in zip(self.dataframe.clients, self.dataframe.date_scrap):
            for obj in c:
                clients_data[obj['id']] = {}
                clients_data[obj['id']]['n_texts'] = len(self.dataframe)
                clients_data[obj['id']]['date'] = d
                clients_data[obj['id']]['neutral'] = 0
                clients_data[obj['id']]['anger'] = 0
                clients_data[obj['id']]['disgust'] = 0
                clients_data[obj['id']]['joy'] = 0
                clients_data[obj['id']]['fear'] = 0
                clients_data[obj['id']]['sadness'] = 0
                clients_data[obj['id']]['surprise'] = 0
                for emotion in emotions:
                    clients_data[obj['id']][emotion['label']] = len(self.dataframe.where(
                        (self.dataframe['top1'] == emotion['label']) | (self.dataframe['top2'] == emotion['label']) | (
                                self.dataframe['top3'] == emotion['label'])).dropna())
        return clients_data

    def __build_data_emotion(self):
        client_data = self.__build_obj_emotions()
        print(client_data)
        for client in client_data:
            yield {
                'ainfo_id': client,
                'data': json.dumps({
                    'n_tweets': client_data[client]['n_texts'],
                    'neutral_count': client_data[client]['neutral'],
                    'neutral_percent': round(client_data[client]['neutral'] / client_data[client]['n_texts'], 2),
                    'anger_count': client_data[client]['anger'],
                    'anger_percent': round(client_data[client]['anger'] / client_data[client]['n_texts'], 2),
                    'disgust_count': client_data[client]['disgust'],
                    'disgust_percent': round(client_data[client]['disgust'] / client_data[client]['n_texts'], 2),
                    'joy_count': client_data[client]['joy'],
                    'joy_percent': round(client_data[client]['joy'] / client_data[client]['n_texts'], 2),
                    'fear_count': client_data[client]['fear'],
                    'fear_percent': round(client_data[client]['fear'] / client_data[client]['n_texts'], 2),
                    'sadness_count': client_data[client]['sadness'],
                    'sadness_percent': round(client_data[client]['sadness'] / client_data[client]['n_texts'], 2),
                    'surprise_count': client_data[client]['surprise'],
                    'surprise_percent': round(client_data[client]['surprise'] / client_data[client]['n_texts'], 2),
                }),
                "year": datetime.date.today().strftime("%Y"),
                "month": datetime.date.today().strftime("%m"),
                "week_of_year": datetime.date.today().strftime("%W"),
                "extracted_at": datetime.datetime.strptime(client_data[client]['date'], '%d/%m/%y').strftime('%Y-%m-%d')
            }

    # %%
    def __build_objs_kw(self):
        clients_data = {}
        for c, s, d in zip(self.dataframe.clients, self.dataframe.kw_weights, self.dataframe.date_scrap):
            for obj in c:
                try:
                    clients_data[obj['id']]['kw'].append(s)
                except KeyError:
                    clients_data[obj['id']] = {}
                    clients_data[obj['id']]['kw'] = [s]
                    clients_data[obj['id']]['date'] = d
        for client in clients_data:
            final_dict = {}
            final_dict['n_analysis'] = 0
            final_dict['id'] = client
            final_dict['kw'] = {}
            final_dict['date'] = clients_data[client]['date']
            for dictionary in clients_data[client]['kw']:
                for kw in dictionary:
                    try:
                        final_dict['kw'][kw] += dictionary[kw]
                    except KeyError:
                        final_dict['kw'][kw] = dictionary[kw]
                final_dict['n_analysis'] += 1
            yield final_dict

    def __build_top_kw(self, temp):
        top_kw = {}
        for t in temp:
            if t[1] < 25:
                break
            if len(t[0]) > 3:
                top_kw[t[0]] = t[1]
        return top_kw

    def __build_data_kw(self):
        client_data = list(self.__build_objs_kw())
        for d in client_data:
            for k in d['kw']:
                d['kw'][k] /= d['n_analysis']
            try:
                maximum = max(d['kw'].values())
                minimum = min(d['kw'].values())
                max_min = maximum - minimum
                for k in d['kw']:
                    d['kw'][k] = round(((d['kw'][k] - minimum) / max_min) * 100)
            except ValueError:
                continue
        for client in client_data:
            temp = sorted(client['kw'].items(), key=lambda item: item[1], reverse=True)
            yield {
                "ainfo_id": client['id'],
                "data": json.dumps({"kw_weights": self.__build_top_kw(temp)}),
                "year": datetime.date.today().strftime("%Y"),
                "month": datetime.date.today().strftime("%m"),
                "week_of_year": datetime.date.today().strftime("%W"),
                "extracted_at": datetime.datetime.strptime(client['date'], '%d/%m/%y').strftime(
                    '%Y-%m-%d')
            }

    def save(self):
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
                   'Authorization': 'Bearer 2|IIvhcPW0VLmm11NXAuEVOxQMI1GLdyJ8cUntGzBB',
                   'X-Tenant': self.tenant}

        # self.dataframe.to_pickle("analise_feita.pickle")

        for raw_url, analysi in zip(post_links, analysis):
            for result in analysi:
                data = json.dumps(result)
                print(data)
                response = requests.post(raw_url, headers=headers, data=data)
                print(response.status_code)
                print(response.json())

    def filter_search_terms(self, text):
        clients = []
        for client in self.clients:
            for term in client["search_terms"]:
                if term.lower() in text.lower():
                    clients.append(client)
                    break
        return clients

    def load_file(self, files):
        self.dataframe = pd.DataFrame(columns=["title", "text", "date_news", "date_scrap", "url", "lang", "clients"])
        # for file in files:
        #     df = pd.read_json(file)
        #     df['clients'] = df['text'].apply(self.filter_search_terms)
        #     self.dataframe = pd.concat([self.dataframe, df[df['clients'].map(lambda x: len(x) > 0)]], axis=0)
        # self.dataframe['text_en'] = self.dataframe.apply(lambda x: self.translate(x['text'], x['lang']), axis=1)
        # self.dataframe['text'] = self.dataframe['text'].apply(lambda x: self.clean_text(x))
        # self.dataframe['text_en'] = self.dataframe['text_en'].apply(lambda x: self.clean_text(x))
        # self.dataframe['title_en'] = self.dataframe.apply(lambda x: self.translate(x['title'], x['lang']), axis=1)


if __name__ == '__main__':
    tenants = get_analysis()

    for tenant in tenants:
        print(tenant)
        analysi = ReputationalAnalysi(tenant, tenants[tenant], "", "", ["data/economico.json", "data/exame.json"])
        analysi.dataframe = pd.read_pickle('analise_feita.pickle')
        # analysi.sentiment()
        # analysi.extract_keywords()
        # analysi.emotion()
        analysi.save()
