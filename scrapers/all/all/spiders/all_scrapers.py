import scrapy
import pandas as pd
from datetime import date, datetime, timedelta
from all.items import AllItem
import locale


def process_text(text_selectors):
    all_text = []
    for text in text_selectors:
        all_text += text.xpath('descendant-or-self::text()[not(ancestor::script)][not(ancestor::style)]').getall()
    all_text = " ".join(all_text)
    all_text = all_text.replace("\n", "")
    return all_text


class AllScrapersSpider(scrapy.Spider):
    stoppers = [0, 0, 0, 0]
    days = 4
    name = "all_scrapers"
    try:
        previous_day_data = pd.read_json('previous_day_data.json')
    except ValueError:
        previous_day_data = pd.DataFrame(columns=['raw_title', 'raw_text', 'date_news', 'date_scrap', 'url', 'lang'])

    def start_requests(self):
        start_urls = {
            "https://falkor-cda.bastian.globo.com/tenants/oglobo/instances/e57aff58-e68a-4317-9397-c38de2b544ab/posts/page/1": [
                self.parse_globo, {
                    'next_page': 'https://falkor-cda.bastian.globo.com/tenants/oglobo/instances/e57aff58-e68a-4317-9397-c38de2b544ab/posts/page/',
                    'stopper': 2}],
            "https://falkor-cda.bastian.globo.com/tenants/valor/instances/e0780b59-5b42-40b0-b7d8-35ccae6fe83c/posts/page/1": [
                self.parse_globo, {
                    'next_page': 'https://falkor-cda.bastian.globo.com/tenants/valor/instances/e0780b59-5b42-40b0-b7d8-35ccae6fe83c/posts/page/',
                    'stopper': 3}],
            'https://forbes.com.br/ultimas-noticias/': [self.parse_exame_forbes, {"page": 1, 'xpaths': {
                "title": '//h1[@class="post__title"]/text()',
                "date": '//span[contains(@style, "color: #AAAAAA;")]/text()',
                "text": '//div[@class="entry-content"]/p',
                "href": '//a[@class="link-title-post-1"]/@href'},
                                                                                  'base_link': '',
                                                                                  'next_page': 'https://forbes.com.br/ultimas-noticias/page/',
                                                                                  'stopper': 0}],
            'https://exame.com/ultimas-noticias/1/': [self.parse_exame_forbes, {"page": 1, 'xpaths': {
                "title": '/html/body/div/main/section/div[2]/section/div[1]/h1/text()',
                "date": '/html/body/div/main/section/div[2]/section/div[4]/div/div/span[2]/p[1]/text()',
                "text": '/html/body/div/main/section/section[1]/div/div/div/div/div[1]/p',
                "href": '//a[@class="touch-area"]/@href'},
                                                                                'base_link': 'https://exame.com',
                                                                                'next_page': 'https://exame.com/ultimas-noticias/',
                                                                                'stopper': 1}]
        }
        for url in start_urls:
            yield scrapy.Request(url=url, callback=start_urls[url][0], cb_kwargs=start_urls[url][1])
        # yield scrapy.Request(url='https://exame.com/ultimas-noticias/1/', callback=self.parse_exame, cb_kwargs={"page": 1})

    def parse_exame_forbes_content(self, response, xpaths, stopper):
        locale.setlocale(locale.LC_TIME, 'pt_PT.UTF-8')
        all_item = AllItem()
        all_item["raw_title"] = response.xpath(xpaths['title']).get()
        if stopper:
            date_from_site = ' '.join(response.xpath(xpaths['date']).getall()[1].strip().split()[:-1])
        else:
            date_from_site = response.xpath(xpaths['date']).getall()[1].strip()
        all_item["date_news"] = datetime.strptime(date_from_site, '%d de %B de %Y').strftime("%d/%m/%y")
        all_item["date_scrap"] = date.today().strftime("%d/%m/%y")
        all_item["url"] = response.url
        all_item["lang"] = "pt"
        all_item["raw_text"] = process_text(response.xpath(xpaths['text']))
        if (all_item["raw_text"] in self.previous_day_data["raw_text"].values
                or datetime.strptime(date_from_site, '%d de %B de %Y').date() >= date.today() - timedelta(days=self.days)):
            self.stoppers[stopper] = 1
        yield all_item

    def parse_exame_forbes(self, response, page, xpaths, base_link, next_page, stopper):
        links = response.xpath(xpaths['href']).getall()
        for link in links:
            yield scrapy.Request(url=''.join([base_link, link]), callback=self.parse_exame_forbes_content,
                                 cb_kwargs={"xpaths": xpaths, "stopper": stopper})
        if not self.stoppers[stopper]:
            yield scrapy.Request(url=next_page + str(page + 1), callback=self.parse_exame_forbes,
                                 cb_kwargs={"page": page + 1, "xpaths": xpaths, "base_link": base_link,
                                            "next_page": next_page, 'stopper': stopper})

    def parse_globo_content(self, response, item, stopper):
        all_item = AllItem()
        all_item["raw_title"] = item["content"]["title"]
        all_item["date_news"] = datetime.strptime(item["created"], '%Y-%m-%dT%H:%M:%S.%fZ').strftime("%d/%m/%y")
        all_item["date_scrap"] = date.today().strftime("%d/%m/%y")
        all_item["url"] = item["content"]["url"]
        all_item["lang"] = "pt"
        all_item["raw_text"] = process_text(response.xpath('//p[contains(@class,"content-text")]'))
        if all_item["raw_text"] in self.previous_day_data["raw_text"].values:
            self.stoppers[stopper] = 1
        yield all_item

    def parse_globo(self, response, next_page, stopper):
        res_json = response.json()
        items = res_json["items"]
        for item in items:
            yield scrapy.Request(url=item["content"]["url"], callback=self.parse_globo_content,
                                 cb_kwargs={"item": item, 'stopper': stopper})
        if datetime.strptime(items[-1]['created'], '%Y-%m-%dT%H:%M:%S.%fZ').date() >= date.today() - timedelta(
                days=self.days) and not self.stoppers[stopper]:
            yield scrapy.Request(url=next_page + str(res_json["nextPage"]), callback=self.parse_globo,
                                 cb_kwargs={"next_page": next_page, 'stopper': stopper})
