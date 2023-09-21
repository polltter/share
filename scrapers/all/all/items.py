# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AllItem(scrapy.Item):
    raw_title = scrapy.Field()
    raw_text = scrapy.Field()
    date_news = scrapy.Field()
    date_scrap = scrapy.Field()
    url = scrapy.Field()
    lang = scrapy.Field()
