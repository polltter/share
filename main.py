import schedule
import time

import subprocess
from datetime import datetime, timedelta



def get_data():
    log("get_data")
    


def analyse_data():

    yesterday = (datetime.utcnow()-timedelta(days=1)).strftime('%Y-%m-%d')

    #get_keywords(clean_kw())
    log(f"Most relevant keywords successfully extracted for data extracted on {yesterday}.")

    #get_sentiment(vader_sent(load_tweets_mongo(yesterday)))
    log(f"Sentiment Analysis successfully performed for data extracted on {yesterday}.")


def aggregate_data_daily():

    yesterday = (datetime.utcnow()-timedelta(days=1)).strftime('%Y-%m-%d')
    
    #agg_kw_daily(yesterday)
    log(f"Keyword extraction results successfully aggregated for {yesterday}.")

    #agg_sentiment_daily(yesterday)
    log(f"Sentiment Analysis results successfully aggregated for {yesterday}.")


def aggregate_data_weekly():

    # week = 00, 01, …, 53
    # see https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    previous_week = (datetime.utcnow()-timedelta(weeks=1)).strftime('%U')

    #agg_kw_weekly(previous_week)
    log(f"Keyword extraction results successfully aggregated for week {previous_week}.")

    #agg_sentiment_weekly(previous_week)
    log(f"Sentiment Analysis results successfully aggregated for week {previous_week}.")


def aggregate_data_monthly():

    current_month = datetime.utcnow().strftime('%m')
    current_year = datetime.utcnow().strftime('%Y')

    if current_month == "01":
        previous_month = "12"
        year = str(int(current_year) - 1)
    else:
        previous_month = str(int(current_month) - 1)
        year = current_year

    #agg_kw_monthly(previous_month, year)
    log(f"Keyword extraction results successfully aggregated for month {previous_month}.")

    #agg_sentiment_monthly(previous_month, year)
    log(f"Sentiment Analysis results successfully aggregated for month {previous_month}.")


def aggregate_data_yearly():

    previous_year = str(int(datetime.utcnow().strftime('%Y')) - 1)

    #agg_kw_yearly(previous_year)
    log(f"Keyword extraction results successfully aggregated for {previous_year}.")

    #agg_sentiment_yearly(previous_year)
    log(f"Sentiment Analysis results successfully aggregated for {previous_year}.")

def execute_scrapy(scrapy_name: str, path: str):
    result = subprocess.run(scrapy_name, shell=True, capture_output=False, text=False, cwd=path)
    if (result.returncode == 0):
        log("scrapy: {} executed successfully".format(scrapy_name))
    else:
        log("scrapy: {} execution failure status code: {}".format(scrapy_name, result.returncode))

def log(text: str):
    with open("data/log.txt", 'a') as file:
        file.write("[{}] {}\n".format(datetime.today(), text))

def main():
    log("running reputation analysis")
    # times are in local time
    # single run for testing purposes
    # schedule.every().day.at("15:10:00").until("23:00:00").do(get_data)
    # schedule.every().day.at("15:12:00").until("23:00:00").do(get_data)
    # schedule.every().day.at("15:18:00").until("23:00:00").do(get_data)
    # schedule.every().day.at("15:18:00").until("23:00:00").do(get_data)
    # schedule.every().day.at("16:11:00").until("23:00:00").do(analyse_data)
    # schedule.every().day.at("16:12:00").until("23:00:00").do(aggregate_data_daily)
    # schedule.every().day.at("16:13:00").until("23:00:00").do(aggregate_data_weekly)
    # schedule.every().day.at("16:14:00").until("23:00:00").do(aggregate_data_monthly)
    # schedule.every().day.at("16:15:00").until("23:00:00").do(aggregate_data_yearly)

    # runs every 6 hours
    #schedule.every().day.at("23:00:00").do(get_data)
    #schedule.every().day.at("05:00:00").do(get_data)
    #schedule.every().day.at("11:00:00").do(get_data)
    #schedule.every().day.at("17:00:00").do(get_data)

    # runs every day at 1 AM
    #schedule.every().day.at("01:00:00").do(analyse_data)

    # runs every day at 2 AM
    #schedule.every().day.at("02:00:00").do(aggregate_data_daily)

    # runs every Sunday at 3 AM
    #schedule.every().sunday.at("03:00:00").do(aggregate_data_weekly)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
   
    main()

   
