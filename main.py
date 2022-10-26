import schedule
import time

from get_twitter_data_local import get_tweets_mongo

from kw_extraction import compute_freq, clean_kw, get_keywords, agg_kw_daily, agg_kw_weekly, agg_kw_monthly, agg_kw_yearly

from sentiment_analysis import load_tweets_mongo, vader_sent, get_sentiment, agg_sentiment_daily, agg_sentiment_weekly, agg_sentiment_monthly, agg_sentiment_yearly 

from datetime import datetime, timedelta


def get_data():

    # test for McDonald's and a time interval of 10 minutes
    #start=(datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d') + 'T23:00:00Z' # yesterday at 23:00:00
    #end=(datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d') + 'T23:30:00Z' # yesterday at 23:10:00

    # start and end times are in UTC ("Z" at the end means that the time is UTC)
    start = (datetime.utcnow()-timedelta(minutes=15, seconds=10)).strftime('%Y-%m-%dT%H:%M:%S') + 'Z'
    end = (datetime.utcnow()-timedelta(seconds=10)).strftime('%Y-%m-%dT%H:%M:%S') + 'Z' # 'end_time' must be a minimum of 10 seconds prior to the request time

    #get_tweets_mongo(company="McDonald's", start_time=start, end_time=end)
    print(f"Data from Twitter successfully stored in MongoDB at {end}.")


def analyse_data():

    yesterday = (datetime.utcnow()-timedelta(days=1)).strftime('%Y-%m-%d')

    #get_keywords(clean_kw())
    print(f"Most relevant keywords successfully extracted for data extracted on {yesterday}.")

    #get_sentiment(vader_sent(load_tweets_mongo(yesterday)))
    print(f"Sentiment Analysis successfully performed for data extracted on {yesterday}.")


def aggregate_data_daily():

    yesterday = (datetime.utcnow()-timedelta(days=1)).strftime('%Y-%m-%d')
    
    #agg_kw_daily(yesterday)
    print(f"Keyword extraction results successfully aggregated for {yesterday}.")

    #agg_sentiment_daily(yesterday)
    print(f"Sentiment Analysis results successfully aggregated for {yesterday}.")


def aggregate_data_weekly():

    # week = 00, 01, …, 53
    # see https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    previous_week = (datetime.utcnow()-timedelta(weeks=1)).strftime('%U')

    #agg_kw_weekly(previous_week)
    print(f"Keyword extraction results successfully aggregated for week {previous_week}.")

    #agg_sentiment_weekly(previous_week)
    print(f"Sentiment Analysis results successfully aggregated for week {previous_week}.")


def aggregate_data_monthly():

    current_month = datetime.utcnow().strftime('%m')
    if current_month == "01":
        previous_month = "12"
    else:
        previous_month = str(int(datetime.utcnow().strftime('%m')) - 1)

    current_year = datetime.utcnow().strftime('%Y')

    #agg_kw_monthly(previous_month, current_year)
    print(f"Keyword extraction results successfully aggregated for month {previous_month}.")

    #agg_sentiment_monthly(previous_month, current_year)
    print(f"Sentiment Analysis results successfully aggregated for month {previous_month}.")


def aggregate_data_yearly():

    previous_year = str(int(datetime.utcnow().strftime('%Y')) - 1)

    #agg_kw_yearly(previous_year)
    print(f"Keyword extraction results successfully aggregated for {previous_year}.")

    #agg_sentiment_yearly(previous_year)
    print(f"Sentiment Analysis results successfully aggregated for {previous_year}.")


def main():

    # times are in local time
    # single run for testing purposes
    schedule.every().day.at("16:10:00").until("23:00:00").do(get_data)
    schedule.every().day.at("16:11:00").until("23:00:00").do(analyse_data)
    schedule.every().day.at("16:12:00").until("23:00:00").do(aggregate_data_daily)
    schedule.every().day.at("16:13:00").until("23:00:00").do(aggregate_data_weekly)
    schedule.every().day.at("16:14:00").until("23:00:00").do(aggregate_data_monthly)
    schedule.every().day.at("16:15:00").until("23:00:00").do(aggregate_data_yearly)

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
        if not schedule.jobs:
            break
        time.sleep(1)

    # for longer periods of time, we should use a different method
    #aggregate_data_monthly()
    #aggregate_data_yearly()


if __name__ == '__main__':

    main()

    print('Success!')