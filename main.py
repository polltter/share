import schedule
import time

from get_twitter_data_local import get_tweets_mongo

from kw_extraction_local import compute_freq, clean_kw, get_keywords

from sentiment_analysis import load_tweets_mongo, vader_sent, get_sentiment

from datetime import datetime, timedelta


def job():

    # test for McDonald's and a time interval of 10 minutes
    #start=(datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d') + 'T23:00:00Z' # yesterday at 23:00:00
    #end=(datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d') + 'T23:30:00Z' # yesterday at 23:10:00

    # start and end times are in UTC ("Z" at the end means that the time is UTC)
    start = (datetime.utcnow()-timedelta(minutes=15, seconds=10)).strftime('%Y-%m-%dT%H:%M:%S') + 'Z'
    end = (datetime.utcnow()-timedelta(seconds=10)).strftime('%Y-%m-%dT%H:%M:%S') + 'Z' # 'end_time' must be a minimum of 10 seconds prior to the request time

    get_tweets_mongo(company="McDonald's", start_time=start, end_time=end)
    print(f"Data from Twitter successfully stored in MongoDB at {end}.")

    #get_keywords(clean_kw())
    #print("Most relevant keywords successfully extracted.")

    #get_sentiment(vader_sent(load_tweets_mongo()))
    #print("Sentiment Analysis successfully performed.")


def main():

    # times are in local time
    # single run for testing purposes
    #schedule.every().day.at("12:46:00").until("23:00:00").do(job)

    # runs every 15 minutes during one hour (similar to running daily for every 6 hours)
    schedule.every().day.at("13:00:00").until("23:00:00").do(job)
    schedule.every().day.at("13:15:00").until("23:00:00").do(job)
    schedule.every().day.at("13:30:00").until("23:00:00").do(job)
    schedule.every().day.at("13:45:00").until("23:00:00").do(job)


    while True:
        schedule.run_pending()
        if not schedule.jobs:
            break
        time.sleep(1)


if __name__ == '__main__':

    main()

    print('Success!')