import time

import pandas as pd
from datetime import datetime
import pandas as pd
from tqdm import tqdm
import tweepy as tw
from config import get_creds


creds = get_creds()
auth = tw.OAuthHandler(creds["twitter_consumer_key"], creds["twitter_consumer_secret"])
auth.set_access_token(creds["twitter_access_token"], creds["twitter_access_token_secret"])
api = tw.API(auth, wait_on_rate_limit=True)

today = datetime.today().strftime("%Y-%m-%d")


def limit_handled(cursor):
    while True:
        try:
            yield cursor.next()
        except tw.RateLimitError:
            print("hit limit, go to sleep")
            time.sleep(15 * 60)


def tweets_by_handle(handle):
    parsed_data = []
    replies = limit_handled(tw.Cursor(api.search, q='to:{}'.format(handle), since=today).items())
    for tweet in limit_handled(tw.Cursor(api.user_timeline, id=handle, since=today, tweet_mode="extended").items()):
        if not tweet.created_at.strftime("%Y-%m-%d") == today:
            break
        data = {}
        data["handle"] = handle
        data["tweet"] = tweet.full_text
        data["date"] = tweet.created_at.strftime("%Y-%m-%d")
        data["hashtags"] = []
        for hashtag in tweet.entities.get('hashtags'):
            data["hashtags"].append(hashtag["text"])
        data["mentions"] = []
        for mention in tweet.entities.get('user_mentions'):
            data["mentions"].append(mention["screen_name"])
        data["retweets"] = tweet.retweet_count
        data["favourites"] = tweet.favorite_count
        tweet_id = tweet.id
        reply_cnt = 0
        try:
            for reply in replies:
                if not reply.created_at.strftime("%Y-%m-%d") == today:
                    break
                if reply.in_reply_to_status_id == tweet_id:
                    reply_cnt += 1
        except:
            pass
        data["replies"] = reply_cnt
        data["url"] = "https://twitter.com/{}/status/{}".format(data["handle"], tweet_id)
        parsed_data.append(data)
    if parsed_data:
        df = pd.DataFrame(parsed_data)
        return df
    raise ValueError("no tweets from {} on {}".format(handle, today))


def tweets_by_keyword(keyword):
    parsed_data = []
    tweets = limit_handled(tw.Cursor(api.search, q=keyword, lang="en", since=today, tweet_mode="extended").items())
    for tweet in tweets:
        if not tweet.created_at.strftime("%Y-%m-%d") == today:
            break
        data = {}
        data["handle"] = tweet.user.name
        data["tweet"] = tweet.full_text
        data["date"] = tweet.created_at.strftime("%Y-%m-%d")
        data["hashtags"] = []
        for hashtag in tweet.entities.get('hashtags'):
            data["hashtags"].append(hashtag["text"])
        data["mentions"] = []
        for mention in tweet.entities.get('user_mentions'):
            data["mentions"].append(mention["screen_name"])
        data["retweets"] = tweet.retweet_count
        data["favourites"] = tweet.favorite_count
        tweet_id = tweet.id
        replies = 0
        try:
            for reply in limit_handled(tw.Cursor(api.search, q='to:{}'.format(handle), since_id=tweet_id).items()):
                if reply.in_reply_to_status_id == tweet_id:
                    replies += 1
        except:
            pass
        data["replies"] = replies
        data["url"] = "https://twitter.com/{}/status/{}".format(data["handle"], tweet_id)
        parsed_data.append(data)
    if parsed_data:
        df = pd.DataFrame(parsed_data)
        return df
    raise ValueError("no tweets on {} on {}".format(keyword, today))

if __name__ == '__main__':
    handles = ["IBDinvestors", "bespokeinvest", "MarketWatch", "steve_hanke",
               "SJosephBurns", "hmeisler", "KimbleCharting", "allstarcharts", "dKellerCMT",
               "Benzinga", "Stocktwits", "BreakoutStocks", "the_real_fly", "WSJmarkets",
               "Stephanie_Link", "nytimesbusiness", "WSJDealJournal", "PeterSchiff", "markflowchatter",
               "RedDogT3", "tradertvneal", "tradertvshawn", "tradertvbrendan", "RANsquawk", "CNBC"]
    tweet_lst = []
    # data = tweets_by_keyword("$GME")
    # tweet_lst.append(data)
    for handle in tqdm(handles):
        try:
            data = tweets_by_handle(handle)
            tweet_lst.append(data)
        except Exception as e:
            print(e)

    tweet_df = pd.concat(tweet_lst)
    #tweet_df.drop_duplicates(inplace=True)
    tweet_df.to_csv("data/tweet_df-{}.csv".format(today), index=False)
    print("done")