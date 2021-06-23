import pandas as pd
from datetime import datetime
from cleanup import cleanup_twitter
from vader import get_vader
from create_stockdf import get_stock_df, get_symbols
from tqdm import tqdm
import regex as re


today = datetime.today().strftime("%Y-%m-%d")
vader = get_vader()
stock_df = get_stock_df()


def twitter_sentiment(twitter_df):
    # cols = twitter_df.columns
    twitter_df["clean"] = twitter_df.apply(lambda x: cleanup_twitter(x["tweet"]), axis=1)
    # twitter_df = twitter_df["sentences"].apply(lambda x: pd.Series(x)) \
    #     .stack() \
    #     .reset_index(level=1, drop=True) \
    #     .to_frame('sentences') \
    #     .join(twitter_df[cols], how='left')
    twitter_df['rating'] = twitter_df['clean'].apply(vader.polarity_scores)
    twitter_df["tickers"] = twitter_df.apply(lambda x: [], axis=1)
    for index, row in tqdm(stock_df.iterrows()):
        symbols = r"(^|\W)(\${})('s)?(\W|$)".format(row["Symbol"])
        twitter_df[twitter_df['clean'].str.contains(symbols, case=False, regex=True)].apply(
            lambda x: x["tickers"].append(row["Symbol"]), axis=1)
        twitter_df.apply(
            lambda x: x["tickers"].extend(re.findall(r"\b{}\b".format(row["Symbol"]), x["hashtags"], re.IGNORECASE)), axis=1)
        twitter_df.apply(
            lambda x: x["tickers"].extend(re.findall(r"\b{}\b".format(row["Name"]), x["hashtags"], re.IGNORECASE)), axis=1)
    twitter_df["tickers"] = twitter_df["tickers"].apply(lambda x: list(set(x)))
    twitter_df.to_csv("data/twitter_sentiment.csv", index=False)

if __name__ == '__main__':
    #news_df = pd.read_csv("data/news_df-{}.csv".format(today))
    twitter_df = pd.read_csv("data/tweet_df-2021-06-16.csv".format(today))

    urls = ["https://twitter.com/CNBC/status/1405267056358936584", "https://twitter.com/CNBC/status/1405185781719838727",
            "https://twitter.com/CNBC/status/1405230679487512577", "https://twitter.com/CNBC/status/1405250259010338817"]
    twitter_df = twitter_df[twitter_df["url"].isin(urls)]
    twitter_sentiment(twitter_df)
    print("done")