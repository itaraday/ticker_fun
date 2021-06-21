import re
import pandas as pd
import numpy as np
from datetime import datetime
from cleanup import cleanup
from create_stockdf import get_stock_df, get_symbols
from vader import reddit_vader
from cleanup import remove_urls, make_sentences_reddit
from tqdm import tqdm

today = datetime.today().strftime("%Y-%m-%d")
stock_df = get_stock_df()
symbols = get_symbols(dollars=True)


def analysis_comments(comment_df):
    return


def analysis_thread(thread_df):
    vader = reddit_vader()
    cols = thread_df.columns
    thread_df["sentences"] = thread_df.apply(lambda x: make_sentences_reddit(remove_urls(x["body"])), axis=1)
    thread_df = thread_df["sentences"].apply(lambda x: pd.Series(x))\
        .stack()\
        .reset_index(level=1, drop=True)\
        .to_frame('sentences')\
        .join(thread_df[cols], how='left')
    thread_df['rating'] = thread_df['sentences'].apply(vader.polarity_scores)
    thread_df = pd.concat([thread_df.drop(['rating'], axis=1), thread_df['rating'].apply(pd.Series)], axis=1)
    thread_df["tickers"] = thread_df.apply(lambda x: [], axis=1)
    first_sentences = thread_df.groupby('id', as_index=False).nth(0).copy()
    for index, row in tqdm(stock_df.iterrows()):
        regexPattern = r"(^|\$|\b)({}|{})('s)?(\b|$)".format(row['Symbol'], "|".join(row["Name"]))
        thread_df[thread_df['sentences'].str.contains(regexPattern, case=True, regex=True)].apply(lambda x: x["tickers"].append(row["Symbol"]), axis=1)
        first_sentences[first_sentences['title'].str.contains( , case=False, regex=True)].apply(lambda x: x["tickers"].append(row["Symbol"]), axis=1)
    thread_df['tickers'] = thread_df['tickers'].apply(lambda x: list(set(x)))

    thread_df.loc[thread_df["tickers"].map(lambda d: len(d)) == 0, "tickers"] = np.NaN
    thread_df = thread_df.groupby(['id'], as_index=False).apply(lambda group: group.ffill())
    thread_df.to_csv("foo.csv", index=False)
    return


if __name__ == '__main__':
    #tests = ["AMC", "GME", "TLRY", "CRSR", "BB", "HUT"]
    #stock_df = stock_df[stock_df["Symbol"].isin(tests)]
    #comment_df = pd.read_csv("data/comment_df-{}.csv".format(today))
    #thread_df = pd.read_csv("data/thread_df-{}.csv".format(today))
    comment_df = pd.read_csv("data/comment_df-2021-06-16.csv".format(today))
    thread_df = pd.read_csv("data/thread_df-2021-06-16.csv".format(today))
    thread_df = thread_df[thread_df["id"].isin(["o0pngx", "o0qakb"])]
    analysis_thread(thread_df)
    exit(1)
    comment_df['corpus'] = comment_df['body'].apply(lambda x: cleanup(x))
    comment_df["valid"] = comment_df['corpus'].apply(lambda x: bool(set(x) - set(symbols)))
    comment_df = comment_df[comment_df["valid"]]

    for index, row in stock_df.iterrows():
        regexPattern = r"(^|\$|\b)({}|{})('s)?(\b|$)".format(row['Symbol'], "|".join(row["Name"]))
        df = comment_df[comment_df['body'].str.contains(regexPattern, case=False, regex=True)]

        if not df.empty:
            ticker_count = df["body"].str.count(regexPattern, flags=re.IGNORECASE).sum()
            comment_count = len(df.index)
            print("{}: Commewnts = {} Mentions = {}".format(row['Symbol'], comment_count, ticker_count))
