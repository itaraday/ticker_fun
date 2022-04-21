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
symbols_lst = get_symbols(dollars=True)


def analysis_comments(comment_df, thread_df):
    comment_df['corpus'] = comment_df['body'].apply(lambda x: cleanup(x))
    comment_df["valid"] = comment_df['corpus'].apply(lambda x: bool(set(x).intersection(set(symbols_lst))))
    comment_df = comment_df[comment_df["valid"]]
    comment_df.reset_index(drop=True, inplace=True)

    vader = reddit_vader()
    scores = comment_df['body'].apply(vader.polarity_scores)
    scores_df = pd.DataFrame.from_records(scores)
    comment_df = comment_df.join(scores_df)

    comment_df["tickers"] = comment_df.apply(lambda x: [], axis=1)
    output = []
    for index, row in tqdm(stock_df.iterrows()):
        common = '?'
        if row['Common']:
            common = ''
        symbols = r"(^|\W)(\${}{})('s)?(\W|$)".format(common, row['Symbol'])
        comment_df[comment_df['body'].str.contains(symbols, case=False, regex=True)].apply(
            lambda x: x["tickers"].append(row["Symbol"]), axis=1)

    # if don't know what the symbol is in a comment use the title
    comment_df["tickers"] = comment_df["tickers"].apply(lambda y: np.nan if len(y) == 0 else y)
    #title_cols = [x for x in thread_df.columns.tolist() if "title" in x]
    title_cols = ['title_tickers', 'title_name_tickers']
    title_df = thread_df.groupby("id").first()[title_cols].reset_index()
    title_df["title_symbols"] = np.NaN
    for col in title_cols:
        title_df[col] = title_df[col].apply(lambda y: np.nan if len(y) == 0 else y)
        title_df["title_symbols"].fillna(title_df[col], inplace=True)
    title_df = title_df[["id", "title_symbols"]]
    comment_df.set_index("thread_id")["tickers"].fillna(title_df.set_index("id")["title_symbols"], inplace=True)
    comment_df["tickers"] = comment_df["tickers"].combine_first(comment_df["thread_id"].map(title_df.set_index("id")["title_symbols"]))

    return comment_df

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
    thread_df["name_tickers"] = thread_df.apply(lambda x: [], axis=1)
    thread_df["title_tickers"] = thread_df.apply(lambda x: [], axis=1)
    thread_df["title_name_tickers"] = thread_df.apply(lambda x: [], axis=1)
    first_sentences = thread_df.groupby('id', as_index=False).nth(0).copy()
    for index, row in tqdm(stock_df.iterrows()):
        try:
            common = '?'
            if row['Common']:
                common = ''
            names = r"(^|\W)({})('s)?(\W|$)".format("|".join(row["Name"]))
            symbols = r"(^|\W)(\${}{})('s)?(\W|$)".format(common, row["Symbol"])
            #title_regex = r"(^|\W)(\$?{}|{})('s)?(\W|$)".format(row["Symbol"], "|".join(row["Name"]))
            #regexPattern = r"(^|\$|\b)({}|{})('s)?(\b|$)".format(row['Symbol'], "|".join(row["Name"]))
            thread_df[thread_df['sentences'].str.contains(symbols, case=False, regex=True)].apply(
                lambda x: x["tickers"].append(row["Symbol"]), axis=1)
            thread_df[thread_df['sentences'].str.contains(names, case=False, regex=True)].apply(
                lambda x: x["name_tickers"].append(row["Symbol"]), axis=1)
            first_sentences[first_sentences['title'].str.contains(symbols, case=False, regex=True)].apply(
                lambda x: x["title_tickers"].append(row["Symbol"]), axis=1)
            first_sentences[first_sentences['title'].str.contains(names, case=False, regex=True)].apply(
                lambda x: x["title_name_tickers"].append(row["Symbol"]), axis=1)
        except Exception as e:
            print(e)
    thread_df['tickers'] = thread_df['tickers'].apply(lambda x: list(set(x)))

    thread_df.loc[thread_df["tickers"].map(lambda d: len(d)) == 0, "tickers"] = np.NaN
    #thread_df["tickers"] = thread_df.groupby(['id'], as_index=False)["tickers"].ffill()
    return thread_df


if __name__ == '__main__':
    #tests = ["AMC", "GME", "TLRY", "CRSR", "BB", "HUT"]
    #stock_df = stock_df[stock_df["Symbol"].isin(tests)]
    #comment_df = pd.read_csv("data/comment_df-{}.csv".format(today))
    #thread_df = pd.read_csv("data/thread_df-{}.csv".format(today))
    comment_df = pd.read_csv("data/comment_df-2021-06-16.csv".format(today))
    thread_df = pd.read_csv("data/thread_df-2021-06-16.csv".format(today))
    thread_df = thread_df[thread_df["id"].isin(["o0pngx", "o0qakb"])]
    comment_df = comment_df[comment_df["thread_id"].isin(["o0pngx", "o0qakb"])]
    thread_df.reset_index(drop=True, inplace=True)


    thread_df = analysis_thread(thread_df)
    comment_df = analysis_comments(comment_df, thread_df)
    thread_df.to_csv("data/thread_df_dev2.csv", index=False)
    comment_df.to_csv("data/comment_df_dev2.csv", index=False)
    #ticker_df.to_csv("data/ticker_df_dev2.csv", index=False)
    exit(1)
