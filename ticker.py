import re
import pandas as pd
import numpy as np
from datetime import datetime
from tqdm import tqdm
from create_stockdf import get_stock_df, get_symbols


today = datetime.today().strftime("%Y-%m-%d")
stock_df = get_stock_df()
symbols_lst = get_symbols(dollars=True)


def aggregate_data(file_pth, media):
    data_df = pd.read_csv(file_pth)
    output = []
    if media == 'twitter':
        data_df["influence"] = data_df["retweets"]*3 + data_df["favourites"]
    elif media == 'comments':
        data_df["vote"] = data_df["up_cnt"] - data_df["down_cnt"]
        data_df["influence"] = (data_df["author_karma"]%100)+data_df["vote"]
    elif media == 'news':
        data_df["influence"] = 5*(1+data_df["meme"].astype(int))
    for index, row in tqdm(stock_df.iterrows()):
        common = '?'
        if row['Common']:
            common = ''
        symbols = r"(^|\W)(\${}{})('s)?(\W|$)".format(common, row['Symbol'])

        df = data_df[data_df['body'].str.contains(symbols, case=False, regex=True)]
        if not df.empty:
            data = {}
            data["symbol"] = row['Symbol']
            data["ticker_count"] = df["body"].str.count(symbols, flags=re.IGNORECASE).sum()
            data["count"] = len(df.index)
            data["compound"] = df["compound"].mean()
            data["reach"] = df["reach"].sum()
            output.append(data)
            print("{}: Comments = {} Mentions = {}".format(row['Symbol'], data["comment_count"], data["ticker_count"]))
    ticker_df = pd.DataFrame(output)
    return ticker_df

if __name__ == '__main__':
    foo = aggregate_data("data/comment_df_dev2.csv", 'comments')
    print("done")