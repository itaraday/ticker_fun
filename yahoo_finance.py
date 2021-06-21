import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm

def get_yesterday_data(symbols):
    today = datetime.now()
    yesterday = today - timedelta(1)
    #if Monday pull Friday data
    if yesterday.weekday() > 4:
        yesterday = yesterday.now() - timedelta(3)

    threads = True
    if isinstance(symbols, str):
        threads=False
    data = yf.download(symbols, yesterday, yesterday, group_by='Ticker', threads=threads)
    if isinstance(symbols, str):
        if data.empty:
            raise ValueError("{} missing data".format(symbol))
        data["Symbol"] = symbol
    else:
        data = data.stack(level=0).rename_axis(['Date', 'Symbol']).reset_index(level=1)
    return data

if __name__ == '__main__':
    # #top 500 tickers
    # tickers = pd.read_html(
    #     'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
    # ticker_lst = tickers["Symbol"].unique().to_list()
    stock_df = pd.read_csv("data/stock_df.csv")
    symbol_lst = stock_df["Symbol"].unique().tolist()
    lst = []
    for symbol in tqdm(symbol_lst):
        try:
            lst.append(get_yesterday_data(symbol))
        except Exception as e:
            pass
    df = pd.concat(lst)
    today = datetime.now().strftime("%Y-%m-%d")
    df.to_csv("data/finance-{}.csv".format(today), index=False)
    stock_df[stock_df["Symbol"].isin(df["Symbol"].tolist())].to_csv("data/stock_df.csv", index=False)