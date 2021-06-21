import time

import pandas
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from tqdm import tqdm
from urllib.parse import urlparse

today = datetime.today().strftime("%Y-%m-%d")


def get_html(ticker):
    keep_trying = True
    while keep_trying:
        headers = {'User-Agent': 'Mozilla/5.0'}
        url = "https://finviz.com/quote.ashx?t={}".format(ticker.lower())
        time.sleep(0.5)
        req = requests.get(url, headers=headers)
        if req.ok:
            keep_trying = False
    return req.content


def get_news(ticker):
    content = get_html(ticker)
    html = BeautifulSoup(content, 'html.parser')
    news_table = html.find(id='news-table')
    prev_date = None
    parsed_data = []
    for row in news_table.findAll('tr'):
        data = {}
        data["ticker"] = ticker
        data["title"] = row.a.text
        data["src"] = urlparse(row.a['href']).netloc
        data["url"] = row.a["href"]
        date_data = row.td.text.strip().split(' ')
        if len(date_data) == 1:
            founddate = prev_date
            # foundtime = date_data[0]
        else:
            founddate = datetime.strptime(date_data[0], '%b-%d-%y').strftime("%Y-%m-%d")
            if not founddate == today:
                break
            prev_date = founddate
            # foundtime = date_data[1]
        data["date"] = founddate
        # data["foundtime"] = foundtime
        parsed_data.append(data)
    if parsed_data:
        df = pd.DataFrame(parsed_data)
        return df
    raise ValueError("no news articles for {} on {}".format(ticker, today))


if __name__ == '__main__':
    tests = ["AMC", "GME", "TLRY", "CRSR", "BB"]
    news_lst = []
    for ticker in tqdm(tests):
        try:
            data = get_news(ticker)
            news_lst.append(data)
        except Exception as e:
            print(e)
    news_df = pd.concat(news_lst)
    news_df.drop_duplicates(inplace=True)
    news_df.to_csv("data/news_df-{}.csv".format(today), index=False)