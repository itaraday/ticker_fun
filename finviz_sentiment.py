import pandas as pd
from datetime import datetime
from cleanup import cleanup
from vader import finviz_vader

today = datetime.today().strftime("%Y-%m-%d")
vader = finviz_vader()


def finviz_sentiment(news_df):
    news_df["corpus"] = news_df["title"].apply(lambda x: cleanup(x, remove_punct=True))
    news_df["clean"] = news_df["corpus"].apply(lambda x: ' '.join(x))
    scores = news_df['title'].apply(vader.polarity_scores)
    scores_df = pd.DataFrame.from_records(scores)
    news_df = news_df.join(scores_df)

    # todo if meme is true verify that based on reddit/twitter
    news_df["meme"] = False
    news_df.loc[news_df["title"].str.contains("meme", case=False), "meme"] = True
    news_df.to_csv("data/news_df_scored-{}.csv".format(today), index=False)

if __name__ == '__main__':
    #news_df = pd.read_csv("data/news_df-{}.csv".format(today))
    news_df = pd.read_csv("data/news_df-2021-06-16.csv".format(today))
    news_df = news_df.drop_duplicates(subset=["ticker", "title"])
    finviz_sentiment(news_df)
    print("done")