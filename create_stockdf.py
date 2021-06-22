#http://www.eoddata.com/symbols.aspx
import pandas as pd
from cleanco import cleanco

def get_stock_df():
    stock_df = pd.read_csv("data/stock_df.csv")
    stock_df = stock_df.drop_duplicates(subset=["Symbol", "Name"])

    stock_df = stock_df.groupby('Symbol')['Name'].apply(lambda x: list(x)).reset_index()
    return stock_df


def get_symbols(dollars=False):
    # for all symbols check for name or $name
    stock_df = get_stock_df()
    symbols = stock_df['Symbol'].unique().tolist()
    symbols = [x.lower() for x in symbols]
    if dollars:
        symbols = symbols + ["${}".format(x) for x in symbols]
    return symbols


if __name__ == '__main__':
    #todo add OTC?
    # http://eoddata.com/
    data_lst = ["TSX", "NYSE", "NASDAQ", "OTCBB"]
    df_lst = pd.DataFrame
    for data in data_lst:
        df = pd.read_csv('data\{}.txt'.format(data), delimiter="\t")
        df.dropna(inplace=True)
        df["market"] = data
        df_lst.append(df)

    stock_df = pd.concat(df_lst)
    del df_lst
    stock_df['Name'] = stock_df['Description'].str.lower()
    stock_df['Name'] = stock_df['Description'].str.replace('.com','')
    stock_df['Name'] = stock_df['Name'].str.replace('[^\w\s]','')
    stock_df['Name'] = stock_df['Name'].apply(lambda x: cleanco(x).clean_name())
    stock_df['Name'].fillna(stock_df['Description'], inplace=True)
    stock_df['Name'] = stock_df['Name'].str.lower()

    stock_df.loc[stock_df["Symbol"]=="CRSR", "Name"] = 'Corsair'
    stock_df.loc[stock_df["Symbol"] == "LOGI", "Name"] = 'Logitech'
    stock_df.loc[stock_df["Symbol"] == "DIS", "Name"] = 'Disney'
    stock_df.to_csv("data/stock_df2.csv", index=False)
    print("done")