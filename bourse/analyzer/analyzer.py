import pandas as pd
import requests
import numpy as np
import sklearn
import tarfile
import os
import dateutil
import glob
import gc

import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')  # inside docker


# def store_file(name, website):
#     if db.is_file_done(name):
#         return
#     if website.lower() == "boursorama":
#         try:
#             df = pd.read_pickle("bourse/data/boursorama/" + name)  # is this dir ok for you ?
#         except:
#             year = name.split()[1].split("-")[0]
#             df = pd.read_pickle("bourse/data/boursorama/" + year + "/" + name)
#         # to be finished


def create_super_data_frame(market):
    years = ['2019', '2020', '2021', '2022', '2023']
    files = [glob.glob(f'/home/bourse/data/boursorama/{year}/{market}*') for year in years]
    all_files = [item for sublist in files for item in sublist]  # Flatten the list

    market_df = pd.DataFrame()
    for file in all_files:
        temp_df = pd.read_pickle(file)
        temp_df['volume'] = temp_df['volume'].astype('int32')
        market_df = pd.concat([market_df, temp_df])
        del temp_df
        gc.collect()

    market_df.sort_index(inplace=True)
    return market_df


def is_pea(company_symbol, pea_symbols):
    return company_symbol in pea_symbols

def rename_companies(df):
    df.rename(columns={'symbol': 'symbol_column'}, inplace=True)
    df['name'] = df.groupby('symbol_column')['name'].transform('last')
    return df

def to_company_format(df):
    company_df = df[['symbol_column', 'name']]
    company_df.reset_index(drop=True, inplace=True)
    company_df = company_df.drop_duplicates(subset=['symbol_column'], keep='last')
    company_df.rename(columns={'symbol_column': 'symbol'}, inplace=True)
    return company_df

def create_companies_df(renamed_df):
    pea_symbols = renamed_df[0]['symbol_column'].values

    companies_format= [to_company_format(df) for df in renamed_df]
    companies_df = pd.concat(companies_format)
    companies_df.reset_index(drop=True, inplace=True)
    companies_df['pea'] = companies_df['symbol'].apply(lambda symbol: is_pea(symbol, pea_symbols))
    return companies_df


def format_last(x):
    try:
        return np.float32(x)
    except:        
        processed_value = x.split('(')[0].replace(' ', '')
        return np.float32(processed_value)  # Split by ( to get rid of the (s) (c) then remove whitespace


def day_stock(df, symbols):
    df['last'] = df['last'].apply(format_last)
    grouped = df.groupby([pd.Grouper(level='symbol'), pd.Grouper(level=0, freq='D')])
    df_day_stock = grouped.agg(open=('last', 'first'), high=('last', 'max'), low=('last', 'min'), close=('last', 'last'), volume=('volume', 'sum'))
    df_day_stock.reset_index(inplace=True)
    df_day_stock['cid'] = df_day_stock['symbol'].apply(lambda symbol: np.where(symbols == symbol)[0][0])
    df_day_stock.rename(columns={'level_1': 'date'}, inplace=True)
    return df_day_stock[['date', 'cid', 'open', 'close', 'high', 'low', 'volume']]


def to_stock_format(df, symbols):
  df = df.copy()
  df['date'] = df.index.map(lambda date_symbol_tuple: date_symbol_tuple[0])
  df.reset_index(drop=True, inplace=True)
  df['cid'] = df['symbol_column'].apply(lambda symbol: np.where(symbols == symbol)[0][0])
  df['value'] = df['last'].apply(format_last)
  return df[['date', 'cid', 'value', 'volume']]


if __name__ == '__main__':
    path = "/home/bourse/data/boursorama.tar"
    if os.path.exists(path):
        print("File already exists")
    else:
        url = 'https://www.lrde.epita.fr/~ricou/pybd/projet/boursorama.tar'
        stream = requests.get(url, stream=True)
        with open('/home/bourse/data/boursorama.tar', 'wb') as f:
            for chunk in stream.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    dir = "/home/bourse/data/boursorama"
    if os.path.exists(dir):
        print("Directory already exists")
    else:
        tar = tarfile.open(path)
        tar.extractall('/home/bourse/data')
        tar.close()

    print("Creating super data frame")
    markets = ["compA"]
    all_df = [create_super_data_frame(market) for market in markets]

    print("Renaming companies")
    renamed_df = [rename_companies(df) for df in all_df]

    print("Creating company data frame")
    companies_df = create_companies_df(renamed_df)

    print("Inserting companies on DB")
    db.df_write(df=companies_df, table='companies', index=False)


    print("Creating stocks data frame")
    symbols = companies_df['symbol'].values # to get cid
    del companies_df
    gc.collect()

    stocks_df = pd.concat([to_stock_format(df, symbols) for df in renamed_df])
    print("Inserting stocks on db")
    db.df_write(df=stocks_df, table='stocks', index=False)
    del stocks_df
    gc.collect()


    print("Creating day stocks data frame")
    day_stocks_df = pd.concat([day_stock(df, symbols) for df in renamed_df])
    del renamed_df
    gc.collect()

    print("Inserting daystocks on db")
    db.df_write(df=day_stocks_df, table='daystocks', index=False)
