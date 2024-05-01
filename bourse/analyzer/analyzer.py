import pandas as pd
import requests
import numpy as np
import tarfile
import os
import dateutil
import glob
import gc
import logging

import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')  # inside docker


# Configure logging
logging.basicConfig(level=logging.DEBUG,  # Set minimum log level to DEBUG
                    format='%(asctime)s - %(levelname)s - %(message)s',  # Include timestamp, log level, and message
                    handlers=[
                        logging.FileHandler("debug.log"),  # Log to a file
                        logging.StreamHandler()  # Log to standard output (console)
                    ])


def load_pickle(file,market):
    key = dateutil.parser.parse((file.split(market)[1].split('.'))[0])
    df = pd.read_pickle(file)
    return key, df

def files_to_handle(market):
        # Build a list of all files across years
    years = ['2019', '2020', '2021', '2022', '2023']
    files = []
    for year in years:
        files.extend(glob.glob(f'/home/bourse/data/boursorama/{year}/{market}*'))

    # Sort files by date assuming file names contain date information right after market
    files.sort(key=lambda x: dateutil.parser.parse(x.split(market)[1].split('.')[0]))
def create_super_data_frame(market):
    files_2019 = glob.glob('/home/bourse/data/boursorama/' + '2019/' + market + '*')
    files_2020 = glob.glob('/home/bourse/data/boursorama/' + '2020/' + market + '*')
    files_2021 = glob.glob('/home/bourse/data/boursorama/' + '2021/' + market + '*')
    files_2022 = glob.glob('/home/bourse/data/boursorama/' + '2022/' + market + '*')
    files_2023 = glob.glob('/home/bourse/data/boursorama/' + '2023/' + market + '*')
    files = files_2019 + files_2020 + files_2021 + files_2022 + files_2023
    market_df = pd.concat({dateutil.parser.parse((f.split(market)[1].split('.'))[0]): pd.read_pickle(f) for f in files})
    market_df['volume'] = market_df['volume'].astype('int32')
    return market_df

def is_pea(company_symbol, pea_symbols):
    return company_symbol in pea_symbols

def rename_companies(df):
    df.rename(columns={'symbol': 'symbol_column'}, inplace=True)
    df['name'] = df.groupby('symbol_column')['name'].transform('last')
    return df

def symbol_to_id(symbol):
    if symbol.startswith('FF11_'):
        return 10
    elif symbol.startswith('1rA'):
        return 6
    return 11

def to_company_format(df):
    company_df = df[['symbol_column', 'name']]
    company_df['mid'] = df['symbol_column'].apply(symbol_to_id)
    company_df['mid'] = company_df['mid'].astype('Int32')
    company_df.reset_index(drop=True, inplace=True)
    company_df = company_df.drop_duplicates(subset=['symbol_column'], keep='last')

    company_df.rename(columns={'symbol_column': 'symbol'}, inplace=True)
    return company_df

def create_companies_df(renamed_df):
    pea_symbols = renamed_df[0]['symbol_column'].values

    companies_format = [to_company_format(df) for df in renamed_df]
    logging.info("Concatenating companies")
    companies_df = pd.concat(companies_format)
    companies_df.reset_index(drop=True, inplace=True)
    logging.info("Adding pea column")
    companies_df['pea'] = companies_df['symbol'].apply(lambda symbol: is_pea(symbol, pea_symbols))
    companies_df.dropna(inplace=True)

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
    df_day_stock = df_day_stock.merge(symbols, left_on='symbol', right_on='symbol', how='left')
    df_day_stock.rename(columns={'level_1': 'date'}, inplace=True)
    return df_day_stock[['date', 'cid', 'open', 'close', 'high', 'low', 'volume']]


def to_stock_format(df, symbols):
  df = df.copy()
  df['date'] = df.index.map(lambda date_symbol_tuple: date_symbol_tuple[0])
  df.reset_index(drop=True, inplace=True)
  df = df.merge(symbols, left_on='symbol_column', right_on='symbol', how='left')
  df['value'] = df['last'].apply(format_last)
  return df[['date', 'cid', 'value', 'volume']]


if __name__ == '__main__':
    path = "/home/bourse/data/boursorama.tar"
    if os.path.exists(path):
        logging.info("File already exists")
    else:
        url = 'https://www.lrde.epita.fr/~ricou/pybd/projet/boursorama.tar'
        stream = requests.get(url, stream=True)
        with open('/home/bourse/data/boursorama.tar', 'wb') as f:
            for chunk in stream.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    dir = "/home/bourse/data/boursorama"
    if os.path.exists(dir):
        logging.info("Directory already exists")
    else:
        tar = tarfile.open(path)
        tar.extractall('/home/bourse/data')
        tar.close()

    logging.info("Creating super data frame")
    markets = ["peapme"] 
    all_df = [create_super_data_frame(market) for market in markets]
    # all_df = [create_super_data_frame_threading(market) for market in markets]

    logging.info("Renaming companies")
    renamed_df = [rename_companies(df) for df in all_df]


    logging.info("Creating company data frame")
    companies_df = create_companies_df(renamed_df)

    logging.info("Inserting companies on DB")
    db.df_write(df=companies_df, table='companies', index=False)

    logging.info("Creating stocks data frame")
    companies_df['cid'] = companies_df['symbol'].apply(lambda name : db.search_company_id(name))
    small_company_df = companies_df[['symbol', 'cid']]
    small_company_df['cid'] = small_company_df['cid'].astype('Int32')
    logging.info(f"small_company_df size: {len(small_company_df)}")
    del companies_df
    gc.collect()

    stocks_df = pd.concat([to_stock_format(df, small_company_df) for df in renamed_df])
    logging.info("Inserting stocks on db")
    db.df_write(df=stocks_df, table='stocks', index=False)
    del stocks_df
    gc.collect()


    logging.info("Creating day stocks data frame")
    day_stocks_df = pd.concat([day_stock(df, small_company_df) for df in renamed_df])
    del renamed_df
    gc.collect()

    logging.info("Inserting daystocks on db")
    db.df_write(df=day_stocks_df, table='daystocks', index=False)