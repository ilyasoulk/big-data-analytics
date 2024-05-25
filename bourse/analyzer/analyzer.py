from operator import index
import pandas as pd
import requests
import numpy as np
import tarfile
import os
import dateutil
import glob
from collections import defaultdict
import gc
import logging
import time
import concurrent.futures
from functools import partial



import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')  # inside docker


# Configure logging
logging.basicConfig(level=logging.DEBUG,  # Set minimum log level to DEBUG
                    format='%(asctime)s - %(levelname)s - %(message)s',  # Include timestamp, log level, and message
                    handlers=[
                        logging.FileHandler("debug.log"),  # Log to a file
                        logging.StreamHandler()  # Log to standard output (console)
                    ])


def get_file_batches():
    # Build a list of all files across years
    years = ['2019', '2020', '2021', '2022', '2023']
    markets = ['compA', 'compB', 'peapme', 'amsterdam']
    files = []
    for year in years:
        for market in markets:
            files.extend(glob.glob(f'/home/bourse/data/boursorama/{year}/{market}*'))

    # Create a dictionary to hold files by date
    files_by_date = defaultdict(list)

    # Group files by date
    for file in files:
        # Extract the market prefix and the date part from the file name
        path_parts = file.split('/')
        filename = path_parts[-1]
        for market in markets:
            if market in filename:
                # Attempt to extract the date part directly following the market name
                start = len(market)
                date_part = filename[start:].split('.')[0]
                try:
                    date = dateutil.parser.parse(date_part).date()  # parse and keep only the date part
                    files_by_date[date].append(file)
                except ValueError:
                    logging.warning(f"Could not parse date from file name: {filename}")
                break

    file_batches = list(files_by_date.values())
    
    return file_batches


def create_dataframe_from_batch(batch):
    markets = ['compA', 'compB', 'peapme', 'amsterdam']
    data_frames = defaultdict(list)

    i = 0
    batch_size = len(batch)

    for file in batch:
        for market in markets:
            if market in file:
                try:
                    date_part = file.split(market)[1].split('.')[0]
                    date = dateutil.parser.parse(date_part)
                    df = pd.read_pickle(file)
                    if market == 'amsterdam':
                        df['mid'] = 1
                    data_frames[date].append(df)
                except ValueError as e:
                    logging.warning(f"Parsing file failed : {e}")
                break

    market_df = pd.concat({pd.to_datetime(date): pd.concat(dfs) for date, dfs in data_frames.items()})

    return market_df


def create_superdf_companies(market):
    # Build a list of all files across years for the specified market
    years = ['2019', '2020', '2021', '2022', '2023']
    files = []
    for year in years:
        files.extend(glob.glob(f'data/boursorama/{year}/{market}*'))

    # Create a dictionary to hold files by date
    files_by_date = defaultdict(list)

    # Group files by date
    for file in files:
        # Extract the date part from the file name
        filename = file.split('/')[-1]
        date_part = filename[len(market):].split('.')[0]
        try:
            date = dateutil.parser.parse(date_part).date()  # parse and keep only the date part
            files_by_date[date].append(file)
        except ValueError:
            logging.warning(f"Could not parse date from file name: {filename}")

    # Prepare to read only the first and last files per day
    market_dfs = []
    for date, date_files in files_by_date.items():
        if date_files:
            # Sort files to ensure they are in the correct order
            sorted_files = sorted(date_files)
            selected_files = [sorted_files[0], sorted_files[-1]] if len(sorted_files) > 1 else [sorted_files[0]]

            # Read the first and last file and extract necessary columns
            for file in selected_files:
                df = pd.read_pickle(file)[['symbol', 'name']]
                df['pea'] = (market == 'peapme')
                market_dfs.append(df)

    # Concatenate all DataFrames into a single DataFrame
    if market_dfs:
        complete_df = pd.concat(market_dfs, ignore_index=True)
        complete_df.drop_duplicates(subset='symbol', keep='last', inplace=True)
        return complete_df

    return pd.DataFrame()  # Return an empty DataFrame if no files were processed


def symbol_to_id(symbol):
    if symbol.startswith('FF11_'):
        return 10
    elif symbol.startswith('1rA'):
        return 6
    return 11

def rename_companies(df):
    df['pea'] = df.groupby('symbol')['pea'].transform(any)
    df['name'] = df.groupby('symbol')['name'].transform('last')
    if 'mid' not in df.columns:
        df['mid'] = df['symbol'].apply(symbol_to_id)
    df['mid'] = df['mid'].astype('Int8')
    df.reset_index(drop=True, inplace=True)
    df = df.drop_duplicates(subset=['symbol'], keep='last')
    df.dropna(inplace=True)
    return df


def format_last(x):
    try:
        return np.float32(x)
    except:        
        processed_value = x.split('(')[0].replace(' ', '')
        return np.float32(processed_value)  # Split by ( to get rid of the (s) (c) then remove whitespace

def day_stock(df):
    df['last'] = df['last'].apply(format_last)
    grouped = df.groupby([pd.Grouper(level='symbol'), pd.Grouper(level=0, freq='D')])
    df_day_stock = grouped.agg(open=('last', 'first'), high=('last', 'max'), low=('last', 'min'), close=('last', 'last'), volume=('volume', 'sum'))
    df_day_stock.reset_index(inplace=True)
    df_day_stock.rename(columns={'level_1': 'date'}, inplace=True)


    df_no_volume = df_day_stock[df_day_stock['volume'] > 0]
    del df_day_stock
    gc.collect()

    df_no_volume['cid'] = df_no_volume['symbol'].apply(db.search_company_id)
    return df_no_volume[['date', 'cid', 'open', 'close', 'high', 'low', 'volume']]

def to_stock_format(df):
    df['date'] = df.index.map(lambda date_symbol_tuple: date_symbol_tuple[0])
    df.reset_index(drop=True, inplace=True)
    df['value'] = df['last'].apply(format_last)



    df_no_volume = df[df['volume'] > 0]
    del df
    gc.collect()


    return df_no_volume[['date', 'cid', 'value', 'volume']]


def is_company_in_db(symbol):
    return db.search_company_id(symbol) != 0


def process_data(batch, companies):
    start_batch = time.time()
    date = batch[0].split(' ')[1]
    logging.info(f"Processing batch : {date}")
    df = create_dataframe_from_batch(batch)
    df['cid'] = df['symbol'].map(companies)
    df['cid'] = df['cid'].astype('Int16')
    tmp_stocks = df.copy()
    stocks_df = to_stock_format(tmp_stocks)
    db.df_write(df=stocks_df, table='stocks', index=False)
    del stocks_df
    del tmp_stocks
    gc.collect()

    day_stocks_df = day_stock(df)
    db.df_write(df=day_stocks_df, table='daystocks', index=False)
    del day_stocks_df
    gc.collect()
    end_batch = time.time()
    logging.info(f"Time taken for a batch : {end_batch - start_batch} seconds")



if __name__ == "__main__":
    start_time = time.time()
    files = ["compA", "compB", "peapme", "amsterdam"]
    logging.info("Creating super dataframe")
    super_df = [create_superdf_companies(file) for file in files]
    renamed_df = [rename_companies(df) for df in super_df]
    full_df = pd.concat(renamed_df)
    full_df.drop_duplicates(subset='symbol', keep='last', inplace=True)
    logging.info(f"Len of companies : {len(full_df)}")
    logging.info("Writing companies to DB")
    db.df_write(df=full_df, table='companies', index=False) 
    end_companies = time.time()
    logging.info(f"Time taken for filling companies : {end_companies - start_time} seconds")
    full_df['cid'] = full_df['symbol'].apply(db.search_company_id)
    companies = full_df[['symbol', 'cid']]
    dict_companies = dict(zip(companies['symbol'], companies['cid']))
    batches = get_file_batches()
    proccess_data_partial = partial(process_data, companies=dict_companies)
    with concurrent.futures.ProcessPoolExecutor(max_workers=16) as executor:
        executor.map(proccess_data_partial, batches)

    end_time = time.time()
    logging.info(f"Time taken for filling the whole DB : {end_time - start_time} seconds")
