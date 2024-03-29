import pandas as pd
import requests
import numpy as np
import sklearn
import tarfile
import os
import dateutil
import glob

import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')  # inside docker


def store_file(name, website):
    if db.is_file_done(name):
        return
    if website.lower() == "boursorama":
        try:
            df = pd.read_pickle("bourse/data/boursorama/" + name)  # is this dir ok for you ?
        except:
            year = name.split()[1].split("-")[0]
            df = pd.read_pickle("bourse/data/boursorama/" + year + "/" + name)
        # to be finished


def create_super_data_frame(market):
    files_2019 = glob.glob('/home/bourse/data/boursorama/' + '2019/' + market + '*')
    files_2020 = glob.glob('/home/bourse/data/boursorama/' + '2020/' + market + '*')
    files_2021 = glob.glob('/home/bourse/data/boursorama/' + '2021/' + market + '*')
    files_2022 = glob.glob('/home/bourse/data/boursorama/' + '2022/' + market + '*')
    files_2023 = glob.glob('/home/bourse/data/boursorama/' + '2023/' + market + '*')
    files = files_2019 + files_2020 + files_2021 + files_2022 + files_2023
    print(f"Found {len(files)} files for {market}")
    market_df = pd.concat({dateutil.parser.parse((f.split(market)[1].split('.'))[0]): pd.read_pickle(f) for f in files})
    market_df.sort_index(inplace=True)
    return market_df


def total_files():
    files_2019 = glob.glob('/home/bourse/data/boursorama/' + '2019/*')
    files_2020 = glob.glob('/home/bourse/data/boursorama/' + '2020/*')
    files_2021 = glob.glob('/home/bourse/data/boursorama/' + '2021/*')
    files_2022 = glob.glob('/home/bourse/data/boursorama/' + '2022/*')
    files_2023 = glob.glob('/home/bourse/data/boursorama/' + '2023/*')
    files = files_2019 + files_2020 + files_2021 + files_2022 + files_2023
    market_names = [f.split()[0].split('/')[-1] for f in files]
    market_names_unique = np.unique(market_names)
    print(f"Found {len(files)} total files")
    print("Markets found:")
    for m in market_names_unique:
        print(m)
    return len(files)


def rename_companies(df):
    df.rename(columns={'symbol': 'symbol_column'}, inplace=True)
    df['name'] = df.groupby('symbol_column')['name'].transform('last')


def create_company_df(df, pea):
    '''
    Note that this function is still not perfect.
    We can have companies in compA that are PEA PME.
    We should check if the company is in the list of companies in the pea pme dataframe.
    '''
    df = df.copy()
    df['pea'] = pea
    company_df = df[['symbol_column', 'name', 'pea']]
    company_df.reset_index(drop=True, inplace=True)
    company_df = company_df.drop_duplicates(subset=['symbol_column'], keep='last')
    company_df.rename(columns={'symbol_column': 'symbol'}, inplace=True)
    return company_df


def format_last(x):
    try:
        return float(x)
    except:
        return float(x.split('(')[0].replace(' ', ''))  # Split by ( to get rid of the (s) (c) then remove whitespace


def day_stock(df):
    df = df.drop(columns=['symbol_column'])
    df['last'] = df['last'].apply(format_last)
    df = df.swap_level(0, 1).sort_index()
    grouped = df.groupby([pd.Grouper(level='symbol'), pd.Grouper(level=1, freq='D')])
    df_day_stock = grouped.agg(open=('last', 'first'), high=('last', 'max'), low=('last', 'min'),
                               close=('last', 'last'), volume=('volume', 'sum'))
    return df_day_stock


def is_pea(company_symbol, df_peapme):
    return company_symbol in df_peapme['symbol'].values


if __name__ == '__main__':
    # store_file("compA 2020-01-01 09:02:02.532411", "boursorama")
    # store_file("compB 2020-01-01 09:02:02.532411", "boursorama")
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
    df_peapme = create_super_data_frame("peapme")
    df_comp_a = create_super_data_frame("compA")
    df_comp_b = create_super_data_frame("compB")
    df_amsterdam = create_super_data_frame("amsterdam")
    print("Renaming companies")
    rename_companies(df_peapme)
    rename_companies(df_comp_a)
    rename_companies(df_comp_b)
    rename_companies(df_amsterdam)
    print("Creating company data frame")
    companies_peapme = create_company_df(df_peapme, True)
    companies_comp_a = create_company_df(df_comp_a, )
    companies_comp_b = create_company_df(df_comp_b, False)
    companies_amsterdam = create_company_df(df_amsterdam, False)
    print("Inserting PeaPME companies")
    db.df_write(df=companies_peapme, table='companies', index=False)
    print("PeaPME companies inserted")
