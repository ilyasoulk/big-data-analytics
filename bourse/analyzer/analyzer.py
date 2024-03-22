import pandas as pd
import requests
import numpy as np
import sklearn
import tarfile
import os
import dateutil
import glob

import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker

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
    market_df = pd.concat({dateutil.parser.parse((f.split(market)[1].split('.'))[0]): pd.read_pickle(f) for f in files})
    return market_df


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


    df_compa = create_super_data_frame("compA")
   # df_compb = create_super_data_frame("compB")
   # df_euronx = create_super_data_frame("euronx")
   # df_lse = create_super_data_frame("lse")
   # df_milano = create_super_data_frame("milano")
   # df_dbx = create_super_data_frame("dbx")
   # df_mercados = create_super_data_frame("mercados")
   # df_amsterdam = create_super_data_frame("amsterdam")
   # df_xetra = create_super_data_frame("xetra")
   # df_bruxelle = create_super_data_frame("bruxelle")