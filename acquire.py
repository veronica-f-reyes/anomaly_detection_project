#Anomaly Detection Project

# 1. Acquire data from mySQL using the python module to connect and query. You will want to end with a single dataframe.

# Functions to obtain Code Up cohort log data from the Codeup Data Science Database: curriculum_logs
#It returns a pandas dataframe.
#--------------------------------

#This function uses my user info from my env file to create a connection url to access the Codeup db.  

from typing import Container
import pandas as pd
import os
from env import host, user, password

# regular imports

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import scipy.stats as stats
import env

#FUNCTION to connect to database for SQL query use
# -------------------------------------------------
def get_db_url(host, user, password, database):
        
    url = f'mysql+pymysql://{user}:{password}@{host}/{database}'
    
    return url

#FUNCTION to get data from Code Up database
# ----------------------------------------
def get_cohort_log_data():
    
    filename = "cohort_logs.csv"

    if os.path.isfile(filename):
        return pd.read_csv(filename, index_col=0)
    else:

        database = 'curriculum_logs'

        #Create SQL query to select data from Code Up database
        query = '''
                SELECT * 
                FROM cohorts
                RIGHT JOIN logs
                ON cohorts.id = logs.cohort_id
                ORDER BY cohorts.id ASC;
                '''

         # read the SQL query into a dataframe
        df = pd.read_sql(query, get_db_url(host,user, password, database))

         # Write that dataframe to disk for later. Called "caching" the data for later.
        df.to_csv(filename)

        # Return the dataframe to the calling code
        return df

def nulls_by_col(df):
    num_missing = df.isnull().sum()
    rows = df.shape[0]
    prcnt_miss = num_missing / rows * 100
    cols_missing = pd.DataFrame({'num_rows_missing': num_missing, 'percent_rows_missing': prcnt_miss})
    return cols_missing

def nulls_by_row(df):
    num_missing = df.isnull().sum(axis=1)
    prcnt_miss = num_missing / df.shape[1] * 100
    rows_missing = pd.DataFrame({'num_cols_missing': num_missing, 'percent_cols_missing': prcnt_miss})\
    .reset_index()\
    .groupby(['num_cols_missing', 'percent_cols_missing']).count()\
    .rename(index=str, columns={'customer_id': 'num_rows'}).reset_index()
    return rows_missing

def summarize(df):
    '''
    summarize will take in a single argument (a pandas dataframe) 
    and output to console various statistics on said dataframe, including:
    # .head()
    # .info()
    # .describe()
    # value_counts()
    # observation of nulls in the dataframe
    '''
    print('=====================================================\n\n')
    print('Dataframe head: ')
    print(df.head(3).to_markdown())
    print('=====================================================\n\n')
    print('Dataframe info: ')
    print(df.info())
    print('=====================================================\n\n')
    print('Dataframe Description: ')
    print(df.describe().to_markdown())
    num_cols = [col for col in df.columns if df[col].dtype != 'O']
    cat_cols = [col for col in df.columns if col not in num_cols]
    print('=====================================================')
    print('DataFrame value counts: ')
    for col in df.columns:
        if col in cat_cols:
            print(df[col].value_counts())
        else:
            print(df[col].value_counts(bins=10, sort=False))
    print('=====================================================')
    print('nulls in dataframe by column: ')
    print(nulls_by_col(df))
    print('=====================================================')
    print('nulls in dataframe by row: ')
    print(nulls_by_row(df))
    print('=====================================================')

def remove_columns(df, cols_to_remove):
    df = df.drop(columns=cols_to_remove)
    return df

def handle_missing_values(df, prop_required_columns=0.5, prop_required_row=0.75):
    threshold = int(round(prop_required_columns * len(df.index), 0))
    df = df.dropna(axis=1, thresh=threshold)
    threshold = int(round(prop_required_row * len(df.columns), 0))
    df = df.dropna(axis=0, thresh=threshold)
    return df



#CALL function to get and create cohort_logs.csv locally
get_cohort_log_data()