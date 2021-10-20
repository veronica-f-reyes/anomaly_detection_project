# # #### Anomaly Detection Project
# 
## Finding Anomalies in Code Up Cohorts Curriculum Logs
# ***
# ## Prepare
# 
# ### Clean up and prepare data obtained to use for exploration and modeling
# 
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import Container
import os
import seaborn as sns
import acquire


def prepare_data():

    filename = "prepped_cohort_log_data.csv"

    if os.path.isfile(filename):
        return pd.read_csv(filename)
    else:
        
        #Acquire Code Up curriculum  data using function in acquire.py
        df = acquire.get_cohort_log_data()

        # Create new column to capture program type
        # create a list of our programs
        programs = [
        (df['program_id']==1.0),
        (df['program_id']==2.0),
        (df['program_id']==3.0),
        (df['program_id']==4.0)
        ]

        # create a list of the values we want to assign for each condition
        values = ['php', 'java', 'data_science', 'front_end']    

        # create a new column and use np.select to assign values to it using our lists as arguments
        df['program_type'] = np.select(programs, values)   

        # drop 'deleted_at' column because no entries have a value, all are null - looks like no logs have been deleted
        df = df.drop(columns=['deleted_at'])

        # drop columns with null entries
        df = df.dropna()

        # make 'time' column a date/time type
        df.time = pd.to_datetime(df.time)
        df.date = pd.to_datetime(df.date)
        df.start_date = pd.to_datetime(df.start_date)
        df.end_date = pd.to_datetime(df.end_date)
        df.created_at = pd.to_datetime(df.created_at)
        df.updated_at = pd.to_datetime(df.updated_at)

        # Rename date and time columns to denote that they are log date and times
        df.rename(columns={"date": "log_date", "time": "log_time"})

        # Write that dataframe to disk for later. Called "caching" the data for later.
        df.to_csv(filename)

    return df




#Plots a normalized value count as a percent using catplot
def category_percentages_by_another_category_col(df, category_a, category_b):
    """
    Produces a .catplot with a normalized value count
    """
    (df.groupby(category_b)[category_a].value_counts(normalize=True)
    .rename('percent')
    .reset_index()
    .pipe((sns.catplot, 'data'), x=category_a, y='percent', col=category_b, kind='bar', ))