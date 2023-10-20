import pandas as pd 
import csv

def username():
    return 'gburdell3'

def data_wrangling():
    df =  pd.read_csv('output/combined_securities_good_sectors_templates.csv', encoding='unicode_escape')
    return df.head() , df.values.tolist()

