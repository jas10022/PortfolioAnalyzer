# May need to install any of the following:
# pip install requests
# pip install bs4
# pip install pandas
# pip install sqlalchemy

# Import the libraries we will use to scrape symbol data
import io
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlalchemy as sa
import sqlite3
from sqlite3 import Error
from datetime import date, datetime, timezone, timedelta
from dateutil.parser import parse

import os
import glob
import re
import csv

import logging
import random

#=========================================================================================

# Define required functions

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

#=========================================================================================

# Ensure our output directory exists
if not os.path.exists('output'):
    os.makedirs('output')

#=========================================================================================

# initialize the log settings
logging.basicConfig(filename = 'output/CSE6242_Final_Project_Team_122.log', level = logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')

#=========================================================================================

# We may need to use proxies to prevent Yahoo from blocking our scraping

# From: https://vpnoverview.com/privacy/anonymous-browsing/free-proxy-servers/ (could not scrape this site so hard-coded)
lst_good_proxies_url = [
	  '163.172.168.124:3128'
	, '163.172.182.164:3128'
	, '164.68.105.235:3128'
	, '173.192.21.89:80'
	, '176.31.200.104:3128'
	, '185.118.141.254:808'
	, '188.100.212.208:21129'
	, '212.112.97.27:3128'
	, '217.113.122.142:3128'
	, '5.199.171.227:3128'
	, '51.254.69.243:3128'
	, '51.68.207.81:80'
	, '81.171.24.199:3128'
	, '83.77.118.53:17171'
	, '83.77.118.53:17171'
	, '83.79.50.233:64527'
	, '84.201.254.47:3128'
	, '91.211.245.176:8080'
	, '93.171.164.251:8080'
	, '95.156.82.35:3128'
]

#=========================================================================================

conn = create_connection("output/team122project.sqlite3")

cur = conn.cursor()
cur.execute("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='securities_by_date') AS securities_exist")
securities_exist, = cur.fetchone()

if bool(securities_exist):
    cur.execute("SELECT Date AS LastUpdate FROM securities_by_date ORDER BY Date DESC LIMIT 1")
    last_update, = cur.fetchone()
else:
    last_update = '2009-12-31T00:00:00' # Hard code to the day before our earliest date to be considered for this project

dt_last_update = datetime.fromisoformat(last_update)

logging.info(f"Last update for an active trading day occurred on: {dt_last_update}")

#=========================================================================================

logging.info("\nStarting Yahoo link processing")

#=========================================================================================

# Generate timestamps for accessing Yahoo Finance API

#dt_now = date.today() - timedelta(days=1)
# For update purposes, do not subtract one day. In fact, there must always be
# at least one day difference between the start and end dates according to API
dt_now = date.today()
dt_now = datetime(dt_now.year, dt_now.month, dt_now.day, tzinfo=timezone.utc)
dt_last_update = dt_last_update + timedelta(days=1)
dt_last_update = datetime(dt_last_update.year, dt_last_update.month, dt_last_update.day, tzinfo=timezone.utc)

timestmp1 = int(dt_last_update.timestamp())
timestmp2 = int(dt_now.timestamp())

logging.info(f"\nYahoo period1 = {timestmp1} corresponds to date {dt_last_update}")
logging.info(f"\nYahoo period2 = {timestmp2} corresponds to date {dt_now}")

#=========================================================================================

datatypes = {
      'Country'             : str
    , 'Exchange_ID'         : str
    , 'Symbol'              : str
    , 'Description'         : str
    , 'Local_Symbol'        : str
    , 'IPO_Date'            : str
    , 'Category1'           : str
    , 'Category2'           : str
    , 'Category3'           : str
    , 'GICS_Sector'         : str
    , 'ISIN'                : str
    , 'SEDOL'               : str
    , 'Market_Cap'          : str #np.int64
    , 'Currency'            : str
    , 'Actions'             : str
    , 'Yahoo_Symbol'        : str
    , 'Yahoo_Listings_Link' : str
}

datecolumns = ['IPO_Date']

# Read the links from our SQLite3 database

df_Yahoo_links_good_sectors_latestdates = pd.read_sql('select * from yahoo_links', con=conn, dtype=datatypes, parse_dates=datecolumns) #, date_format='mm/dd/yyyy')

logging.info(f"\nRead {df_Yahoo_links_good_sectors_latestdates.shape[0]} rows.\n{df_Yahoo_links_good_sectors_latestdates.head()}")

#=========================================================================================

# Create a series of dates from the latest date to be updated to yesterday
idxDates = pd.Series(pd.date_range(start=dt_last_update.date(), end=date(dt_now.year, dt_now.month, dt_now.day)))
# Create an empty dataframe with just the index (= the series of dates)
dfAllDates = pd.DataFrame(index=idxDates)
# Rename the index
dfAllDates.index.name="Date"

logging.info(f"\nFirst 10 dates in dataframe dfAllDates:\n{dfAllDates.head(10)}")

logging.info(f"\nLast 10 dates in dataframe dfAllDates:\n{dfAllDates.tail(10)}")

# -----------------------------------------------------------------------------------------------------------------------
# This is where the Yahoo downloads actually occur - may need to use random proxy URLs to get past Yahoo's request limits
# -----------------------------------------------------------------------------------------------------------------------

loopUntil = df_Yahoo_links_good_sectors_latestdates.shape[0]
####################################################################
####################################################################
####################################################################
loopUntil = 4 # Try with a smaller set of data, instead of all files
####################################################################
####################################################################
####################################################################
dict_dfYahooSecurities = {}

with requests.session():
    header = {
        'Connection': 'keep-alive',
        'Expires': '-1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) \
        AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36'
    }

    for i in range(loopUntil):
        symbol, link = df_Yahoo_links_good_sectors_latestdates.iloc[i][["Yahoo_Symbol", "Yahoo_Listings_Link"]]

        # Replace the placeholders in our link with actual timestamps
        link = link.replace('{timestmp1}', str(timestmp1)).replace('{timestmp2}', str(timestmp2))

        #proxy_url = random.choice(lst_good_proxies_url)
        #logging.info(f"\nAttempting download historical prices for symbol '{symbol}'' from {link} using proxy {proxy_url}")
        print(f"\nAttempting download historical prices for symbol '{symbol}'' from {link}")
        try:
            #dict_dfYahooSecurities[symbol] = pd.read_csv(link, usecols=["Date", "Adj Close"], parse_dates=True, infer_datetime_format=True)
            # We cannot use read_csv directly, because Yahoo will block our requests if we issue too many from the same IP address, so...
            #response = requests.get(link, proxies={"http": proxy_url, "https": proxy_url})
            response = requests.get(link, headers=header)
            #logging.info(f"\n{response.text[:200]}")
            #logging.info(f"\n{response.text[-200:]}")
            dict_dfYahooSecurities[symbol] = pd.read_csv(io.StringIO(response.text), usecols=["Date", "Adj Close"])
            # Convert the Date column to the correct data type
            dict_dfYahooSecurities[symbol]['Date'] = pd.to_datetime(dict_dfYahooSecurities[symbol]['Date'], utc=False) #, utc=True
            dict_dfYahooSecurities[symbol].columns = ["Date", symbol]
            dict_dfYahooSecurities[symbol].set_index('Date', drop=True, inplace=True)

            # We don't need to save to CSV since we write directly to table in SQLite3 database now
            """
            outputfilename =f"output/history_security_{symbol}.csv"
            with open(outputfilename, "w") as filehandle:
                #dict_dfYahooSecurities[symbol].to_csv(filehandle, index=False, lineterminator = '\r', encoding='utf-8-sig')
                dict_dfYahooSecurities[symbol].to_csv(filehandle, index=False, lineterminator = '\n', encoding='utf-8')
            if not filehandle.closed:
                filehandle.close()
            filehandle = None
            """
        except Exception as e:
            logging.exception(str(e))
            continue

logging.info("\nJoining all columns of the downloaded securities into a single dataframe")

for df in dict_dfYahooSecurities.values():
    dfAllDates = dfAllDates.join(df)

# Create a dataframe of rows for each valid trading date by removing rows where all symbols have null values
dfToAppend = dfAllDates[dfAllDates.any(axis=1)]

# Check if there are actually any rows to append - there may be none
if dfToAppend.shap[0] > 0:
    print(f"\nSaving joined dataframe to table 'securities_by_date' in database 'output/team122project.sqlite3'")
    dfToAppend.to_sql(name='securities_by_date', con=conn, if_exists='append', index=True)
else:
    print(f"There were no rows containing valid data to be appended to the table 'securities_by_date'")