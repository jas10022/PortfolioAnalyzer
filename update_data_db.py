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

# initialize the log settings
log_timestamp = datetime.utcnow().isoformat(sep="_", timespec="seconds").replace("-", "").replace(":", "")
logging.basicConfig(filename = f'output/CSE6242_Final_Project_Team_122_{log_timestamp}.log', level = logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')

#=========================================================================================

# Connect to the database so that we can determine when the last update time was

historical_ticker_data_file = "output/historical_ticker_data.csv"

"""
conn = create_connection("output/team122project.sqlite3")
cur = conn.cursor()

cur.execute("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='securities_by_date') AS securities_exist")
securities_exist, = cur.fetchone()

if bool(securities_exist):
    cur.execute("SELECT Date AS LastUpdate FROM securities_by_date ORDER BY Date DESC LIMIT 1")
    last_update, = cur.fetchone()
else:
    last_update = '2009-12-31T00:00:00' # Hard code to the day before our earliest date to be considered for this project
"""
# Wrong! We no longer store data in the SQLite database - all ticker prices are stored in the file output/combined_securities.csv

default_start_date = '2009-12-31T00:00:00' # Hard code to the day before our earliest date to be considered for this project
try:
    df_combined_securities = pd.read_csv(historical_ticker_data_file)
    if df_combined_securities.shape[0] < 1:
        last_update = default_start_date
    else:
        last_update = date.fromisoformat(df_combined_securities.iloc[-1]['Date'])
except FileNotFoundError as e:
    logging.info(f"Error reading historical data: {str(type(e))} : {e}")
    last_update = default_start_date
    df_combined_securities = pd.DataFrame() # Create an empty dataframe because we could not find a CSV file on disk
except KeyError as e:
    logging.info(f"Error reading historical data: {str(type(e))} : {e}")
    last_update = default_start_date

logging.info(f"Last successful update saved: {last_update}")

#=========================================================================================

# Create the start and end datetimes for queying Yahoo! Finance

dt_now = date.today()
dt_now = datetime(dt_now.year, dt_now.month, dt_now.day, tzinfo=timezone.utc)

dt_last_update = datetime.fromisoformat(last_update)
dt_last_update = dt_last_update + timedelta(days=1)
dt_last_update = datetime(dt_last_update.year, dt_last_update.month, dt_last_update.day, tzinfo=timezone.utc)

logging.info(f'Time range: {dt_last_update.isoformat(sep=" ", timespec="seconds")} to {dt_now.isoformat(sep=" ", timespec="seconds")}')

if dt_last_update == dt_now:
    logging.info(f'CSV file {historical_ticker_data_file} contains the latest data - stopping the process')
    SystemExit()

timestmp1 = int(dt_last_update.timestamp())
timestmp2 = int(dt_now.timestamp())

logging.info(f"\nYahoo period1 = {timestmp1} corresponds to date {dt_last_update}")
logging.info(f"\nYahoo period2 = {timestmp2} corresponds to date {dt_now}")

#=========================================================================================

# Prepare for reading from Yahoo! Finance

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
    , 'last_update'         : str
}

datecolumns = ['IPO_Date', 'last_update']

time_22hours_ago = (datetime.utcnow() - timedelta(hours=22)).isoformat(sep=" ", timespec="seconds")

conn = create_connection("output/team122project.sqlite3")
cur = conn.cursor()

df_Yahoo_links_good_sectors_latestdates = pd.read_sql('select * from yahoo_links', con=conn, dtype=datatypes, parse_dates=datecolumns) #, date_format='mm/dd/yyyy')

#logging.info(f"\nRead {df_Yahoo_links_good_sectors_latestdates.shape[0]} rows.\n{df_Yahoo_links_good_sectors_latestdates.head()}")

list_needs_updating = df_Yahoo_links_good_sectors_latestdates[df_Yahoo_links_good_sectors_latestdates["last_update"] < time_22hours_ago].index.to_list()

#=========================================================================================

# Create a series of dates from the latest date to be updated to yesterday
idxDates = pd.Series(pd.date_range(start=dt_last_update.date(), end=date(dt_now.year, dt_now.month, dt_now.day)))
# Create an empty dataframe with just the index (= the series of dates)
dfAllDates = pd.DataFrame(index=idxDates)
# Rename the index
dfAllDates.index.name="Date"

#logging.info(f"\nFirst 10 dates in dataframe dfAllDates:\n{dfAllDates.head(10)}")
#logging.info(f"\nLast 10 dates in dataframe dfAllDates:\n{dfAllDates.tail(10)}")

# -----------------------------------------------------------------------------------------------------------------------
# This is where the Yahoo downloads actually occur - may need to use random proxy URLs to get past Yahoo's request limits
# -----------------------------------------------------------------------------------------------------------------------

loopUntil = list_needs_updating
####################################################################
####################################################################
####################################################################
#loopUntil = list_needs_updating[:4] # Try with a smaller set of data, instead of all files
####################################################################
####################################################################
####################################################################
dict_dfYahooSecurities = {}

# Read the links from our SQLite3 database

with requests.session():
    header = {
        'Connection': 'keep-alive',
        'Expires': '-1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) \
        AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36'
    }

    for i in loopUntil:
        symbol, link = df_Yahoo_links_good_sectors_latestdates.loc[i][["Yahoo_Symbol", "Yahoo_Listings_Link"]]

        # Replace the placeholders in our link with actual timestamps
        link = link.replace('{timestmp1}', str(timestmp1)).replace('{timestmp2}', str(timestmp2))

        #proxy_url = random.choice(lst_good_proxies_url)
        #logging.info(f"\nAttempting download historical prices for symbol '{symbol}' from {link} using proxy {proxy_url}")
        logging.info(f"\nAttempting download historical prices for symbol '{symbol}' from {link}")
        try:
            #dict_dfYahooSecurities[symbol] = pd.read_csv(link, usecols=["Date", "Adj Close"], parse_dates=True, infer_datetime_format=True)
            # We cannot use read_csv directly, because Yahoo will block our requests if we issue too many from the same IP address, so...
            #response = requests.get(link, proxies={"http": proxy_url, "https": proxy_url})
            response = requests.get(link, headers=header)

            if response.status_code == 200:
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
            else:
                logging.info(f"Failed {symbol}: {response.status_code}")
        except Exception as e:
            logging.exception(str(e))
            #print(str(e))
            continue

logging.info("\nJoining all columns of the downloaded securities into a single dataframe")

#=========================================================================================

# Merge all the data into a single dataframe

for df in dict_dfYahooSecurities.values():
    dfAllDates = dfAllDates.join(df)

#=========================================================================================

# Find the names of the securities that we successfully retrieved from Yahoo before Yahoo started throttling its responses to our requests
list_updated_symbols = dfAllDates.columns.to_list()

if len(list_updated_symbols) == 0:
    # No more symbols downloaded this run - we can stop and join all previously downloaded CSV files
    logging.info(f"\nNo more symbols downloaded this run - process can end for this day")
    base_filename = f"^database_{log_timestamp.split('_')[0]}" + "_[0-9]+_[0-9]+.csv"
    file_pieces = ["output/"+f for f in os.listdir("output") if re.search(base_filename, f)]
    dfNewDates = pd.DataFrame()
    for i in range(len(file_pieces)):
        dfNextPiece = pd.read_csv(file_pieces[i], index_col=0)
        dfNewDates = pd.concat([dfNewDates, dfNextPiece], axis=1, ignore_index=False)

    # Re-order the columns in the merged dataframe alphabetically to match column order in master dataframe
    new_col_order = sorted(dfNewDates.columns)
    dfNewDates = dfNewDates[new_col_order]

    # Before appending to master dataframe, see if columns match in both
    columns_match = dfNewDates.columns.to_list() == df_combined_securities.columns.to_list()
    if columns_match:
        logging.info(f"Column order and count {len(dfNewDates.columns.to_list())} of dfNewDates matches column order and count {len(df_combined_securities.columns.to_list())} of df_combined_securities")
    else:
        logging.info(f"Column order of dfNewDates DOES NOT MATCH that of df_combined_securities, or lengths do not match {len(dfNewDates.columns.to_list())} vs {len(df_combined_securities.columns.to_list())}")
    
    # Now append dfNewDates to bottom of existing master dataframe
    df_combined_securities = pd.concat([df_combined_securities, dfNewDates])

    logging.info(f"Before dropping empty rows, rowcount = {df_combined_securities.shape[0]}")
    df_trading_dates = df_combined_securities.drop(df_combined_securities[df_combined_securities.any(axis=1) == False].index, axis=0)
    logging.info(f"After dropping empty rows, rowcount = {df_trading_dates.shape[0]}")

    outputfilename = historical_ticker_data_file
    logging.info(f"\nSaving joined dataframe to file: {outputfilename}")
    with open(outputfilename, "w") as filehandle:
        #dfAllDates.to_csv(filehandle, index=True, lineterminator = '\r', encoding='utf-8-sig')
        df_trading_dates.to_csv(filehandle, index=True, lineterminator = '\n', encoding='utf-8')
    if not filehandle.closed:
        filehandle.close()
    filehandle = None

    logging.info(f"Master data file updated, rowcount = {df_trading_dates.shape[0]}")

else:
    indexes_to_update = df_Yahoo_links_good_sectors_latestdates[df_Yahoo_links_good_sectors_latestdates['Yahoo_Symbol'].isin(list_updated_symbols)].index

    # Set the update timestamp on all those securities that we successfully retireved latest (nightly) prices for
    update_time = datetime.utcnow().isoformat(sep=" ", timespec="seconds")
    df_Yahoo_links_good_sectors_latestdates.loc[indexes_to_update, "last_update"] = update_time
    df_Yahoo_links_good_sectors_latestdates.to_sql(name='yahoo_links', con=conn, if_exists='replace', index=False)

    # Drop weekends and holidays in case we have any such "empty" rows
    logging.info(f"Before dropping empty rows, rowcount = {dfAllDates.shape[0]}")
    df_trading_dates = dfAllDates.drop(dfAllDates[dfAllDates.any(axis=1) == False].index, axis=0)
    logging.info(f"After dropping empty rows, rowcount = {df_trading_dates.shape[0]}")

    # Prepare the filename for writing out the CSV file for this run on Yahoo! Finance - we will do this again in another hour
    outputfilename = f"output/database_{log_timestamp}_{len(df_trading_dates.columns)}.csv"
    logging.info(f"\nSaving joined dataframe to file: {outputfilename}")
    with open(outputfilename, "w") as filehandle:
        #dfAllDates.to_csv(filehandle, index=True, lineterminator = '\r', encoding='utf-8-sig')
        df_trading_dates.to_csv(filehandle, index=True, lineterminator = '\n', encoding='utf-8')
    if not filehandle.closed:
        filehandle.close()
    filehandle = None

    logging.info(f"\nAwaiting another run against the Yahoo! Finance")

logging.info(f"\nProcess ended for this run")