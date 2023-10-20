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

# Translation dictionaries

'''
https://stockmarketmba.com/globalstockexchanges.php
https://help.yahoo.com/kb/exchanges-data-providers-yahoo-finance-sln2310.html

We are only interested in USA stock markets for this project
'''
dict_exchange_codes_stockmarketmba_Yahoo = {
      "UA" : ""
    , "UN" : ""
    , "UQ" : ""
    , "UR" : ""
    , "UW" : ""
    , "UV" : ""
}

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

# Create or connect to our database then see if we have data in any of the expected tables

#engine = sa.create_engine('sqlite:///output/team122project.sqlite3', echo=False) #, echo=True) use echo=True to print verbose info to the screen
#sqlite_connection = engine.connect()



#=========================================================================================

conn = create_connection("output/team122project.sqlite3")
cur = conn.cursor()
cur.execute("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='yahoo_links') AS yahoo_links_exists")
yahoo_links_exists, = cur.fetchone()

if bool(yahoo_links_exists):
    logging.info("\nDB file output/team122project.sqlite3 already has table yahoo_links - skipping over this section")
else:

    # ==================================================================================================================================
    # Part 1: Create a list of potential Yahoo URLs we can use for downloading historical prices for various equities all over the world
    # ==================================================================================================================================

    # Get the start page from the https://stockmarketmba.com site
    # to obtain all the different exchanges we can work with

    url_start  = "https://stockmarketmba.com/globalstockexchanges.php"

    logging.info(f"\nGetting stock exchange information from {url_start}")

    page_start = requests.get(url_start)
    soup_start = BeautifulSoup(page_start.text, 'html.parser')
    #tbls_start = soup_start.find_all('table') # , {'class':'dataTable'}
    tbls_start = soup_start.find_all("table", attrs={"id": "ETFs"})
    #print(f"Page located at {url_start} contains {len(tbls_start)} table(s)")

    tbl_start = tbls_start[0]
    rows_start = tbl_start.find('tbody').find_all('tr')
    #print(f"Page located at {url_start} contains {len(rows_start)} row(s) in the data table")

    #=========================================================================================

    # Get the header data from the table, and modify it
    # replacing spaces with underscores, and add two new columns

    head_start = [h.text.replace(' ', '_') for h in tbl_start.find('thead').find('tr').find_all('th')]
    head_start.append('Yahoo_Code')
    head_start.append('Listings_Link')
    #print("Table headers (modified)")
    #print(head_start)

    #=========================================================================================

    # Create a dictionary of exchanges, and populate it from the data in the table
    # located in the start page above. As necessary, make data type changes.

    dict_exchanges = {}
    for r in range(len(rows_start)):
        row_start = rows_start[r]
        cells_start  = row_start.find_all('td')
        if cells_start[0].text in dict_exchange_codes_stockmarketmba_Yahoo:
            dict_exchange = {}
            for c in range(len(cells_start)):
                if head_start[c] == '#_of_Stocks':
                    dict_exchange[head_start[c]] = int(cells_start[c].text.replace(',', ''))
                else:
                    dict_exchange[head_start[c]] = cells_start[c].text

            dict_exchange[head_start[-2]] = dict_exchange_codes_stockmarketmba_Yahoo[cells_start[0].text]
            dict_exchange[head_start[-1]] = "https://stockmarketmba.com/" + cells_start[-1].find('a').attrs['href']

            dict_exchanges.update({cells_start[0].text: dict_exchange})

    # Convert dictionary to pandas dataframe so we can do additional manipulation on it

    exchanges = pd.DataFrame.from_dict(dict_exchanges, orient='index').reset_index(drop=True)
    #print(exchanges)

    #=========================================================================================

    # Display a summary count of all stock symbols across all exchanges
    #print(f"Total number of stock symbols for all exchanges: {sum(exchanges['#_of_Stocks'])}")
    logging.info(f"Total number of stock symbols for all exchanges: {sum(exchanges['#_of_Stocks'])}")

    #=========================================================================================

    # Report on the list of unique country names
    #print("Countries available for processing:")
    #print('\r\n'.join(list(exchanges["Country"].unique())))

    #=========================================================================================

    # Choose some subset of countries for processing

    filter_countries_of_interest = exchanges["Country"].isin([
        "USA" # We only use USA data for this project
    ])

    #print(exchanges[filter_countries_of_interest])

    #=========================================================================================

    # Create and display a subset of columns from the subset of countries of interest

    countries_of_interest = exchanges[filter_countries_of_interest][["Listings_Link", "Country", "Yahoo_Code"]]
    list_countries_of_interest = list(zip(
          list(countries_of_interest["Listings_Link"])
        , list(countries_of_interest["Country"])
        , list(countries_of_interest["Yahoo_Code"])
    ))
    #for url, country, yahoo_code in list_countries_of_interest:
    #    print(f"{url}\t{yahoo_code}\t{country}")
    logging.info("\n" + "\n".join([f"{url}\t{yahoo_code}\t{country}" for url, country, yahoo_code in list_countries_of_interest]))

    #=========================================================================================

    # Create a dataframe containing links for all securities for all exchanges we are interested in

    df_Yahoo_links = pd.DataFrame()

    #=========================================================================================

    # Get all the symbols relevant to the market(s) we are interested in,
    # additionally filtering only for those we believe will have data on Yahoo

    ##########################################################################################
    ##########################################################################################
    ##########################################################################################
    # For some unknown reason, this code generates duplicate rows - we should not be using it.
    # I don't have the desire to investigate why since once the data is corrected (DISTINCT)
    # we never need to run this code again. We just need to make sure we keep the table
    # yahoo_links intact in the database located at output/team122project.sqlite3
    ##########################################################################################
    ##########################################################################################
    ##########################################################################################

    for url, country, yahoo_code in list_countries_of_interest:

        logging.info(f"Getting symbols from {url}")

        exchange_key = url[-2:]

        page_exchange = requests.get(url)
        soup_exchange = BeautifulSoup(page_exchange.text, 'html.parser')
        tbl_exchange = soup_exchange.find_all("table", attrs={"id": "ETFs"})[0]

        head_exchange = [h.text.replace(' ', '_') for h in tbl_exchange.find('thead').find('tr').find_all('th')]
        head_exchange.append('Yahoo_Symbol')
        head_exchange.append('Yahoo_Listings_Link')

        rows_equities = tbl_exchange.find('tbody').find_all('tr')

        dict_equities_exchange = {}

        for r in range(len(rows_equities)):
            row_equity = rows_equities[r]
            cells_equity = row_equity.find_all('td')
            dict_equity_exchange = {}
            if cells_equity[0].find('a'):
                dict_equity_exchange["Country"] = country
                dict_equity_exchange["Exchange_ID"] = exchange_key
                for c in range(len(cells_equity)):
                    if head_exchange[c] == 'IPO Date':
                        dict_equity_exchange[head_exchange[c]] = parse(cells_equity[c].text)
                    elif head_exchange[c] == 'Market Cap':
                        dict_equity_exchange[head_exchange[c]] = int(cells_equity[c].text.replace(',', ''))
                    else:
                        dict_equity_exchange[head_exchange[c]] = cells_equity[c].text
                if dict_equity_exchange["Category2"] == "Common stocks": # We only track common stocks - all other forms of debt or equity are ignored for the purpose of this project
                    if dict_equity_exchange["ISIN"] != "" or dict_equity_exchange["SEDOL"] != "": # Needs either one of these columns populated to generate a valid Yahoo URL
                        if dict_equity_exchange["Local_Symbol"] == "":
                            dict_equity_exchange["Local_Symbol"] = cells_equity[0].find('a').contents[0]
                        ticker = dict_equity_exchange["Local_Symbol"] + yahoo_code # Some local symbols are wrong for Yahoo - Thailand is an example (already contains a dotted suffix which must be removed and replaced with Yahoo suffix for Thailand)
                        dict_equity_exchange[head_exchange[-2]] = ticker
                        # Instead of replacing the timestamps with actual values now, let's leave the timestamps as placeholders for now
                        # we will use these "template" links each day for updates too
                        dict_equity_exchange[head_exchange[-1]] = f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={{timestmp1}}&period2={{timestmp2}}&interval=1d&events=history&includeAdjustedClose=true"
                        # Eventual format after replacement of placeholders
                        # https://query1.finance.yahoo.com/v7/finance/download/MSFT?period1=946684800&period2=1697414400&interval=1d&events=history&includeAdjustedClose=true

                        logging.info(f"{cells_equity[0].text}")
                        dict_equities_exchange.update({cells_equity[0].text: dict_equity_exchange})

                df_equities_exchange = pd.DataFrame.from_dict(dict_equities_exchange, orient='index').reset_index(drop=True)
                
                #outputfilename = f"output/securities_{country}_{exchange_key}.csv"
                #with open(outputfilename, "w") as filehandle:
                #    df_equities_exchange.to_csv(filehandle, index=False, lineterminator="\n", encoding='utf-8') # 'utf-8-sig'

                # Add the current exchange's symbols to master dataframe
                df_Yahoo_links = pd.concat([df_Yahoo_links, df_equities_exchange])
    #=========================================================================================

    arr_countries = df_Yahoo_links.Country.unique()

    arr_sectors = df_Yahoo_links.GICS_Sector.unique()

    # We only track stocks in certain sectors
    arr_sectors_keep = np.array(['Financials', 'Communication Services',
        'Consumer Discretionary', 'Information Technology', 'Industrials',
        'Consumer Staples', 'Energy', 'Materials', 'Health Care',
        'Capital Goods', 'Real Estate', 'Utilities', 'Retailing',
        'Real Estate Development & Operations', 'Commercial REITs',
        'Technology Hardware & Equipment', 'Food Beverage & Tobacco',
        'Automobiles & Components', 'Consumer Services',
        'Health Care Equipment & Services', 'Consumer Durables & Apparel',
        'Banks', 'Minerals', 'Information technology'])

    df_Yahoo_links_goodsectors = df_Yahoo_links[df_Yahoo_links.GICS_Sector.isin(arr_sectors_keep)]

    #print(f"From the {df_Yahoo_links.shape[0]} rows in the original list, removing 'bad industry sectors' reduced the number or rows to {df_Yahoo_links_goodsectors.shape[0]}")
    logging.info(f"\nFrom the {df_Yahoo_links.shape[0]} rows in the original list, removing 'bad industry sectors' reduced the number or rows to {df_Yahoo_links_goodsectors.shape[0]}")

    arr_ipo_dates = df_Yahoo_links_goodsectors.IPO_Date.unique()

    # Many of the IPO dates are set to a fake date (Jan. 1, 1900) - we will filter these out
    df_Yahoo_links_good_sectors_dates = df_Yahoo_links_goodsectors[df_Yahoo_links_goodsectors.IPO_Date != min(arr_ipo_dates)]

    df_Yahoo_links_good_sectors_dates['Market_Cap'] = pd.to_numeric(df_Yahoo_links_good_sectors_dates['Market_Cap'].str.replace(',', ''))

    df_Yahoo_links_good_sectors_dates20000101 = df_Yahoo_links_good_sectors_dates[df_Yahoo_links_good_sectors_dates['IPO_Date'] < '2000-01-01']

    logging.info(f"\nAfter filtering data to use only good industry sectors and valid IPO dates, the number of securities available is: {df_Yahoo_links_good_sectors_dates20000101.shape[0]}")

    # Write the master dataframe to a SQLite3 database
    df_Yahoo_links_good_sectors_dates20000101.to_sql(name='yahoo_links', con=conn, if_exists='replace', index=False)

    logging.info(f"Table 'yahoo_links' created in database 'output/team122project.sqlite3' with {df_Yahoo_links_good_sectors_dates20000101.shape[0]} rows")

# ==================================================================================================================================
# Part 2: Use the Yahoo links we created in Part 1 to download the historical prices
# ==================================================================================================================================

logging.info("\nStarting Yahoo link processing")

#=========================================================================================

# Generate timestamps for accessing Yahoo Finance API

# look only as far as yesterday's close date, in case closing price is not yet available when running today
dt_now = date.today() - timedelta(days=1)
dt_now = datetime(dt_now.year, dt_now.month, dt_now.day, tzinfo=timezone.utc)
dt_20000101 = datetime(2000, 1, 1, tzinfo=timezone.utc)

timestmp1 = int(dt_20000101.timestamp())
timestmp2 = int(dt_now.timestamp())

logging.info(f"\nYahoo period1 = {timestmp1} corresponds to date {dt_20000101}")
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

df_Yahoo_links_good_sectors_dates20000101 = pd.read_sql('select * from yahoo_links', con=conn, dtype=datatypes, parse_dates=datecolumns) #, date_format='mm/dd/yyyy')

logging.info(f"\nRead {df_Yahoo_links_good_sectors_dates20000101.shape[0]} rows.\n{df_Yahoo_links_good_sectors_dates20000101.head()}")

#=========================================================================================

# Create a series of dates from Jan 4, 2000 to Dec 31, 2020
idxDates = pd.Series(pd.date_range(start=dt_20000101.date(), end=date(dt_now.year, dt_now.month, dt_now.day)))
# Create an empty dataframe with just the index (= the series of dates)
dfAllDates = pd.DataFrame(index=idxDates)
# Rename the index
dfAllDates.index.name="Date"

logging.info(f"\nFirst 10 dates in dataframe dfAllDates:\n{dfAllDates.head(10)}")

logging.info(f"\nLast 10 dates in dataframe dfAllDates:\n{dfAllDates.tail(10)}")

# -----------------------------------------------------------------------------------------------------------------------
# This is where the Yahoo downloads actually occur - may need to use random proxy URLs to get past Yahoo's request limits
# -----------------------------------------------------------------------------------------------------------------------

loopUntil = df_Yahoo_links_good_sectors_dates20000101.shape[0]
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
        symbol, link = df_Yahoo_links_good_sectors_dates20000101.iloc[i][["Yahoo_Symbol", "Yahoo_Listings_Link"]]

        # Replace the placeholders in our link with actual timestamps
        link = link.replace('{timestmp1}', str(timestmp1)).replace('{timestmp2}', str(timestmp2))

        #proxy_url = random.choice(lst_good_proxies_url)
        #logging.info(f"\nAttempting download historical prices for symbol '{symbol}'' from {link} using proxy {proxy_url}")
        logging.info(f"\nAttempting download historical prices for symbol '{symbol}'' from {link}")
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

logging.info(f"\nSaving joined dataframe to table 'securities_by_date' in database 'output/team122project.sqlite3'")
dfAllDates.to_sql(name='securities_by_date', con=conn, if_exists='replace', index=True)

"""
outputfilename = f"output/database_partial_{len(dict_dfYahooSecurities.keys())}_local_currency.csv"
logging.info(f"\nSaving joined dataframe to file: {outputfilename}")
with open(outputfilename, "w") as filehandle:
    #dfAllDates.to_csv(filehandle, index=True, lineterminator = '\r', encoding='utf-8-sig')
    dfAllDates.to_csv(filehandle, index=True, lineterminator = '\n', encoding='utf-8')
if not filehandle.closed:
    filehandle.close()
filehandle = None
"""