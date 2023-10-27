from flask import Flask, request, jsonify
import csv
import sqlite3
import datetime

app = Flask(__name__)

conn = sqlite3.connect('../output/team122project/team122project.sqlite3')
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_data (
        id INTEGER PRIMARY KEY,
        ticker TEXT,
        date TEXT,
        strike REAL
    )
''')
conn.commit()


def filter_strike_data(reader, tickers, start_date, end_date, mode, range_mode):
    filter_data = {}
    
    current_date = datetime.date.today()
    current_day = int(current_date.day)
    
    if mode == 'yearly':
        if not range_mode:
            range_mode = "1"
        current_month = int(current_date.month)
        previous_month = current_month
        current_year = int(current_date.year)
        previous_year = current_year - int(range_mode)
        
    if  mode == 'monthly':
        if not range_mode:
            range_mode = "1"
        current_month = int(current_date.month)
        previous_month = current_month - int(range_mode) - 1
        current_year = int(current_date.year)
        previous_year = current_year
           
    for row in reader:
        date = row['Date']
        if mode == 'daily':
            if start_date and end_date:
                for ticker in tickers:
                    if ticker in row:
                        strike = float(row[ticker])      
                        if start_date <= date <= end_date:
                            strike_data = {'date': date, 'strike': strike}
                            if ticker not in filter_data:
                                filter_data[ticker] = []
                            filter_data[ticker].append(strike_data)
            else:
                for ticker in tickers:
                    if ticker in row:
                        strike = float(row[ticker])
                        strike_data = {'date': date, 'strike': strike}
                        if ticker not in filter_data:
                            filter_data[ticker] = []
                        filter_data[ticker].append(strike_data)
        
        elif mode == 'monthly' or mode == 'yearly':

            if previous_month and previous_month <= 0:
                previous_month = previous_month + 12
            
            if previous_year and previous_year < 2010:
                previous_year = 2010
                
            for ticker in tickers:
                if ticker in row:
                    strike = float(row[ticker])
                    if (f"{previous_year}-{previous_month:02d}-{current_day}") <= date <= (f"{current_year}-{current_month:02d}-{current_day}"):
                        strike_data = {'date': date, 'strike': strike}
                        if ticker not in filter_data:
                            filter_data[ticker] = []
                        filter_data[ticker].append(strike_data)

    return filter_data


@app.route('/get_tickers_strike', methods=['POST'])
def get_tickers_strike():
    request_data = request.get_json()
    
    tickers = request_data.get('tickers', [])
    start_date = request_data.get('start_date', None)
    end_date = request_data.get('end_date', None)
    mode = request_data.get('mode', 'daily')
    range_mode = request_data.get('range_mode', None)
    
    modes = ['daily', 'monthly', 'yearly']
    if mode not in modes:
        return jsonify({'error': 'Invalid mode. Mode must be one of ["daily", "monthly", "yearly"]'}), 400

    result = dict()

    try:
        with open('../output/historical_ticker_data/historical_ticker_data.csv', 'r') as file:
            reader = csv.DictReader(file)
            result = filter_strike_data(reader, tickers, start_date, end_date, mode, range_mode)
    
    except FileNotFoundError:
        return jsonify({'error': 'Data file not found'}), 404

    return jsonify(result), 200

if __name__ == '__main__':
    app.run(debug=True)