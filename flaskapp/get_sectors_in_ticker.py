from flask import Flask, request, jsonify
import sqlite3


app = Flask(__name__)

def get_sectors_by_ticker(ticker):
    conn = sqlite3.connect('../output/team122project/team122project.sqlite3')
    cursor = conn.cursor()

    cursor.execute('SELECT Description, GICS_sector FROM yahoo_links WHERE Symbol = ?', (ticker,))
    #print("fetchone", cursor.fetchone())
    #print("fetchall", cursor.fetchall())

    results = cursor.fetchone()
    
    conn.close()

    return results

@app.route('/get_sectors_in_ticker', methods=['POST'])
def get_sectors_in_ticker():
    request_data = request.get_json()
    
    tickers = request_data.get('tickers', [])
    print("tickers", tickers)

    if not tickers:
        return jsonify({"error": "Please provide at least one ticker"}), 400

    result = dict()
    for ticker in tickers:
        print("ticker", ticker)

        get_data = get_sectors_by_ticker(ticker)
        print("get_data", get_data)
        data = {'company': get_data[0], 'ticker': ticker}
        sector = get_data[1]
        print("data", data)

        if sector not in result:
            result[sector] = []
        result[sector].append(data)
        print("result", result)
    return result, 200
    #return jsonify(result), 200


if __name__ == '__main__':
    app.run()