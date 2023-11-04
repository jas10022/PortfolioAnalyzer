from flask import Flask, request, jsonify
import sqlite3


app = Flask(__name__)

def get_tickers_by_sector(sector):
    conn = sqlite3.connect('../output/team122project/team122project.sqlite3')
    cursor = conn.cursor()

    cursor.execute('SELECT Description, Symbol FROM yahoo_links WHERE GICS_sector = ?', (sector,))
    #print("fetchone", cursor.fetchone())
    #print("fetchall", cursor.fetchall())

    results = cursor.fetchall()
    
    conn.close()

    return results

@app.route('/get_tickers_in_sector', methods=['POST'])
def get_tickers_in_sector():
    request_data = request.get_json()
    
    sectors = request_data.get('sectors', [])
    
    if not sectors:
        return jsonify({"error": "Please provide at least one sector"}), 400

    result = dict()
    filter_data = dict()
    for sector in sectors:
        get_data = get_tickers_by_sector(sector)

        for company, ticker in get_data:
            data = {'company': company, 'ticker': ticker}

            if sector not in filter_data:
                filter_data[sector] = []
            filter_data[sector].append(data)
    result = filter_data
    return result, 200
    #return jsonify(result), 200


if __name__ == '__main__':
    app.run()