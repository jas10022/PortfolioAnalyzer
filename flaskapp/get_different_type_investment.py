from flask import Flask, request, jsonify
import sqlite3
from datetime import datetime

app = Flask(__name__)

# Function to retrieve companies in the same sector get_companies_in_sector
def get_type_investment(type_of_investment, sector):
    conn = sqlite3.connect('../output/team122project/team122project.sqlite3')
    cursor = conn.cursor()

    cursor.execute('SELECT Symbol, Description, Market_Cap, last_update FROM yahoo_links WHERE (Category3 = ? and GICS_sector = ?)', (type_of_investment, sector,))
    #print("fetchone", cursor.fetchone())
    #print("fetchall", cursor.fetchall())

    results = cursor.fetchall()
    
    conn.close()

    return results

@app.route('/get_different_type_investment', methods=['POST'])
def get_different_type_investment():
    request_data = request.get_json()
    
    sector  = request_data.get('sector')
    capitalization = request_data.get('capitalization', None)
    type_of_investment  = request_data.get('type_of_investment')
    
    if not sector:
        return jsonify({"error": "Please provide at least one sector"}), 400

    if not type_of_investment:
        return jsonify({"error": "Please provide at least one type_of_investment"}), 400

    result = dict()
    filter_data = dict()
    get_data = get_type_investment(type_of_investment, sector)
    
    for ticker, company, revenue, date in get_data:
        date_format = datetime.fromisoformat(date)
        data = { "company": company,"revenue": revenue,"date": date_format.strftime("%Y-%m-%d")}
        if ticker not in filter_data:
            filter_data[ticker] = []
        filter_data[ticker].append(data)
    result = filter_data
    return result, 200
    #return jsonify(result), 200


if __name__ == '__main__':
    app.run()