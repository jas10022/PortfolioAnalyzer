# API: et_tickers_strike
## URL:  http://localhost:5000/get_tickers_strike
## Method: [POST]
## Case1:
{
    "tickers": ["AA", "GOOGL"],
    "start_date": "2011-01-06",
    "end_date": "2014-01-06",
    "mode": "daily",
    "range_mode": ""
}
## Case2:
{
    "tickers": ["AA", "GOOGL"],
    "start_date": "",
    "end_date": "",
    "mode": "daily",
    "range_mode": ""
}
## Case3:
{
    "tickers": ["AA", "GOOGL"],
    "start_date": "",
    "end_date": "",
    "mode": "monthly",
    "range_mode": ""
}
## Case4:
{
    "tickers": ["AA", "GOOGL"],
    "start_date": "",
    "end_date": "",
    "mode": "monthly",
    "range_mode": "2"
}
## Case5:
{
    "tickers": ["AA", "GOOGL"],
    "start_date": "",
    "end_date": "",
    "mode": "yearly",
    "range_mode": "2"
}
## Case6:
{
    "tickers": ["AA", "GOOGL"],
    "start_date": "",
    "end_date": "",
    "mode": "yearly",
    "range_mode": ""
}