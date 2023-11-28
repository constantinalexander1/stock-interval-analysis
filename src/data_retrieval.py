# Alpha vantage API endpoint for market data retrieval
import os
from dotenv import load_dotenv
import requests
import csv
from pathlib import Path
from datetime import date
import json
import time

load_dotenv()


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def store_data(symbol: str, csv_list: [[str]]):
    print(f"    Writing {symbol} data", end="...")

    filename = (symbol + "_" + str(date.today()) + ".csv")
    current_path_raw = os.path.dirname(__file__)
    current_path = Path(current_path_raw)

    target_path = str(current_path.parent) + "/sample_data/" + filename


    header = ["date", "open", "high", "low", "close", "volume"]
    with open(target_path, 'w', encoding='UTF8') as file:

        writer = csv.writer(file)
        # write the header
        writer.writerow(header)

        # write the data
        writer.writerows(csv_list)

    print(f"{bcolors.OKGREEN}OK{bcolors.ENDC}")


# returns dictionary with alpha vantage info
def query_alpha_vantage(search_string: str, symbol: str, full_output: bool) -> dict:
    print(f"    Quering Alpha Vantage", end="...")

    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    
    outputsize = ""
    if full_output:
        outputsize = "&outputsize=full"

    url = "https://www.alphavantage.co/query?" + search_string + "&symbol=" + symbol + "&interval=1min" + outputsize + "&apikey=" + api_key
    r = requests.get(url)
    json_data = r.json()

    print(f"{bcolors.OKGREEN}OK{bcolors.ENDC}")

    return json_data
   
# returns pair: (symbol, [[date, open, high, low, close, volume]])
def parse_1min_interval_data(result_dictionary: dict) -> (str, [[str]]):
    print(f"    Parsing API response", end="...")
    meta_data_dict = result_dictionary["Meta Data"]
    time_series_dict = result_dictionary["Time Series (1min)"]

    time_series_list = list(time_series_dict.items())

    csv_list = []

    for item in time_series_list:
        row = []
        row.append(item[0]) # date
        row.append(item[1]["1. open"])
        row.append(item[1]["2. high"])
        row.append(item[1]["3. low"])
        row.append(item[1]["4. close"])
        row.append(item[1]["5. volume"])

        csv_list.append(row)


    print(f"{bcolors.OKGREEN}OK{bcolors.ENDC}")

    return (meta_data_dict["2. Symbol"], csv_list)


def get_stock_intraday(ticker_symbol):
    search_string = "function=TIME_SERIES_INTRADAY"
    json_dict = query_alpha_vantage(search_string, ticker_symbol, True)
    _, csv_list = parse_1min_interval_data(json_dict)        
    store_data(ticker_symbol, csv_list)



def main():
    print("\nRetrieving market data. Results will be stored in 'sample_data' folder...")
    get_stock_intraday("IBM")
    


# Call entire script via console
# TODO: Add command line arguments for detailed execution
if __name__ == "__main__":
    main()



