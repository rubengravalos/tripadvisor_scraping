import search
import reviews
import os
import csv
from tqdm import tqdm
import argparse
import datetime
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys

parser = argparse.ArgumentParser(description='Description of your input arguments')
parser.add_argument('--start', type=int, default=0, help='Element to start calling API with')
parser.add_argument('--stop', type=int, default=0, help='Element to stop calling API with. Note that this element is not included to be called.')
parser.add_argument('--overwrite', type=str, default="YES", help='Indicates whether overwrite the existing file details.csv or not.')
args = parser.parse_args()

""" .scv file name to store hotels' details """
csv_file = 'data/details.csv'


def get_scraping_params (config_data, total_files) :
    start_at = args.start
    stop_at = args.stop
    n_calls = stop_at - start_at
    current_date = datetime.today()
    if config_data.get("first_call_at") != "" :
        starting_date = datetime.strptime(config_data.get("first_call_at"), "%Y-%m-%d")
    else :
        starting_date = datetime.today()
    day_diff = (current_date - starting_date).days
    max_n_calls = config_data.get("max_n_calls")

    """ New data to be written over configuratoin JSON file """
    new_config = {
        "max_n_calls": config_data['max_n_calls'],
        "done_calls": config_data['done_calls'],
        "first_call_at": starting_date.strftime('%Y-%m-%d'),
        "last_call_at": datetime.today().strftime('%Y-%m-%d'),
        "search": {
            "n_elements": config_data['search']['n_elements'],
            "last_starting_at": config_data['search']['last_starting_at'],
            "last_stopping_at": config_data['search']['last_stopping_at']
        },
        "details": {
            "n_elements": total_files,
            "last_starting_at": start_at,
            "last_stopping_at": stop_at
        },
        "reviews": {
            "n_elements": config_data['reviews']['n_elements'],
            "last_starting_at": config_data['reviews']['last_starting_at'],
            "last_stopping_at": config_data['reviews']['last_stopping_at']
        }
    }

    """ Check if there have been passed more than a month since first call (Is the period to reset the number of free calls available for the user) """
    if day_diff <= 30 :
        done_calls = config_data.get("done_calls")
    else:
        done_calls = 0
        new_config['first_call_at'] = datetime.today().strftime('%Y-%m-%d')

    """ Restrict the number of calls to be done matching the remaining free calls available for the user within the current period """
    remaining_calls = max_n_calls - done_calls
    if n_calls > remaining_calls :
        n_calls = remaining_calls
        if n_calls <= 0 :
            date_of_reset = starting_date + relativedelta(months=1) # Add one month
            print("You cannot make any more free API calls until " + date_of_reset.strftime('%Y-%m-%d') + ".")
            sys.exit()
        stop_at = start_at + n_calls
        print("It exceeds the maximum number of calls per month, stopping element modified to " + str(stop_at) + ".")
    new_config['done_calls'] += n_calls
    new_config['details']['last_stopping_at'] = stop_at
    print("Total number of calls for this execution: " + str(n_calls) + ".")
    
    return start_at, stop_at, n_calls, new_config

def overwrite (csv_file=csv_file) :
    headers = ["location_id", "name", "web_url", "street1", "street2", "city", "state", "country", "postcode", "email", "phone", "website", "rating", "num_reviews", "price_level"]
    with open(csv_file, "w", newline="") as csv_file:
        # Create a DictWriter object
        writer = csv.DictWriter(csv_file, fieldnames=headers)
        writer.writeheader()

def scrape_data(start_at, stop_at, n_calls, search_data) :

    with tqdm(total=n_calls, unit='hotel') as pbar:

        """ Access the list of elements inside "data" and get the value of location_id """
        for element in range(start_at, stop_at):
            location_id = search_data[element].get("location_id")
            if location_id is not None:
                details_json_data = search.location_details(search._key, location_id)
                if details_json_data is not None :
                  # Extrae los campos requeridos
                  filtered_data = [{
                      "location_id": details_json_data.get("location_id", ""),
                      "name": details_json_data.get("name", ""),
                      "web_url": details_json_data.get("web_url", ""),
                      "street1": details_json_data.get("address_obj", {}).get("street1", ""),
                      "street2": details_json_data.get("address_obj", {}).get("street2", ""),
                      "city": details_json_data.get("address_obj", {}).get("city", ""),
                      "state": details_json_data.get("address_obj", {}).get("state", ""),
                      "country": details_json_data.get("address_obj", {}).get("country", ""),
                      "postcode": details_json_data.get("address_obj", {}).get("postalcode", ""),
                      "email": details_json_data.get("email", ""),
                      "phone": details_json_data.get("phone", ""),
                      "website": details_json_data.get("website", ""),
                      "rating": details_json_data.get("rating", ""),
                      "num_reviews": details_json_data.get("num_reviews", ""),
                      "price_level": details_json_data.get("price_level", "")
                  }]

                  # Procesa y almacena los datos JSON
                  process_and_store_data(filtered_data, csv_file=csv_file)

            else:
                print("No se encontró 'location_id' en este elemento.")

            # Actualiza la barra de progreso
            pbar.update(1)

def process_and_store_data(data_json, csv_file = csv_file):
    # Check if the CSV file exists
    file_exists = os.path.exists(csv_file)
    # File mode for opening the CSV
    mode = "a" if file_exists else "w"
    # Open the CSV file in writing
    with open(csv_file, mode, newline="") as csv_file:
        # Create a DictWriter object
        fields = data_json[0].keys()  # Using keys from the first dictionary as headers
        writer = csv.DictWriter(csv_file, fieldnames=fields)

        # If the file is new, write the headers
        if not file_exists:
            writer.writeheader()

        # Write each data row
        for row in data_json:
            writer.writerow(row)

""" Check for conditional execution """
if __name__ == "__main__":
    """ Code to be executed if you run the script directly """
    """ Open the search.json file for reading """
    search_data = search.read_json(search.json_search)

    """ Open and read the configuration JSON file """
    config_data = search.read_json(search.json_config)

    """ Get the value of the parameters for the scraping """
    total_files = len(search_data)
    start_at, stop_at, n_calls, new_config = get_scraping_params(config_data, total_files)

    """ Create an empty .csv file if args.overwrite is True """
    if args.overwrite :
        overwrite()

    """ Call the API for any location_id in search.json and get the location details of each hotel """
    scrape_data(start_at, stop_at, n_calls, search_data)

    """ Get the number of hotels contained in data/details.csv """
    data = reviews.read_csv(csv_file)
    n_search = len(data)
    new_config['reviews']['n_elements'] = n_search

    """ Update config.json with the new parameters after the scraping """
    search.write_json(new_config, search.json_config)

    print("DONE!")
    #print("Number of hotels' details added: " + str(n_calls))
    print("Total number of hotels in data/details.csv: " + str(n_search))