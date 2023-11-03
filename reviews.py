import tripadvisor_scraping.search as search
import csv
import argparse
import os
from tqdm import tqdm
import datetime
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys

parser = argparse.ArgumentParser(description='Description of your input arguments')
parser.add_argument('--start', type=int, default=0, help='Element to start calling API with')
parser.add_argument('--stop', type=int, default=0, help='Element to stop calling API with. Note that this element is not included to be called.')
parser.add_argument('--overwrite', type=bool, default=False, help='Indicates whether overwrite the existing file reviews.csv or not.')
args = parser.parse_args()

csv_details = 'data/details.csv'
csv_reviews = 'data/reviews.csv'

def read_csv(csv_file=csv_details) :
    list_of_hotels = []
    with open(csv_file, mode="r", newline="") as file:
        reader = csv.DictReader(file)
        # Skip the header row (if it exists)
        for row in reader :
            list_of_hotels.append(row)
    return list_of_hotels

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
            "n_elements": config_data['details']['n_elements'],
            "last_starting_at": config_data['details']['last_starting_at'],
            "last_stopping_at": config_data['details']['last_stopping_at']
        },
        "reviews": {
            "n_elements": total_files,
            "last_starting_at": start_at,
            "last_stopping_at": stop_at
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
    new_config['reviews']['last_stopping_at'] = stop_at
    print("Total number of calls for this execution: " + str(n_calls) + ".")
    
    return start_at, stop_at, n_calls, new_config

def scrape_data(start_at, stop_at, n_calls, search_data) :
    results = []
    with tqdm(total=n_calls, unit='hotel') as pbar:

        # Accede a la lista de elementos dentro de "data" y obtén el valor de "location_id"
        for element in range(start_at, stop_at):
            location_id = search_data[element]['location_id']
            if location_id is not None:
                reviews_json_data = search.location_reviews(search._key, location_id)
                # Extract values of "id", "title", and "text" for each element in the "data" array
                if reviews_json_data :
                    for item in reviews_json_data['data']:
                        if "bug" in item['title'] or "bug" in item['text'] :
                            result = {
                                "status": "",
                                "location_id": location_id,
                                "id": item['id'],
                                "title": item['title'],
                                "text": item['text'],
                                "review_url": item['url'],
                                "name": search_data[element]['name'],
                                "website": search_data[element]['website'],
                                "web_url": search_data[element]['web_url'],
                                "street1": search_data[element]['street1'],
                                "street2": search_data[element]['street2'],
                                "city": search_data[element]['city'],
                                "state": search_data[element]['state'],
                                "country": search_data[element]['country'],
                                "postcode": search_data[element]['postcode'],
                                "email": search_data[element]['email'],
                                "phone": search_data[element]['phone'],
                                "website": search_data[element]['website'],
                                "rating": search_data[element]['rating'],
                                "num_reviews": search_data[element]['num_reviews'],
                                "price_level": search_data[element]['price_level']
                            }
                            results.append(result)
            # Actualiza la barra de progreso
            pbar.update(1)
    return results

def update_reviews(matched_reviews, csv_file=csv_reviews) :
    headers = ["status", "id", "title", "text", "review_url", "location_id", "name", "web_url", "street1", "street2", "city", "state", "country", "postcode", "email", "phone", "website", "rating", "num_reviews", "price_level"]

    # Check if the CSV file exists
    file_exists = os.path.exists(csv_file)
    existing_reviews = []
    if file_exists and not args.overwrite :
        existing_reviews = read_csv(csv_reviews)
        repeated_indexes = []        
        for ereview in existing_reviews :
            ereview['status'] = "OLD"
            for mreview in matched_reviews :
                if str(ereview['id']) == str(mreview['id']) :
                    ereview['status'] = "REPEATED"
                    repeated_indexes.append(matched_reviews.index(mreview))
        for mreview in matched_reviews :
            if matched_reviews.index(mreview) not in repeated_indexes :
                mreview['status'] = "NEW"
                existing_reviews = [mreview] + existing_reviews
    else :
        for mreview in matched_reviews :
            mreview['status'] = "NEW"
            existing_reviews += [mreview]
    # Open the CSV file in writing
    with open(csv_file, "w", newline="") as csv_file:
        headers = existing_reviews[0].keys()  # Get the column headers from the first dictionary
        writer = csv.DictWriter(csv_file, fieldnames=headers)
        # Write the headers to the CSV file
        writer.writeheader()
        # Write the data from each dictionary to the CSV file
        for review in existing_reviews :
            writer.writerow(review)


""" Check for conditional execution """
if __name__ == "__main__":
    """ Code to be executed if you run the script directly """
    """ Open the search.json file for reading """
    search_data = read_csv(csv_details)

    """ Open and read the configuration JSON file """
    config_data = search.read_json(search.json_config)

    """ Get the value of the parameters for the scraping """
    total_files = len(search_data)
    start_at, stop_at, n_calls, new_config = get_scraping_params(config_data, total_files)

    """ Get the reviews containing the work "bugs" """
    reviews = scrape_data(start_at, stop_at, n_calls, search_data)

    """ Update the reviews.csv file with the data scraped """
    update_reviews(reviews)

    """ Get the number of reviews contained in data/reviews.csv """
    data = read_csv(csv_reviews)
    n_search = len(data)

    """ Update config.json with the new parameters after the scraping """
    search.write_json(new_config, search.json_config)


    print("DONE!")
    print("Number of reviews added: " + str(len(reviews)))
    print("Total number of reviews in data/reviews.csv: " + str(n_search))