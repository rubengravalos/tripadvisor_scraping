from tqdm import tqdm
import requests
import json
import argparse
import datetime
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys

parser = argparse.ArgumentParser(description='Description of your input arguments')
parser.add_argument('--start', type=int, default=0, help='Element to start calling API with')
parser.add_argument('--stop', type=int, default=0, help='Element to stop calling API with. Note that this element is not included to be called.')
parser.add_argument('--overwrite', type=str, default="YES", help='Indicates whether overwrite the existing file search.json or not.')
args = parser.parse_args()

_key = "BC073290243D4F699FDBC9BA4204A72C"

json_dataset = 'data/lga-scc-pairs.json'
json_search = 'data/search.json'
json_config = 'config.json'

def location_search (key, searchQuery, address):
    """
    INPUT PARAMS
    - key : The Partner API Key.
    - searchQuery : Text to use for searching based on the name of the location.
    
    OUTPUT FORMAT
    {
        "data": [
            {
            "location_id": 0,
            "name": "string",
            "distance": "string",
            "rating": "string",
            "bearing": "string",
            "address_obj": {
                "street1": "string",
                "street2": "string",
                "city": "string",
                "state": "string",
                "country": "string",
                "postalcode": "string",
                "address_string": "string",
                "phone": "string",
                "latitude": 0,
                "longitude": 0
            }
            }
        ],
        "error": {
            "message": "string",
            "type": "string",
            "code": 0
        }
    }
    """
    
    _searchQuery = searchQuery.replace(" ", "%20")
    _address = address.replace(" ", "%20")

    url = "https://api.content.tripadvisor.com/api/v1/location/search?key=" + str(key) + "&searchQuery=hotel%20" + _searchQuery + "&category=hotels&address=" + _address + "&language=en"
    headers = {"accept": "application/json"}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            # La respuesta es exitosa, puedes cargar los datos JSON
            return response.json()
        else:
            # La solicitud no fue exitosa; muestra el código de estado
            print(f"Couldn't obtainig data from Tripadvisor API. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        # Ocurrió un error en la solicitud
        print(f"Error en la solicitud a la API: {e}")

    except Exception as e:
        # Maneja otras excepciones que puedan ocurrir
        print(f"Ocurrió un error inesperado: {e}")

def location_details (key, locationId):
    """
    INPUT PARAMS
    - key : The Partner API Key.
    - locationId : A unique identifier for a location on Tripadvisor. The location ID can be obtained using the Location Search.
    
    OUTPUT FORMAT
    {
        "location_id": 0,
        "name": "string",
        "description": "string",
        "web_url": "string",
        "address_obj": {
            "street1": "string",
            "street2": "string",
            "city": "string",
            "state": "string",
            "country": "string",
            "postalcode": "string",
            "address_string": "string"
        },
        "ancestors": [
            {
            "abbrv": "string",
            "level": "string",
            "name": "string",
            "location_id": 0
            }
        ],
        "latitude": 0,
        "longitude": 0,
        "timezone": "string",
        "email": "string",
        "phone": "string",
        "website": "string",
        "write_review": "string",
        "ranking_data": {
            "geo_location_id": 0,
            "ranking_string": "string",
            "geo_location_name": "string",
            "ranking_out_of": 0,
            "ranking": 0
        },
        "rating": 0,
        "rating_image_url": "string",
        "num_reviews": "string",
        "review_rating_count": {
            "additionalProp": "string"
        },
        "subratings": {
            "additionalProp": {
            "name": "string",
            "localized_name": "string",
            "rating_image_url": "string",
            "value": 0
            }
        },
        "photo_count": 0,
        "see_all_photos": "string",
        "price_level": "string",
        "hours": {
            "periods": [
            {
                "open": {
                "day": 0,
                "time": "string"
                },
                "close": {
                "day": 0,
                "time": "string"
                }
            }
            ],
            "weekday_text": [
            "string"
            ],
            "subcategory": [
            {
                "name": "string",
                "localized_name": "string"
            }
            ]
        },
        "amenities": [
            "string"
        ],
        "features": [
            "string"
        ],
        "cuisine": [
            {
            "name": "string",
            "localized_name": "string"
            }
        ],
        "parent_brand": "string",
        "brand": "string",
        "category": {
            "name": "string",
            "localized_name": "string"
        },
        "subcategory": [
            {
            "name": "string",
            "localized_name": "string"
            }
        ],
        "groups": [
            {
            "name": "string",
            "localized_name": "string",
            "categories": [
                {
                "name": "string",
                "localized_name": "string"
                }
            ]
            }
        ],
        "styles": [
            "string"
        ],
        "neighborhood_info": [
            {
            "location_id": "string",
            "name": "string"
            }
        ],
        "trip_types": [
            {
            "name": "string",
            "localized_name": "string",
            "value": "string"
            }
        ],
        "awards": [
            {
            "award_type": "string",
            "year": 0,
            "images": {
                "tiny": "string",
                "small": "string",
                "large": "string"
            },
            "categories": [
                "string"
            ],
            "display_name": "string"
            }
        ],
        "error": {
            "message": "string",
            "type": "string",
            "code": 0
        }
    }
    """
    
    url = "https://api.content.tripadvisor.com/api/v1/location/" + str(locationId) + "/details?key=" + str(key) + "&key=" + str(key) + "&language=en&currency=AUD"
    headers = {"accept": "application/json"}

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            # La respuesta es exitosa, puedes cargar los datos JSON
            return response.json()
        else:
            # La solicitud no fue exitosa; muestra el código de estado
            print(f"Error al obtener datos. Código de estado: {response.status_code}")
    except requests.exceptions.RequestException as e:
        # Ocurrió un error en la solicitud
        print(f"Error en la solicitud a la API: {e}")

    except Exception as e:
        # Maneja otras excepciones que puedan ocurrir
        print(f"Ocurrió un error inesperado: {e}")

def location_reviews (key, locationId):
    """
    INPUT PARAMS
    - key : The Partner API Key.
    - locationId : A unique identifier for a location on Tripadvisor. The location ID can be obtained using the Location Search.
    
    OUTPUT FORMAT
    {
        "data": [
            {
            "id": 0,
            "lang": "string",
            "location_id": 0,
            "published_date": "string",
            "rating": 0,
            "helpful_votes": 0,
            "rating_image_url": "string",
            "url": "string",
            "trip_type": "string",
            "travel_date": "string",
            "text": "string",
            "title": "string",
            "owner_response": "string",
            "is_machine_translated": true,
            "user": {
                "username": "string",
                "user_location": {
                "name": "string",
                "id": "string"
                },
                "review_count": 0,
                "reviewer_badge": "string",
                "avatar": {
                "additionalProp": "string"
                }
            },
            "subratings": {
                "additionalProp": {
                "name": "string",
                "localized_name": "string",
                "rating_image_url": "string",
                "value": 0
                }
            }
            }
        ],
        "error": {
            "message": "string",
            "type": "string",
            "code": 0
        }
    }
    """
    
    url = "https://api.content.tripadvisor.com/api/v1/location/" + str(locationId) + "/reviews?key=" + str(key) + "&key=" + str(key) + "&language=en"
    headers = {"accept": "application/json"}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            # La respuesta es exitosa, puedes cargar los datos JSON
            return response.json()
        else:
            # La solicitud no fue exitosa; muestra el código de estado
            print(f"Error al obtener datos. Código de estado: {response.status_code}")
    except requests.exceptions.RequestException as e:
        # Ocurrió un error en la solicitud
        print(f"Error en la solicitud a la API: {e}")

    except Exception as e:
        # Maneja otras excepciones que puedan ocurrir
        print(f"Ocurrió un error inesperado: {e}")

def read_json (file) :
    with open(file, 'r') as archivo_json:
        data = json.load(archivo_json)
    return data

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
            "n_elements": total_files,
            "last_starting_at": start_at,
            "last_stopping_at": stop_at
        },
        "details": {
            "n_elements": config_data['details']['n_elements'],
            "last_starting_at": config_data['details']['last_starting_at'],
            "last_stopping_at": config_data['details']['last_stopping_at']
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
    new_config['search']['last_stopping_at'] = stop_at
    print("Total number of calls for this execution: " + str(n_calls) + ".")
    
    return start_at, stop_at, n_calls, new_config

def scrape_data (start_at, stop_at, n_calls, pairs) :
    all_data_scraped = []
    n_elements_added = 0
    with tqdm(total=n_calls, unit='call') as pbar:
        for element in range(start_at, stop_at):
            search_json_data = location_search(_key, pairs[element]['scc'], pairs[element]["lga"])

            # Extracting the 'data' key
            data_list = search_json_data.get('data', [])

            response_data = []
            for item in data_list :
                # Creating a new list with the selected items
                if item :
                    response_data = [
                        {
                            'location_id': item.get('location_id', ""),
                            'name': item.get('name', ""),
                            'address_obj': {
                                'city': item['address_obj'].get('city', ""),
                                'state': item['address_obj'].get('state', ""),
                                'country': item['address_obj'].get('country', ""),
                                'address_string': item['address_obj'].get('address_string', "")
                            }
                        }
                    ]
                    all_data_scraped.append(response_data[0])   
                    n_elements_added += 1

            # Update the progress bar
            pbar.update(1)
    return all_data_scraped, n_elements_added

def write_json (data, json_file) :
    with open(json_file, 'w') as json_file:
        json.dump(data, json_file, indent=4)

def write_hotel_locations (data) :
    if args.overwrite == "NO" :
        # Read the existing JSON data from the file
        existing_data = read_json(json_search)
        # Append all new data to the existing data contained in search.json file
        existing_data += data
        write_json(existing_data, json_search)
    else :
        write_json(data, json_search)

""" Check for conditional execution """
if __name__ == "__main__":
    """ Code to be executed if you run the script directly """
    
    """ 
    Read all data pairs of 'lga_names' and 'scc_names' from .json file dataset 
    - LGA : Local Government Area
    - SCC : State Suburbs
    """
    data = read_json(json_dataset)

    """ Open and read the configuration JSON file """
    config_data = read_json(json_config)

    """ Get the value of the parameters for the scraping """
    total_files = len(data)
    start_at, stop_at, n_calls, new_config = get_scraping_params(config_data, total_files)

    """ Call the API pair by pair of [scc_names, lga_names] and get the location data of the hotels found for each pair """
    all_data_scraped, n_elements_added = scrape_data(start_at, stop_at, n_calls, data)

    """ Write the new data scraped into search.json file """
    write_hotel_locations(all_data_scraped)

    """ Get the number of hotels contained in data/search.json """
    data = read_json(json_search)
    n_search = len(data)
    new_config['details']['n_elements'] = n_search

    """ Update config.json with the new parameters after the scraping """
    write_json(new_config, json_config)


    print("DONE!")
    print("Number of hotels added: " + str(n_elements_added))
    print("Total number of hotels in data/search.json: " + str(n_search))