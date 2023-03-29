import json
import requests
import datetime
import pymongo
import os

# Code author: Tyler Watson
# ISMG 6020 Assignment 1 (Final Submission)
# Spring 2023
# 2/13/2023
# All code is original

# set the working directory
os.chdir(r'# hidden #')

# make a connection to the mongo client
client = pymongo.MongoClient("## hidden ##")

# select the database and collection names
db = client['co_labor']
collection = db['co_labor_20230213']

# base url to send the API request to
data_url =  "https://data.colorado.gov/resource/cjkq-q9ih.json"

# list of the three selected industry
# three different industries to compared to the previous submission of the assignment
ind_codes = [1022, 518, 1025]

# -------------------------------------------------------------------#

# begin functional programming 
def get_data(ind_codes, data_url, collection):
    # empty dictionary to hold the api responses for each code
    ind_code_response = {}

    # for loop to iterate through codes in list
    for code in ind_codes:
        # dictionary to store the request parameters
        # annual data for Denver County where ownership = 00 (aggregate of all types), starting with the most recent year
        api_query = {"areatyname":"County",
        "areaname":"Denver County",
        "periodtype":"1",
        "indcode":f"{code}",
        "ownership": "00",
        "$limit": 1000,
        "$order":"-periodyear"}

        # get request to retrieve parameters from url
        data_request = requests.get(data_url, api_query)

        # if the request code is 200 (OK) then print a success message
        if data_request.status_code == 200:
            print(f'Connection for industry code {code} was successful!')

            # send the request parameters for each ind_code
            ind_code_response[code] = data_request.json()

            # write the json to a txt file
            with open(f"{code}.txt","w") as file:
                file.write(json.dumps(data_request.json()))

            # insert the data into the mongo collection
            collection.insert_many(data_request.json())

            # print out the first record for each ind_code as a preview of the data
            print('\n')
            print(f'{code} | {data_request.json()[0]}')
            print('\n')
                
        # else if the status code != 200, print an error message along with the URL causing the error
        else:
            print(f'Error code: {data_request.status_code} | URL: {data_url}')

    return ind_code_response

# -------------------------------------------------------------------#

def get_records_count(collection):
    # mongodb query to sum up the count of records associated with each ind_code
    records_count =  [{"$group": {"_id": "$indcode", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}]

    # aggregate the results
    result = collection.aggregate(records_count) 

    # return the count of records for each ind_code
    return result

# -------------------------------------------------------------------#

# call the functions
ind_code_response = get_data(ind_codes, data_url, collection)
result = get_records_count(collection)

# print out the count of records populated for each ind_code
print('\n')
for res in result:
    print(f'Indcode: {res["_id"]} | Count: {res["count"]}')
