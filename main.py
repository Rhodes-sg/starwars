import bson
import pymongo
import requests
from pprint import pprint


def api_scrape(link):

    i = 0
    collection_list = []

    while True:
        i += 1
        page = requests.get(link + "/?page={}".format(i))
        if page.status_code == 404:
            break
        else:
            page = requests.get(link + "/?page={}".format(i)).json()
            for document in range(0, len(page['results'])):
                collection_list.append(page['results'][document])
    return collection_list


starships_list = api_scrape("https://swapi.dev/api/starships")

db = pymongo.MongoClient()["starwars"]
characters = db.characters
starships = db.starships

if "starships" in db.list_collection_names():
    starships.drop()

for starship in starships_list:
    if not starship['pilots']:
        continue
    else:
        for index, pilot in enumerate(starship['pilots']):
            pilot_info = requests.get(pilot).json()
            pilot_name = pilot_info['name']
            pilot_id = characters.find_one({'name': pilot_name})['_id']
            starship['pilots'][index] = bson.ObjectId(pilot_id)

for document in starships_list:
    starships.insert_one(document)


