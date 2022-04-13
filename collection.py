import pymongo
import requests


class Collection:
    def __init__(self, database_name: str, collection_name: str, client: str = 'mongodb://localhost:27017/', contents: list = []):
        self.db = pymongo.MongoClient(client)[database_name]
        self.collection = self.db[collection_name]
        self.collection_name = collection_name
        self.contents = contents

    def api_scrape(self, URI):
        page_number = 0
        while True:  # infinite loop, breaks when api call hits an error
            page_number += 1  # increase page number with each call, starts at 1
            page = requests.get(URI + "/?page={}".format(page_number))  # adds page number to URI
            if page.status_code == 404:
                break
            else:
                page = requests.get(URI + "/?page={}".format(page_number)).json()
                # for each dictionary in the list 'results', append dictionary to list contents
                for document in page['results']:
                    self.contents.append(document)
        return self.contents

    # change value of a field, to reference ObjectId of another document in another collection
    def reference(self, parent_collection, field: str):
        for document in self.contents:
            if not document[field]:  # if value is empty
                continue
            else:
                for index, URI in enumerate(document[field]):
                    field_info = requests.get(URI).json()  # retrieve document for value to reference
                    field_name = field_info['name']  # value of field 'name'

                    # search collection for corresponding document and retrieve ObjectId
                    object_id = parent_collection.collection.find_one({'name': field_name})['_id']
                    document[field][index] = object_id  # change value to ObjectId of referenced document

    def insert_collection(self):
        # if collection exists in database, delete existing collection
        if self.collection_name in self.db.list_collection_names():
            self.collection.drop()
        # loop through list, inserting each document into new collection
        for document in self.contents:
            self.collection.insert_one(document)
