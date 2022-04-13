import pymongo
import requests
import bson


class Collection:
    def __init__(self, database_name: str, collection_name: str, client: str = 'mongodb://localhost:27017/', contents: list = []):
        self.db = pymongo.MongoClient(client)[database_name]
        self.collection = self.db[collection_name]
        self.collection_name = collection_name
        self.contents = contents

    def api_scrape(self, URI):
        i = 0

        while True:
            i += 1
            page = requests.get(URI + "/?page={}".format(i))
            if page.status_code == 404:
                break
            else:
                page = requests.get(URI + "/?page={}".format(i)).json()
                for document in range(0, len(page['results'])):
                    self.contents.append(page['results'][document])
        return self.contents

    def reference(self, parent_collection, field: str):
        for document in self.contents:
            if not document[field]:
                continue
            else:
                for index, URI in enumerate(document[field]):
                    field_info = requests.get(URI).json()
                    field_name = field_info['name']
                    object_id = parent_collection.collection.find_one({'name': field_name})['_id']
                    document[field][index] = bson.ObjectId(object_id)

    def insert_collection(self):
        if self.collection_name in self.db.list_collection_names():
            self.collection.drop()
        for document in self.contents:
            self.collection.insert_one(document)
