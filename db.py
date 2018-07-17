# # -*- coding: utf-8 -*-
"""
CURD function for MongoDB and FirebaseDataBase
Author: Hein Htet Naing (Hector) , 2018 
"""

from pymongo import MongoClient
import pyrebase
import time


class MongoDB(object):
    """
    host: mongodb host eg. 'mongodb://localhost:27017/'
    """

    def __init__(self, host, db_name):
        self.client = MongoClient(str(host))
        self.db = self.client[str(db_name)]

    def insert(self, collection_name, data):
        collection = self.db[str(collection_name)]
        collection.insert(data)

    def update(self, collection_name, update_on={}, data={}):
        """ To update existing row, insert new value
        and adding new attribute
        """
        collection = self.db[str(collection_name)]
        collection.update(update_on, {"$set": data})

    def find(self, collection_name, no_limit=0):
        """ Get one whole collection(table)"""
        collection = self.db[str(collection_name)]
        result = []
        for d in collection.find().limit(int(no_limit)):
            result.append(d)
        return result

    def find_one(self, collection_name):
        """ Get one document(row) , return the first record"""
        collection = self.db[str(collection_name)]
        result = collection.find_one()

    def find_last_doc(self, collection_name, sort_on):
        """ Get last document (row)"""
        result = []
        collection = self.db[str(collection_name)]
        cursor = collection.find().sort([(str(sort_on), -1)]).limit(1)
        for c in cursor:
            result.append(c)
        return result

    def find_att(self, collection_name, att=[]):
        """ Get att array from a collection
        att = array of attributes name
        """
        query = {}
        for a in att:
            query[str(a)] = 1

        collection = self.db[str(collection_name)]
        collection = collection.find({}, query)
        result = []
        for c in collection:
            result.append(c)
        return result

    def remove(self, collection_name):
        """ to remove one collection"""
        collection = self.db[str(collection_name)]
        collection.remove()


class FireDB(object):
    def __init__(self):
        self.config = {
            # paste your config from firebase here
        }
        self.firebase = pyrebase.initialize_app(self.config)
        self.db = self.firebase.database()

    def insert(self, data={}, path=' '):
        time_now = time.strftime("%d-%m-%y-%T", time.localtime())
        data['time'] = time_now
        if(path == ' '):
            self.db.child().child(time_now).set(data)
        if(path != ' '):
            self.db.child(uid).child(time_now).set(data)

    def get(self, path=''):
        data = []
        result = self.db.child(path).get()
        for r in result.each():
            data.append(r.val())
        return data

    def remove(self, path=''):
        self.db.child(path).remove()


if __name__ == "__main__":
    data = {
        "points": "[[1,2,3,4],[1,3,4,5],[1,3,4,5,5]]"
    }
    firebase = FireDB()
    path = 'detection/17-07-18-14:24:08'
    firebase.insert(uid= path,data =data)
    print(firebase.get(path)[0]['points'].split(']'))
    firebase.remove(path)
