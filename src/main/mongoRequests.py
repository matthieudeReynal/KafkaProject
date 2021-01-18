from pymongo import MongoClient
from bson.raw_bson import RawBSONDocument
import time

client = MongoClient('mongodb://localhost:27017/', document_class = RawBSONDocument).samples
collection = client.candles.candles

def getCandlesOnLastHour(): # print ids list of candles on last hour
   now = int(time.time())
   oneHour = 60*60
   myquery = collection.find({"time" : {"$gt" : ('"'+str(now - oneHour)+'"')}})
   for i in range (20):
           print(myquery[i]["_id"])

getCandlesOnLastHour()