import json
from kafka import KafkaConsumer
from kafka import KafkaProducer
from pymongo import MongoClient
import bsonjs
from bson.raw_bson import RawBSONDocument

consumer = KafkaConsumer("candles2",
     bootstrap_servers='localhost:9092',
     group_id="candles-analysis",
     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
     )
producer = KafkaProducer(bootstrap_servers="localhost:9092")

# define Mongo DB base
client = MongoClient('mongodb://localhost:27017/', document_class = RawBSONDocument).samples
collection = client.candles.candles

def candlesFormat(data): # give to data the format of a dictionnary
        tmp = {}
        tmp["time"] = str(data[0])
        tmp["low"] = str(data[1])
        tmp["high"] = str(data[2])
        tmp["open"] = str(data[3])
        tmp["close"] = str(data[4])
        tmp["volume"] = str(data[5])
        return (tmp)


def docAlreadyPresent(doc): # verify if a tuple is already present or not in the database
   presence = False
   mycol = collection
   myquery = { "time": doc["time"] }
   if(mycol.find_one(myquery) != None):
           presence = True
   return presence

# we will need to iterate on previous rows to define needed indicators
# so we stock all messages in a list
# this is probably not optimized
messages = []
i = 0 
for message in consumer:
    # get message
    messages.append(message.value)
    print(message.value)
    message = message.value
    candleTime = message[0]

    # insert into MongoDB
    json_record = candlesFormat(message)
    j = json.dumps(json_record)
    raw_bson = bsonjs.loads(j)
    bson_record = RawBSONDocument(raw_bson)
    if docAlreadyPresent(json_record) == False:
        result = collection.insert_one(bson_record)
    print('{} added to {}'.format(message, collection))

    close = message[4]
    time = message[0]

    # compute trend indicator
    if i == 0:
        trend = 0
    if i >0:
        if close> messages[i-1][4]:
            trend = 1
        if close < messages[i-1][4]:
            trend = -1
        else :
            trend = 0
    strategyTrendInfos = [time, close, trend]
    # insert message into new producer
    producer.send("trendSample", json.dumps(strategyTrendInfos).encode())
    
    # compute sma on 90 candles
    if i < 90:
        sma = 0
    elif i >= 90:
        sma = 0
        for j in range(i-90, i):
            sma += messages[j][4]
        sma /= 90
    strategyMeanInfos = [time, close, sma] 
    # insert message into new consumer 
    producer.send("smaSample", json.dumps(strategyMeanInfos).encode())
    i = i + 1
   