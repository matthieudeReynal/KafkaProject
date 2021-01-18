import json
from kafka import KafkaConsumer
from kafka import KafkaProducer
import time
import datetime

consumer = KafkaConsumer("smaSample",
     bootstrap_servers='localhost:9092',
     group_id="candles-analysis",
     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
     )

producer = KafkaProducer(bootstrap_servers="localhost:9092")

tradingThreshold = 1.01
for message in consumer:
     message = message.value
     # we convert epoch time to iso time format
     date = datetime.datetime.fromtimestamp(message[0]).strftime('%c')
     
     # according sma we tell the user to long or short
     if message[2] >= tradingThreshold * message[1]:
          print(date," : short")
     if message[2] <= tradingThreshold * message[1] and message[2] > 0:
          print(date, " : long") 
