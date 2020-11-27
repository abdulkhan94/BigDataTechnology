from google.cloud import pubsub
from time import sleep
import datetime
import csv
import json
import random

publisher = pubsub.PublisherClient()

project = "data228-final"
topic = "data228-in"

topic_path = publisher.topic_path(project, topic)

json_dict = {}

with open('./source_data/aapl_streaming_apr.csv', 'r') as csvfile:
    spamreader = csv.reader(csvfile)
    next(spamreader)
    for row in spamreader:
        try:
            json_dict['ticker'] = row[0]
            json_dict['date'] = row[1]
            json_dict['open'] = float(row[2])
            json_dict['high'] = float(row[3])
            json_dict['low'] = float(row[4])
            json_dict['close'] = float(row[5])
            json_dict['adj_close'] = float(row[6])
            json_dict['volume'] = int(row[7])
            jstr = json.dumps(json_dict)
            print("Publishing {} to {} at {}...".format(jstr, topic_path, datetime.datetime.now()))
            publisher.publish(topic_path, bytes(jstr, "utf-8"))
            print(jstr)
            json_dict.clear()
            sleep(random.randint(1, 3))
        except IndexError:
            break
