#from google.cloud import pubsub
from time import sleep
import datetime
import csv
import json
import random

#publisher = pubsub.PublisherClient()

#project = "data228"
#topic = "data228-hw8-in"

#topic_path = publisher.topic_path(project, topic)

json_dict = {}

with open('./source_data/aapl_2020.csv', 'r') as csvfile:
    spamreader = csv.reader(csvfile)
    next(spamreader)
    for row in spamreader:
        try:
            json_dict['ticker'] = row[0]
            json_dict['date'] = row[1]
            json_dict['price'] = float(row[2])
            jstr = json.dumps(json_dict)
            #print("Publishing {} to {} at {}...".format(jstr, topic_path, datetime.datetime.now()))
            #publisher.publish(topic_path, bytes(jstr, "utf-8"))
            print(jstr)
            json_dict.clear()
            sleep(random.randint(1, 3))
        except IndexError:
            break


'''
publisher = pubsub.PublisherClient()

project = "data228"
topic = "data228-hw8-in"

topic_path = publisher.topic_path(project, topic)

json_dict = {}

with open('hw8_data.csv', 'r') as csvfile:
    spamreader = csv.reader(csvfile)
    next(spamreader)

    for row in spamreader:
        try:
            json_dict["id"] = int(row[0])
            json_dict["steps"] = int(row[1])
            jstr = json.dumps(json_dict)
            print("Publishing {} to {} at {}...".format(jstr, topic_path, datetime.datetime.now()))
            publisher.publish(topic_path, bytes(jstr, "utf-8"))
            json_dict.clear()
            sleep(5)
        except IndexError:
            break
'''
