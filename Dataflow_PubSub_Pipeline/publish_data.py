from google.cloud import pubsub 
from time import sleep
import random
import datetime
import csv
import json


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
            publisher.publish(topic_path, bytes(jstr,"utf-8"))
            json_dict.clear()
            sleep(5)
        except IndexError:
            break

