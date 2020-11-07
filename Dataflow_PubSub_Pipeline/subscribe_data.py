from google.cloud import pubsub

subscriber = pubsub.SubscriberClient()

topic = "data228-hw8-out"
subs = "hw8-out-sub"
project = "data228"


subs_path = subscriber.subscription_path(project, subs)
topic_path = pubsub.PublisherClient.topic_path(project, topic)

print("Listening to pub sub topic {}...\n".format(topic_path))

def callback(message):
    print(message.data)
    message.ack()


future = subscriber.subscribe(subs_path, callback)
try:
    future.result()
except KeyboardInterrupt:
    future.cancel()

