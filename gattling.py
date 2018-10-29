import json
import random
import string
import time

from google.cloud import pubsub

PROJECT = "zuhlkecloudhack"
TOPIC= "flight_messages"

N = 50
flight_numbers = ["1", "2", "3", "4", "5"]


def get_message(N=10):
    return  ''.join(random.choice(string.ascii_uppercase + string.digits + " ") for _ in range(N))


def run():
    publisher = pubsub.PublisherClient()
    topic = publisher.topic_path(PROJECT, TOPIC)
    publisher.get_topic(topic)

    cnt=1
    t0 = time.time()
    lim = 100
    while True:
        publish(publisher, topic)
        time.sleep(0.01)

        if cnt % lim == 0:
            t1 = time.time()
            diff = t1 -t0
            print("Msg / sec: %s"%(lim * 1. / diff))
            cnt = 0
            t0 = t1
        cnt += 1


def publish(publisher, topic):
    msg = {
        "flight-number": random.choice(flight_numbers),
        "message": get_message(N),
        "message-type": "INFO",
        "timestamp": "2012-04-23T18:25:43.511Z"
    }
    publisher.publish(topic, json.dumps(msg).encode())


if __name__ == '__main__':
    run()