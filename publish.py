import os
import time
from google.cloud import pubsub_v1

if __name__ == "__main__":

    service_account_path = os.environ.get("SERVICE_ACCOUNT_PATH")
    print("Service account file : ", service_account_path)
    pubsub_topic = os.environ.get("PUBSUB_TOPIC")
    project = os.environ.get("PROJECT")

    input_file = 'mobile_game.txt'

    # create publisher
    publisher = pubsub_v1.PublisherClient()

    with open(input_file, 'rb') as ifp:
        # skip header
        header = ifp.readline()

        # loop over each record
        for line in ifp:
            event_data = line   # entire line of input CSV is the message
            print('Publishing {0} to {1}'.format(event_data, pubsub_topic))
            publisher.publish(pubsub_topic, event_data)
            time.sleep(1) #publishes every 1 second
