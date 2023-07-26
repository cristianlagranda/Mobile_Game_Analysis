from google.cloud import pubsub_v1
import time
import os

if __name__ == "__main__":


    service_account_path = os.environ.get("SERVICE_ACCOUNT_PATH")
    print("Service account file : ", service_account_path)
    subscription_path = os.environ.get("SUBSCRIPTION_PATH")

    subscriber = pubsub_v1.SubscriberClient()

    def callback(message):
        print(('Received message: {}'.format(message)))
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)

    while True:
        time.sleep(60)
