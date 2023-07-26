import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam import window
from apache_beam.transforms.trigger import AccumulationMode, AfterCount, Repeatedly

service_account_path = os.environ.get("SERVICE_ACCOUNT_PATH")
print("Service account file : ", service_account_path)
subscription_path = os.environ.get("SUBSCRIPTION_PATH")

input_subscription = os.environ.get("INPUT_SUBSCRIPTION")

player_topic = os.environ.get("PLAYER_TOPIC")
team_topic = os.environ.get("TEAM_TOPIC")

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options=options)

def custom_timestamp(elements):
  unix_timestamp = elements[16].rstrip().lstrip()
  return beam.window.TimestampedValue(elements, int(unix_timestamp))

def encode_byte_string(element):
  print (element)
  element = str(element)

  return element.encode('utf-8')

def player_pair(element_list):
  # key-value pair of (player_id, 1)
  return element_list[1],1

def score_pair(element_list):
  # key-value pair of (team_id, 1)
  return element_list[3],1

def print_element(element):
    print("intermediate result: ", element)
    return element

def decode_and_split(row):
    return row.decode('utf-8').split(',')

pubsub_data = (
                p
                | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription= input_subscription)
                | 'Parse data' >> beam.Map(decode_and_split)
                #| 'Apply custom timestamp' >> beam.Map(custom_timestamp) we are going to use publish timestamps
              )

player_score = (
                pubsub_data
                | 'Form k,v pair of (player_id, 1)' >> beam.Map( player_pair ) # ('PL_10', 1) we get the player that did a kill, and a 1
                #we will get a result every 10 events.
                | 'Window for player' >> beam.WindowInto(window.GlobalWindows(), trigger=Repeatedly(AfterCount(10)), accumulation_mode=AccumulationMode.ACCUMULATING)
                #we get the sum, by key, after 10 events of the same key ('PL_10', 10)
                | 'Group players and their score' >> beam.CombinePerKey(sum)
                | 'Encode player info to byte string' >> beam.Map(encode_byte_string)
                | 'Write player score to pub sub' >> beam.io.WriteToPubSub(player_topic)
              )

team_score = (
                pubsub_data
                | 'Form k,v pair of (team_score, 1)' >> beam.Map( score_pair )
                | 'Window for team' >> beam.WindowInto(window.GlobalWindows(), trigger=Repeatedly(AfterCount(10)), accumulation_mode=AccumulationMode.ACCUMULATING)
                | 'Group teams and their score' >> beam.CombinePerKey(sum)
                | 'Encode teams info to byte string' >> beam.Map(encode_byte_string)
                | 'Write team score to pub sub' >> beam.io.WriteToPubSub(team_topic)
              )

result = p.run()
result.wait_until_finish()
