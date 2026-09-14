"""
Phase 3's job: Read what Phase 2's producer is publishing. 
    Do something with it (flag anomalies), and forward it onward.
    This stands in for AWS Lambda before actually deploying.

    - its config dict needs everything producer had, plus one new required key: group.id
        group id: how Kafka tracks this specific consumer's reading position, in case of crash or restart, can continue
    - pick any string for the group id ("telemetry-processor")

    Subscribing
        before a consumer can read anything, it has to explicitly tell Kafka which topic(s) it wants-- `consumer.subscribe(name_of_list)`
        this is the API's shape.
        A single consumer is able to subscribe to several topics at once.

    Poll Loop
        Producer was "fire and forget" -- call `produce()`, and it's done (well, queued).
        Consumer works the opposite way. Must continously ask "anything new" -- `consumer.poll(timeout)`
        - while True: `.poll(1.0)`, then checks whether something came back before deciding what to do next
"""
#---------------------------------------------------------------------------------------------------------------------------------------------------------------
import json
from confluent_kafka import Consumer

groupId = 'telemetry-processor'
config_dict = {
    "bootstrap.servers": "localhost:9092",
    "group.id": groupId
}
topic_List = ["equipment-telemetry"]
consumer = Consumer(config_dict)
consumer.subscribe(topic_List)

# Since this is a Consumer, and it must continously asks "anything for me yet?", a loop must be used
timeout = 1.0
while True:
    msg = consumer.poll(timeout)
    if msg is None:
        continue
    elif msg.error():
        err = msg.error()
        #print err
    else:
        producer_message = msg.value() #returns bytes, not a Python string
        decoded_producer_msg = producer_message.decode('utf-8')
        config_JSON_map = json.loads(decoded_producer_msg)
        print(config_JSON_map[3]) #should print engine_temp value
