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


    Phase 3: AWS side consumer
        -add basic validation/anamoly flagging 
            -?what am I validating?
            -?what am I flagging?

        3. Forward result over HTTPS to Stug Endpoint
        Rather than waiting until then to test any of this, use a public echo service in the meantime: 
            https://httpbin.org/post 
         accepts any POST request and echoes back exactly what it received — headers, body, everything — 
         as its response. 
         It's a genuinely useful tool to keep in your back pocket beyond just this project, any time you 
         want to confirm a request is shaped correctly before you have a real server to point it at.
         signature requests.post(url, json=some_dict)
"""
#---------------------------------------------------------------------------------------------------------------------------------------------------------------
import json, requests
from confluent_kafka import Consumer

flagged_telems = []
groupId = 'telemetry-processor'
config_dict = {
    "bootstrap.servers": "localhost:9092",
    "group.id": groupId
}
topic_List = ["equipment-telemetry"]
consumer = Consumer(config_dict)
consumer.subscribe(topic_List)

# Since this is a Consumer, and it must continously asks "anything for me yet?", a loop must be used
timeout = 0.2
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
        telemetry_dict = json.loads(decoded_producer_msg)
        if int(telemetry_dict["engine_temperature"]) >= 85:
            #flag
            #✔ pass #REPLACE THIS LINE WITH ACTUAL RUNABLE CODE
            #✔ add to list of flagged temperatures
            flagged_telems.append(telemetry_dict)
            # ✔print out equipment_Id and engine_temperature
            print(f"Flagged equipment and related temperature: {telemetry_dict}")
            # requests.post(url, json=some_dict)
            # public echo service confirming if request is shaped correctly before using/having real server point to it
            test_request = requests.post("https://httpbin.org/post", json=telemetry_dict)
            print(f"POST response: {test_request.json()}")
        #print(telemetry_dict["engine_temperature"]) #should print engine_temp value
        #above should no longer be there due to what's inside the closest if block

