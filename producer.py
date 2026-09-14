import json, random, time
from confluent_kafka import Producer

#success: msg has partition/topic info;error=None
#error: error!=null;msg=None
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [partition {msg.partition()}]")

engineTempAsStr = ''
config_dict = {
  "bootstrap.servers": "localhost:9092"
}
tractor_Id_List = [
    "31274034",
    "32410735",
    "80475633",
    "87054302",
    "32147325"
]
telemetry_reading_dict = {}
producer = Producer(config_dict)
for i in range(10):
    engineTempAsStr = str(random.randint(60, 95))
    equipmentIDAsStr = random.choice(tractor_Id_List)

    telemetry_reading_dict = {
      "equipment_Id": equipmentIDAsStr,
      "engine_temperature": engineTempAsStr
    }

    telem_JSON = json.dumps(telemetry_reading_dict)

    producer.produce("equipment-telemetry", telem_JSON, callback=delivery_report) #callback registered
    producer.poll(0) #it only actually runs when the client processes its event queue, which happens inside poll() or flush()
    # above should be called after every produce() call to let callbacks fire incrementally

    time.sleep(2)

producer.flush() #blocks until every queued message's callback has fired; this should ALWAYS come last
