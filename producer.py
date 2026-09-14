import json, random, time
from confluent_kafka import Producer

#success: msg has partition/topic info;error=None
#error: error!=null;msg=None
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [partition {msg.partition()}]")

config_dict = {
  "bootstrap.servers": "localhost:9092"
}
telemetry_reading_dict = {}
for i in range(10):
    engineTempAsStr = str(random.randint(60, 95))
    time.sleep(2)

telemetry_reading_dict = {
  "equipment_Id": "12345678",
  "engine_temperature": "67"
}

telem_JSON = json.dumps(telemetry_reading_dict)

producer = Producer(config_dict)
producer.produce("equipment-telemetry", telem_JSON, callback=delivery_report) #callback registered
producer.poll(0) #it only actually runs when the client processes its event queue, which happens inside poll() or flush()
# above should be called after every produce() call to let callbacks fire incrementally
producer.flush() #blocks scripts and ensures everything has at least failed up to this point; this should ALWAYS come last
