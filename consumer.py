"""
Phase 3's job: Read what Phase 2's producer is publishing. 
    Do something with it (flag anomalies), and forward it onward.
    This stands in for AWS Lambda before actually deploying.

    - its config dict needs everything producer had, plus one new required key: group.id
        group id: how Kafka tracks this specific consumer's reading position, in case of crash or restart, can continue
    - pick any string for the group id ("telemetry-processor")
"""
#---------------------------------------------------------------------------------------------------------------------------------------------------------------
from confluent_kafka import Consumer

groupId = 'telemetry-processor'
config_dict = {
    "bootstrap.servers": "localhost:9092",
    "group id": groupId
}
consumer = Consumer(config_dict)
