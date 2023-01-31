from confluent_kafka import Consumer, KafkaException, TopicPartition
from serde import decodeFromResult
from dotenv import load_dotenv
import argparse
import cv2
import os

def consume(consumer):
    while True:
        msg = consumer.poll(timeout=5.0)
        if msg is None:
            print(f"Waiting for message {args.partition} ...")
        elif msg.error():
            raise KafkaException(msg.error())
        else:
            # print(f"Receiving message {args.partition} ...")
            msg = decodeFromResult(msg.value())
            list = [msg["car_num"], msg["bus_num"], msg["truck_num"]]    
            print(f"vehicles number of frame {args.partition} : {list}")
            cv2.imshow(f"camera {args.partition}", msg["img"])
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break
    cv2.destroyAllWindows()
    
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("partition", type=int)
    args = parser.parse_args()

    load_dotenv()
    broker = os.environ.get("BROKER")
    topic = os.environ.get("RESULT_TOPIC")
    group = "detection_group"
    conf = {"bootstrap.servers": broker,
            "group.id": group,
            "auto.offset.reset": "earliest",}

    c = Consumer(conf)
    c.assign([TopicPartition(topic, args.partition)])

    consume(c)
