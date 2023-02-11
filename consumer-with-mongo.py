from confluent_kafka import Consumer, KafkaException, TopicPartition
from serde import decodeFromResult
from dotenv import load_dotenv
import argparse
import cv2
import json
import time
from flask import Flask, render_template, Response, url_for
import pymongo
from bson.binary import Binary

#MongoDB part
dbClient = pymongo.MongoClient("mongodb://localhost:27017/")
trafficDatabase = dbClient["trafficDatabase"]
trafficCollection = trafficDatabase["trafficCollection"]

#config of Consumer
conf = {"bootstrap.servers": 'tuanmai:9092',
        "group.id": "detection_group",
        "auto.offset.reset": "earliest",}

def consume(topic, partition):
    consumer = Consumer(conf) #with each topic and partition as argument, create an instance of Consumer
    consumer.assign([TopicPartition(topic, partition)])
    while True:
        msg = consumer.poll(timeout=5.0)
        if msg is None:
            print(f"Waiting for message {partition} ...")
        elif msg.error():
            raise KafkaException(msg.error())
        else:
            # print(f"Receiving message {args.partition} ...")
            msg = decodeFromResult(msg.value())
            list = [msg["car_num"], msg["bus_num"], msg["truck_num"]]  # to create chart on website
            jsonList = json.dumps(list)
            img = cv2.imwrite(f'camera{partition}.jpg', msg['img']) #convert numpy.ndarray to .jpg then send to flask server
            binaryData = Binary(open(f'camera{partition}.jpg', 'rb').read())
            dbDoc = { #create documents to insert to db
                "cameraID": msg["cameraID"],
                "timestamp": msg["timestamp"],
                "img": binaryData, #need to convert to base64 before insert
                "car_num": msg["car_num"],
                "bus_num": msg["bus_num"],
                "truck_num": msg["truck_num"],
            }
            print(dbDoc)
            # x = trafficCollection.insert_one(dbDoc) #insert to db
            # print(x.acknowledged)
            print(f"vehicles number of frame {partition} : {list}")
            yield (b'--frame\r\n'
                    b'Content-Type: image/jpeg\r\n\r\n' + open(f'camera{partition}.jpg', 'rb').read()+ b'\r\n')
            time.sleep(0.01) #Firefox need sometime to display, if you view it on Chrome, comment this line of code
            yield jsonList

#Flask server
app = Flask(__name__)
@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

#Route
@app.route('/video_feed0')
def video_feed0():
    return Response(consume('test', 0), mimetype='multipart/x-mixed-replace; boundary=frame')

if __name__ == "__main__":
    app.run(debug=True)
