from datetime import datetime
from confluent_kafka import Producer
from serde import encodeToRaw
from dotenv import load_dotenv
import concurrent.futures
import torch
import argparse
import cv2
import sys
import os
import numpy as np
import time

# List of video are used for object detection
videos = [("Traffic2.m4v",0), ("Traffic2.m4v",1), ("Traffic2.m4v",2)]

########################### Convert model to np.array (frame has been detected)
def score_frame(frame):
    model.to('cuda')
    frame = [frame]
    results = model(frame)
    
    labels, cord = results.xyxyn[0][:, -1], results.xyxyn[0][:, :-1]
    return labels, cord

def plot_boxes(results, frame):
    labels, cord = results
    n = len(labels)
    x_shape, y_shape = frame.shape[1], frame.shape[0]
    for i in range(n):
        row = cord[i]
        # this condition is to take confidence above 0.3 and remove wrong labels which is more than 10
        if row[4] >= 0.3 and labels[i] < 8:
            x1, y1, x2, y2 = int(
                row[0]*x_shape), int(row[1]*y_shape), int(row[2]*x_shape), int(row[3]*y_shape)
            objName_dep = str(model.names[int(labels[i])])
            # + " " + str(round(float(row[4]),2))
            # Draw a shape and text for object
            (text_width, text_height) = cv2.getTextSize(objName_dep, cv2.FONT_HERSHEY_SIMPLEX, 0.6, thickness=2)[0]
            cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
            cv2.rectangle(frame, (x1, y1), (x1 + text_width - 2, y1 - text_height), (0, 255, 0), cv2.FILLED)
            cv2.putText(frame, objName_dep, (x1+3, y1-3), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255,255,255), 2)

            cv2.putText(frame, "Car: " + str(torch.numel(labels[labels==2])) + " "
                               "Bus: " + str(torch.numel(labels[labels==5])) + " "
                               "Truck: " + str(torch.numel(labels[labels==7])), (20, 60), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0,0,255), 2)
    return frame
###############################################################
# throw a callback for each time message is sent to kafka
def delivery_callback(err, msg):
    if err:
        sys.stderr.write("%% Message failed delivery: %s\n" % err)
    else:
        sys.stderr.write(
            "%% Message delivered to %s [%d] @ %d\n"
            % (msg.topic(), msg.partition(), msg.offset())
        )
# Send list of detected frame to each partition in kafka
def send(video):
    url, id = video
    # Get url of video
    vid = cv2.VideoCapture(url)
    count = 1
    while True:
        start_time = time.perf_counter()
        # If true, capture the frame
        ret, frm = vid.read()
        if ret is False:
            cv2.destroyAllWindows()
            vid.release()
            break
        if count == 1:
            # Resize frame to 416x416
            gray = cv2.resize(frm, (416, 416))

            # pass gray frame has been resized to Covert class
            results = score_frame(gray)
            frame = plot_boxes(results, gray)
            end_time = time.perf_counter()

            # display fps and time using opencv
            dt = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            fps = 1 / np.round(end_time - start_time, 3)
            cv2.rectangle(frame, (16, 15), (287, 39), (0, 0, 0), -1)
            cv2.putText(frame, f'FPS: {int(fps)}' + " "*2 + dt, (20, 32), cv2.FONT_HERSHEY_DUPLEX, 0.5, (255, 255, 255), 2)

            # encode msg and send to kafka topic
            p.produce(
                topic, 
                encodeToRaw(frame, str(id)),
                # callback=delivery_callback, 
                partition=id
            )
            p.poll(1)
            count += 1
        count += 1
        if count == 26:
            count = 1
    print(f"exiting message {id}")
    vid.release()

if __name__ == "__main__":

    # pass the CLI argument 
    # parser = argparse.ArgumentParser()
    # parser.add_argument("topic", type=str)
    # args = parser.parse_args()
    
    # Load yolov5 model
    model = torch.hub.load('ultralytics/yolov5', 'yolov5s')
    # model = torch.hub.load('ultralytics/yolov5', 'custom', path='yolov5s.engine')

    # Load variables from .env file
    load_dotenv()

    # kafka config
    broker = os.environ.get("BROKER")
    topic = os.environ.get("RESULT_TOPIC")
    conf = {"bootstrap.servers": broker}
    p = Producer(**conf)

    try:
        # Send messages to multiple partition in topic with parallel method 
        with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.map(send, videos)
    finally:
        sys.exit()
	

