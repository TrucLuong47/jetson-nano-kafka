from datetime import datetime
from collections import Counter
import cv2
import torch
import numpy as np
import time

def score_frame(frame):
    model.to('cuda' if torch.cuda.is_available() else 'cpu')
    frame = [frame]
    results = model(frame)
    labels, cord = results.xyxyn[0][:, -1], results.xyxyn[0][:, :-1]
    return labels, cord

def plot_boxes(results, frame):
    labels, cord = results
    n = len(labels)
    x_shape, y_shape = frame.shape[1], frame.shape[0]
    vehicles_count = []
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
            
            vehicles_count = [torch.numel(labels[labels==2]), torch.numel(labels[labels==5]), torch.numel(labels[labels==7])]

    return {
        "frame": frame,
        "vehicles_count": vehicles_count
    }
if __name__ == "__main__":

    model = torch.hub.load('ultralytics/yolov5', 'yolov5s')
    # model = torch.hub.load('ultralytics/yolov5', 'custom', path='yolov5s_2.engine')

    vid = cv2.VideoCapture('Traffic2.m4v')
    while vid.isOpened():
        start_time = time.perf_counter()
        # --
        ret, frm = vid.read()
        if not ret: 
            break
        gray = cv2.resize(frm, (416, 416))
        results = score_frame(gray)
        pb_results = plot_boxes(results, gray)
        # --
        end_time = time.perf_counter()
        dt = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        fps = 1 / np.round(end_time - start_time, 3)
        
        # print(pb_results["vehicles_count"])

        cv2.rectangle(pb_results["frame"], (16, 15), (287, 39), (0, 0, 0), -1)
        cv2.putText(pb_results["frame"], f'FPS: {int(fps)}' + " "*2 + dt, (20, 32), cv2.FONT_HERSHEY_DUPLEX, 0.5, (255, 255, 255), 2)

        cv2.imshow("Public camera", pb_results["frame"])
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break 
    vid.release()
    cv2.destroyAllWindows()
    print("\nExiting.")
