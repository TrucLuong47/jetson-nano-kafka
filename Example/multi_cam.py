from datetime import datetime
import cv2
import torch
import numpy as np
import time

def score_frame(frame):
    """
    Takes a single frame as input, and scores the frame using yolo5 model.
    :param frame: input frame in numpy/list/tuple format.
    :return: Labels and Coordinates of objects detected by model in the frame.
    """
    model.to('cuda' if torch.cuda.is_available() else 'cpu')
    frame = [frame]
    results = model(frame)

    labels, cord = results.xyxyn[0][:, -1], results.xyxyn[0][:, :-1]
    return labels, cord


def class_to_label(x):
    """
    For a given label value, return corresponding string label.
    :param x: numeric label
    :return: corresponding string label
    """
    return int(x)


def plot_boxes(results, frame):
    """
    Takes a frame and its results as input, and plots the bounding boxes and label on to the frame.
    :param results: contains labels and coordinates predicted by model on the given frame.
    :param frame: Frame which has been scored.
    :return: Frame with bounding boxes and labels ploted on it.
    """
    labels, cord = results
    n = len(labels)
    x_shape, y_shape = frame.shape[1], frame.shape[0]
    for i in range(n):
        row = cord[i]
        if row[4] >= 0.2:
            x1, y1, x2, y2 = int(
                row[0]*x_shape), int(row[1]*y_shape), int(row[2]*x_shape), int(row[3]*y_shape)
            bgr = (0, 255, 0)
            cv2.rectangle(frame, (x1, y1), (x2, y2), bgr, 2)
            cv2.putText(frame, str(model.names[class_to_label(labels[i])]) + " " + str(round(float(row[4]),2)), (x1, y1), cv2.FONT_HERSHEY_SIMPLEX, 0.5, bgr, 2)

    return frame

if __name__ == "__main__":

    # model = torch.hub.load('ultralytics/yolov5', 'custom', 'detect.pt')
    # model = torch.hub.load('ultralytics/yolov5', 'yolov5s', pretrained=True)
    model = torch.hub.load('ultralytics/yolov5', 'custom', path='yolov5s_2.engine')

    vid1 = cv2.VideoCapture('Traffic1.m4v')
    vid2 = cv2.VideoCapture('Traffic2.m4v')
    vid3 = cv2.VideoCapture('Traffic3.m4v')

    while vid1.isOpened() or vid2.isOpened():
        start_time = time.perf_counter()
        # --
        okay1 , frm1 = vid1.read()
        okay2 , frm2 = vid2.read()
        okay3 , frm3 = vid3.read()

        gray1 = cv2.resize(frm1, (416, 416))
        gray2 = cv2.resize(frm2, (416, 416))
        gray3 = cv2.resize(frm3, (416, 416))

        results1 = score_frame(gray1)
        frame1 = plot_boxes(results1, gray1)
        results2 = score_frame(gray2)
        frame2 = plot_boxes(results2, gray2)
        results3 = score_frame(gray3)
        frame3 = plot_boxes(results3, gray3)

        # --
        end_time = time.perf_counter()
        dt = str(datetime.now())
        fps = 1 / np.round(end_time - start_time, 3)

        cv2.putText(frame1, f'FPS: {int(fps)}' + " "*2 + dt, (20, 70),
                    cv2.FONT_HERSHEY_DUPLEX, 0.7, (172, 7, 237), 2)
        cv2.putText(frame2, f'FPS: {int(fps)}' + " "*2 + dt, (20, 70),
                    cv2.FONT_HERSHEY_DUPLEX, 0.7, (172, 7, 237), 2)
        cv2.putText(frame3, f'FPS: {int(fps)}' + " "*2 + dt, (20, 70),
                    cv2.FONT_HERSHEY_DUPLEX, 0.7, (172, 7, 237), 2)

        if okay1:
            cv2.imshow('cam1' , frame1)
        if okay2:
            cv2.imshow('cam2' , frame2)
        if okay3:
            cv2.imshow('cam3' , frame3)

        if not okay1 or not okay2 or not okay3:
            print('Cant read the video , Exit!')
            break

        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
    
        cv2.waitKey(1)

    vid1.release()
    vid2.release()
    vid3.release()
    cv2.destroyAllWindows()
    print("\nExiting.")
