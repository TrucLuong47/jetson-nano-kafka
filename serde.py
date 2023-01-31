# type: ignore
from datetime import datetime
# from schema_pb2 import Result
from test_pb2 import Result
import numpy as np
import cv2 as cv

def encodeToResult(frame, id, vehicles_count):
    carN, busN , truckN = vehicles_count
    m = Result()
    _, buffer = cv.imencode(".jpg", frame)
    m.cameraID = id
    m.frame = buffer.tobytes()
    m.timestamp = str(int(datetime.now().timestamp()))
    m.Vehicles.car = carN
    m.Vehicles.bus = busN
    m.Vehicles.truck = truckN
    return m.SerializeToString()

def decodeFromResult(buffer):
    m = Result()
    m.ParseFromString(buffer)
    # decode frame to img
    nparr = np.frombuffer(m.frame, np.uint8)
    img = cv.imdecode(nparr, cv.IMREAD_COLOR)
    return {
        "cameraID": m.cameraID,
        "frame": m.frame,
        "timestamp": m.timestamp,
        "img": img,
    }