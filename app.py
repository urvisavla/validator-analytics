import sys
import zmq
import json

#  Socket to talk to server
context = zmq.Context()
socket = context.socket(zmq.SUB)

print("Collecting validator info from pipeline ...")
socket.connect("tcp://127.0.0.1:5555")
socket.subscribe("")

while True:

    message = socket.recv()
    json_object = json.loads(message)
    json_formatted_str = json.dumps(json_object, indent=2)
    print(f"Validator info:\n\n{json_formatted_str}")

