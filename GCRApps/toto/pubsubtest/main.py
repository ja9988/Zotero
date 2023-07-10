import os
import requests
from flask import Flask, request
from google.cloud import bigquery
import base64
import json
from external_funcs import get_fof_bs4_page

app = Flask(__name__)

def binary_to_dict(the_binary):
    try:
        jsn = ''.join(chr(int(x, 2)) for x in the_binary.split())
        d = json.loads(jsn)  
    except Exception as e:
        print(f"error {e}")
        d = 0
    return d

def bytestr_to_dict(bytestr_dict):
    the_dict = json.loads(base64.b64decode(bytestr_dict).decode("utf-8"))
    return the_dict

def test_request(username, page):
    whotwi_link = f"https://en.whotwi.com/{username}/friends/user_list?page={page}+&view_type=list"
    whotwi_get = requests.get(whotwi_link)
    return whotwi_get

@app.route("/", methods=["POST"])
def index():
    envelope = request.get_json()

    # try:
    #     print(f"Trying to print pub/sub message...{envelope}")
    #     # print()
    # except Exception as e:
    #     print(f"pub/sub message failure {e}")

    if not envelope:
        msg = "no Pub/Sub message received"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    if not isinstance(envelope, dict) or "message" not in envelope:
        msg = "invalid Pub/Sub message format"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    pubsub_message = envelope["message"]
    pubsub_message_2 = pubsub_message.copy()
    name = "World"
    if isinstance(pubsub_message, dict) and "data" in pubsub_message:
        name = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()

    # print(f"Hello {name}!")

    test_decode = bytestr_to_dict(pubsub_message_2["data"])
    # print(f"testing bytestr_to_dict\n..........................................\n{test_decode}\ntype was: {type(test_decode)}\n..........................................")

    if 'brick_type' in test_decode:
        try:
            # print(f"Brick type {test_decode['brick_type']} received. Parsing...")
            brick_payload = test_decode['payload']
            username = brick_payload['username']
            page = brick_payload['page']
            page_max = brick_payload['page_max']
            message_number = test_decode['message_number']
            message_batch = test_decode['message_batch']
            message_id = pubsub_message['message_id']
            print(f"Brick {message_number} received for page {page} of {page_max} for {username}. Batch {message_batch}. MessageId {message_id}")
            # test_get = test_request(username, page)
            # print(f"GET response for page {page} of {page_max} for {username}: {test_get.status_code}")

            test_page = get_fof_bs4_page(username, page)
            if len(test_page) > 0:
                print(f"Brick {message_number} for page {page} of {page_max} for {username} was a success. Batch {message_batch}")
        except Exception as e:
            print(f"Problem parsing brick {e}")
            pass
    # print(test_decode)

    return ("", 204)
    
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))