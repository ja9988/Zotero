import os
import requests
from flask import Flask, request
from google.cloud import bigquery
import base64
import json
import random
from time import sleep
# from send_brick import dispatch_bricks
# from twitter_api_tools import get_metadata_tapi
from process_message import read_brick, check_brick
import traceback
from google.cloud import spanner


app = Flask(__name__)
instance_id = 'toto'
database_id = 'scrape_whotwi'
spanner_client = spanner.Client()
instance = spanner_client.instance(instance_id)
spanner_db = instance.database(database_id)
project_id = 'sneakyscraper'
client = bigquery.Client(project=project_id)

@app.route("/", methods=["POST"])
def index():
    inbound_brick = request.get_json(force=True)

    is_new_pubsub_message = check_brick(inbound_brick, client, spanner_db)
    if is_new_pubsub_message is False:
        print(f"received_duplicate_pubsub_message: {inbound_brick}")
        return ("", 204)

    try:
        read_response = read_brick(inbound_brick, client, spanner_db)
        print(f"Read response was: {read_response}")
    except Exception as error:
        print(f'Problem with read_brick: {inbound_brick}')
        full_stack_trace = ''.join(traceback.format_exception(None, error, error.__traceback__))
        print(full_stack_trace)
        pass

    return ("", 204)
    
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))