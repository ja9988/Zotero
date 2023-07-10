import os
import requests
from flask import Flask, request
from google.cloud import bigquery
import base64
import json
# from external_funcs import read_brick
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

@app.route("/", methods=["POST"])
def index():
    print("PASS!")
    project_id = 'sneakyscraper'
    client = bigquery.Client(project=project_id)
    inbound_brick = request.get_json(force=True)
    # parent_accounts = request.get_json(force=True)
    # print(f"parent_accounts: {parent_accounts} type: {type(parent_accounts)}")
    print(f"Inbound Brick: {inbound_brick}")
    print(f"Inbound Brick Data Type: {type(inbound_brick)}")

    is_new_pubsub_message = check_brick(inbound_brick, client, spanner_db)
    if is_new_pubsub_message is False:
        print(f"received_duplicate_pubsub_message: {inbound_brick}")
        return ("", 204)


    try:
        read_response = read_brick(inbound_brick, client, spanner_db)
        print(f"Read response was: {read_response}")
    except Exception as error:
        print(f'Problem with classifybricks: {inbound_brick}')
        full_stack_trace = ''.join(traceback.format_exception(None, error, error.__traceback__))
        print(full_stack_trace)
        pass

    
    
    

    #  # MetadataRequests for Twitter API V2
    # metadata_requests = MetadataRequests().all_reqs
    # # Twitter API Token
    # bearer_token = os.environ.get('twitter_api_bearer_staging')
    # # Get Parent metadata using the Twitter API V2
    # twitter_metadata = get_metadata_tapi(parent_accounts, metadata_requests, bearer_token)
    # print(f"Twitter metadata of grandparent accounts has length: {len(twitter_metadata)}")
    # # Validate the Twitter API Metadata
    # twitter_metadata_check = check_twitter_metadata(twitter_metadata, parent_accounts)
    # if len(twitter_metadata_check) == 0:
    #     print('Twitter metadata was healthy and will be used to speed up crawling')
    #     print(twitter_metadata)
    #     fof_parent_input = twitter_metadata
    # else:
    #     print('Unfortunately Twitter couldnt be scraped, proceeding with whotwi...')
    #     fof_parent_input = parent_accounts

    return ("", 204)
    
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))