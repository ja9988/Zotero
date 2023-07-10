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


app = Flask(__name__)


@app.route("/", methods=["POST"])
def index():
    project_id = 'sneakyscraper'
    client = bigquery.Client(project=project_id)
    inbound_brick = request.get_json(force=True)

    is_new_pubsub_message = check_brick(inbound_brick, client)
    if is_new_pubsub_message is False:
        print(f"received_duplicate_pubsub_message: {inbound_brick}")
        return ("", 204)

    # print(f"Inbound Brick: {inbound_brick}")
    # print(f"Inbound Brick Data Type: {type(inbound_brick)}")
    try:
        read_response = read_brick(inbound_brick, client)
        print(f"read_response_004_was: {read_response}")
    except Exception as e:
        print(f"problem_with_reading_brick: {e}")
        pass



    #---------
    # inbound_brick = request.get_json(force=True)
    # inbound_brick = transform_brick(inbound_brick)
    # # parent_accounts = request.get_json(force=True)
    # # print(f"parent_accounts: {parent_accounts} type: {type(parent_accounts)}")
    # # print(f"Inbound Brick: {inbound_brick}")
    # # print(f"Inbound Brick Data Type: {type(inbound_brick)}")
    # pubsub_message_id = inbound_brick['message']['message_id']
    #  # Create BigQuery Client
    # # print('Creating BigQuery Client...')
    # project_id = 'sneakyscraper'
    # client = bigquery.Client(project=project_id)

    # # pubsub_message_id_count = check_pubsub_message_id(pubsub_message_id, client)
    # is_new_pubsub_message = check_brick(inbound_brick, client)
    # print(f"pubsub_message_id: {pubsub_message_id}, is_new_pubsub_message: {is_new_pubsub_message}")
    # if is_new_pubsub_message is False:
    #     return ("", 204)
    # brick_attributes = inbound_brick['message']['attributes']
    # brick_type = brick_attributes['brick_type']

    # if brick_type == 'parents':
    #     # PROCESSING HERE
    #     # print("Do some processing here....")

    #     # FINISHED PROCESSING
    #     # When finished processing, write to brick_log
    #     table_id_brick_log = 'sneakyscraper.scrape_whotwi.brick_log'
    #     test_stream_brick = stream_pubsub_record_to_bq(inbound_brick, table_id_brick_log, client)
    #     print(f"Result of streaming brick record: {test_stream_brick}")


    #     # CHECK BATCH STATUS
    #     # After writing to brick_log, check to see if this was the last message
    #     is_last_brick_message = check_brick_batch(inbound_brick, client)
    #     if is_last_brick_message is True:
    #         time_to_wait = random.random()*(5)
    #         sleep(time_to_wait)
    #         # SEND ANOTHER PUBSUB MESSAGE TO pagedispatch
    #         print(f"This was the final message received! {inbound_brick}")
    #         project_id = "sneakyscraper"
    #         topic_id = "pagedispatch"
    #         last_brick_message = [{'last_parent_uuid': inbound_brick['message']['attributes']['brick_batch_uuid']}]
    #         send_last_brick = dispatch_bricks(last_brick_message, project_id, topic_id, brick_origin='last_parent_test', brick_type='last_parent')
    #         print(f"Send last parent brick to pagedispatch: {send_last_brick}")
    #     else:
    #         print("Still receiving bricks...")
    # elif brick_type == 'children':
    #     print('Processing children brick...')


    # read_response = read_brick(inbound_brick)

    # print(f"Read response was: {read_response}")

    
    
    

    #  # MetadataRequests for Twitter API V2
    # metadata_requests = MetadataRequests().all_reqs
    # # Twitter API Token
    # bearer_token = 'AAAAAAAAAAAAAAAAAAAAAPeTRgEAAAAACzBnb6G9%2FUpVmZ8aIGR1v2QQlQo%3DTDMZqgFapKqT2UgcziKVTzUDApCODAPkbAH8GAKyEyz0tDYVhi'
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