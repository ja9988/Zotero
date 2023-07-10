from concurrent import futures
from google.cloud import pubsub_v1
# from collections.abc import Callable
from typing import Callable
import json
import uuid
import time

def dict_to_bytestr(the_dict):
    bytestr_dict = json.dumps(the_dict).encode('utf-8')
    return bytestr_dict

def gen_publisher_and_path(project_id, topic_id):
    # Generate credentials
    # credentials = service_account.Credentials.from_service_account_file('/Users/mar2194/Downloads/sneakyscraper-3711ecde4bd2.json')
    # Generate Publisher
    publisher = pubsub_v1.PublisherClient()
    # Create topic path
    topic_path = publisher.topic_path(project_id, topic_id)
    # Futures list?
#     publish_futures = []
    return publisher, topic_path

def gen_brick_batch_uuid(brick_type=None):
    brick_batch_uuid = str(uuid.uuid4())
    if brick_type is not None and (type(brick_type) is int or type(brick_type) is str):
        brick_batch_uuid = str(brick_type)+'_'+brick_batch_uuid
        
    return brick_batch_uuid

def get_callback(
    publish_future: pubsub_v1.publisher.futures.Future, data: str
) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
    def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
        try:
            # Wait 60 seconds for the publish call to succeed.
            # print(publish_future.result(timeout=60))
            publish_future_result = publish_future.result(timeout=60)
        except futures.TimeoutError:
            print(f"Publishing {data} timed out!")

    return callback


def dispatch_bricks(list_of_bricks, project_id, topic_id, brick_origin='parents_test', brick_type='parents'):
    success_message = ""
    publish_futures = []
    brick_batch_uuid = gen_brick_batch_uuid()
    brick_batch_total = str(len(list_of_bricks))
    for brick_ind, brick in enumerate(list_of_bricks):
        publisher, topic_path = gen_publisher_and_path(project_id, topic_id)
        success_message = f"Published bricks with error handler to {topic_path}, {brick_batch_uuid}."
        time.sleep(1.25) # Will this keep help the autoscaling process and prevent lost pub/sub messages??
        brick_number = str(brick_ind+1)
        publish_futures = throw_brick(topic_path, publisher, publish_futures, brick, brick_batch_uuid, 
                                      brick_batch_total, brick_number, brick_origin, brick_type)
    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
        
    # print()
        
    return success_message


def throw_brick(topic_path, publisher, publish_futures, brick, brick_batch_uuid, brick_batch_total, brick_number, brick_origin, brick_type):
    data = dict_to_bytestr(brick)
    publish_future = publisher.publish(topic_path, data, brick_batch_uuid=brick_batch_uuid,
                                      brick_batch_total=brick_batch_total,
                                      brick_number=brick_number,
                                      brick_origin=brick_origin,
                                      brick_type=brick_type)
    publish_future.add_done_callback(get_callback(publish_future, data))
    publish_futures.append(publish_future)
    return publish_futures