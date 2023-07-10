from external_funcs import MetadataRequests, check_twitter_metadata, get_fof_bs4_page_limit
from external_funcs import get_followers_bs4_page_limit
import base64
import json
# from twitter_api_tools import get_metadata_tapi
from send_brick import dispatch_bricks
import random
from time import sleep
import datetime
from google.cloud import spanner
from google.api_core.exceptions import GoogleAPICallError
from google.api_core.datetime_helpers import DatetimeWithNanoseconds

# def check_twitter_metadata(twitter_metadata, list_of_parents):
#     try:
#         for username in twitter_metadata.keys():
#             if twitter_metadata[username]['num_friends'] == 0:
#                 return list_of_parents
#         return []
#     except Exception as e:
#         print(f"Exception in check_twitter_metadata: {e}")
#         return list_of_parents


# def check_pubsub_message_id(pubsub_message_id, client):
#     sql = f"""SELECT COUNT(*) as num_messages
#     FROM sneakyscraper.scrape_whotwi.brick_log
#     WHERE 
#     pubsub_message_id = '{pubsub_message_id}';"""
#     try:
#         query_job = client.query(sql)
#         records = [dict(row) for row in query_job]
#     except Exception as e:
#         records = []
#         print(f"Error with check_pubsub_message_id: {e}")
#         pass

#     return records

# def check_pubsub_message_id(pubsub_message_id, instance_id, database_id):
    
#     spanner_client = spanner.Client()
#     instance = spanner_client.instance(instance_id)
#     database = instance.database(database_id)
#     results_list = []
    
#     with database.snapshot() as snapshot:
#         results = snapshot.execute_sql(
#             "SELECT COUNT(*) as num_messages "
#             "FROM brick_log "
#             "WHERE pubsub_message_id = @pubsub_message_id;",
#             params={"pubsub_message_id": pubsub_message_id},
#             param_types={"pubsub_message_id": spanner.param_types.STRING},
#         )

#         for row in results:
#             results_list.append({"num_messages": row[0]})
            
#     return results_list


def revbits(x):
    return int(bin(x)[2:].zfill(64)[::-1], 2)

# CANT FIGURE OUT WHY THIS DOESNT WORK
# def check_pubsub_message_id(pubsub_message_id, instance_id, database_id):
    

#     pubsub_message_id_int = int(pubsub_message_id)
#     pubsub_message_id_rev = revbits(pubsub_message_id_int)
    
#     print(f"Inside check_pubsub_message_id, type(pubsub_message_id_rev): {type(pubsub_message_id_rev)}")
#     print(f"type(pubsub_message_id): {type(pubsub_message_id)}")
#     print(f"type(pubsub_message_id_int): {type(pubsub_message_id_int)}")
#     spanner_client = spanner.Client()
#     instance = spanner_client.instance(instance_id)
#     database = instance.database(database_id)
#     results_list = []
    
#     with database.snapshot() as snapshot:
#         results = snapshot.execute_sql(
#             "SELECT COUNT(*) as num_messages "
#             "FROM brick_log "
#             "WHERE pubsub_message_id_reverse = @pubsub_message_id_rev;",
#             params={"pubsub_message_id_rev": pubsub_message_id_rev},
#             param_types={"pubsub_message_id_rev": spanner.param_types.INT64},
#         )

#         for row in results:
#             results_list.append({"num_messages": row[0]})
            
#     return results_list

# TODO: Try next!
# def check_pubsub_message_id(pubsub_message_id, instance_id, database_id):
def check_pubsub_message_id(pubsub_message_id, database):
    

    pubsub_message_id_int = int(pubsub_message_id)
    pubsub_message_id_rev = str(revbits(pubsub_message_id_int))
    
    print(f"Inside check_pubsub_message_id, type(pubsub_message_id_rev): {type(pubsub_message_id_rev)}")
    print(f"type(pubsub_message_id): {type(pubsub_message_id)}")
    print(f"type(pubsub_message_id_int): {type(pubsub_message_id_int)}")
    # spanner_client = spanner.Client()
    # instance = spanner_client.instance(instance_id)
    # database = instance.database(database_id)
    results_list = []
    
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            "SELECT COUNT(*) as num_messages "
            "FROM brick_log "
            "WHERE pubsub_message_id_reverse = @pubsub_message_id_rev;",
            params={"pubsub_message_id_rev": pubsub_message_id_rev},
            param_types={"pubsub_message_id_rev": spanner.param_types.STRING},
        )

        for row in results:
            results_list.append({"num_messages": row[0]})
            
    return results_list



# def check_pubsub_message_id(pubsub_message_id, instance_id, database_id):
    
#     pubsub_message_id_int = int(pubsub_message_id)
#     pubsub_message_id_rev = str(revbits(pubsub_message_id_int))
    
    
#     spanner_client = spanner.Client()
#     instance = spanner_client.instance(instance_id)
#     database = instance.database(database_id)
#     results_list = []
    
#     with database.snapshot() as snapshot:
#         results = snapshot.execute_sql(
#             "SELECT COUNT(*) as num_messages "
#             "FROM brick_log "
#             f"WHERE pubsub_message_id_reverse = {pubsub_message_id_rev};",
#         )

#         for row in results:
#             results_list.append({"num_messages": row[0]})
            
#     return results_list

def check_brick(brick_json, client, spanner_db):
    pubsub_message_id = brick_json['message']['message_id']
    instance_id = 'toto'
    database_id = 'scrape_whotwi'
    
    pubsub_message_id_count = check_pubsub_message_id(pubsub_message_id, spanner_db)
    # pubsub_message_id_count = check_pubsub_message_id(pubsub_message_id, instance_id, database_id)
    pubsub_message_count = pubsub_message_id_count[0]['num_messages']
    if pubsub_message_count == 0:
        is_new_pubsub_message = True
    else:
        is_new_pubsub_message = False

    return is_new_pubsub_message

def decode_data_dict(data_dict):
    dict_str = base64.b64decode(data_dict).decode("utf-8").strip()
    the_dict = json.loads(dict_str)
    return the_dict

def transform_brick(brick_json):
    '''Transform the data and several fields data type from base64 encoded
    string and string to dicts, int, etc.
    
    Inputs
    ------
    brick_json : dict
        Brick message

    Returns
    -------
    brick_json : dict
        Brick message with certain fields transformed from string to
        another datatype
    
    '''
    brick_batch_total = int(brick_json['message']['attributes']['brick_batch_total'])
    brick_json['message']['attributes']['brick_batch_total'] = brick_batch_total
    brick_number = int(brick_json['message']['attributes']['brick_number'])
    brick_json['message']['attributes']['brick_number'] = brick_number
    brick_data = decode_data_dict(brick_json['message']['data'])
    brick_json['message']['data'] = brick_data

    return brick_json

def check_brick_batch(brick_json, client, database):
    '''
    database: google.cloud.spanner_v1.database.Database

    
    '''
    brick_batch_uuid = brick_json['message']['attributes']['brick_batch_uuid']
    brick_batch_total = brick_json['message']['attributes']['brick_batch_total']
    instance_id = 'toto'
    database_id = 'scrape_whotwi'
    
    brick_batch_count = check_brick_batch_count(brick_batch_uuid, database)
    # brick_batch_count = check_brick_batch_count(brick_batch_uuid, instance_id, database_id)
    batch_count = brick_batch_count[0]['num_messages']

    if batch_count == brick_batch_total:
        is_last_brick_message = True
    else:
        is_last_brick_message = False

    return is_last_brick_message

# def check_brick_batch_count(brick_batch_uuid, client):
#     sql = f"""SELECT COUNT(*) as num_messages
#     FROM sneakyscraper.scrape_whotwi.brick_log
#     WHERE 
#     brick_batch_uuid = '{brick_batch_uuid}';"""
#     try:
#         query_job = client.query(sql)
#         records = [dict(row) for row in query_job]
#     except Exception as e:
#         records = []
#         print(f"Error with check_brick_batch_count: {e}")
#         pass

#     return records

# def check_brick_batch_count(brick_batch_uuid, instance_id, database_id):
def check_brick_batch_count(brick_batch_uuid, database):
    
    # spanner_client = spanner.Client()
    # instance = spanner_client.instance(instance_id)
    # database = instance.database(database_id)
    results_list = []
    
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            "SELECT COUNT(brick_batch_uuid) as num_messages "
            "FROM brick_log@{FORCE_INDEX=brick_log_by_uuid} "
            "WHERE brick_batch_uuid = @brick_batch_uuid;",
            params={"brick_batch_uuid": brick_batch_uuid},
            param_types={"brick_batch_uuid": spanner.param_types.STRING},
        )
        
        for row in results:
            results_list.append({"num_messages": row[0]})
            
    return results_list

def stream_pubsub_record_to_bq(brick_json, table_id, client):
    brick_record = {}
    brick_record['time_received'] = brick_json['message']['publish_time']
    brick_record['brick_batch_uuid'] = brick_json['message']['attributes']['brick_batch_uuid']
    brick_record['brick_batch_total'] = brick_json['message']['attributes']['brick_batch_total']
    brick_record['brick_number'] = brick_json['message']['attributes']['brick_number']
    brick_record['brick_origin'] = brick_json['message']['attributes']['brick_origin']
    brick_record['brick_type'] = brick_json['message']['attributes']['brick_type']
    brick_record['pubsub_message_id'] = brick_json['message']['message_id']

    errors = stream_to_bq(brick_record, table_id, client)

    return errors

# def stream_pubsub_record_to_spanner(brick_json, instance_id, database_id):
def stream_pubsub_record_to_spanner(brick_json, database):
    '''
    database: google.cloud.spanner_v1.database.Database


    '''
    if type(brick_json['message']['publish_time']) is datetime.datetime:
        time_received = brick_json['message']['publish_time'].isoformat() + 'Z'
    else:
        time_received = brick_json['message']['publish_time']

    brick_batch_uuid = brick_json['message']['attributes']['brick_batch_uuid']
    brick_batch_total = brick_json['message']['attributes']['brick_batch_total']
    brick_number = brick_json['message']['attributes']['brick_number']
    brick_origin = brick_json['message']['attributes']['brick_origin']
    brick_type = brick_json['message']['attributes']['brick_type']
    pubsub_message_id = brick_json['message']['message_id']
    # pubsub_message_id_hash = hash(pubsub_message_id)
    pubsub_message_id_reverse = str(revbits(int(pubsub_message_id)))

    
    # spanner_client = spanner.Client()
    # instance = spanner_client.instance(instance_id)
    # database = instance.database(database_id)

    with database.batch() as batch:
        batch.insert(
            table="brick_log",
            columns=("time_received", "brick_batch_uuid", "brick_batch_total", "brick_number",
                    "brick_origin", "brick_type", "pubsub_message_id", "pubsub_message_id_reverse"),
            values=[
                (time_received, brick_batch_uuid, brick_batch_total, brick_number, 
                 brick_origin, brick_type, pubsub_message_id, pubsub_message_id_reverse),
            ],
        )

    return "pass"
    
def stream_list_to_bq(some_list, table_id, client):
    errors = client.insert_rows_json(table_id, some_list)
    if errors == []:
            pass
    else:
        print(f"Encountered errors while inserting rows: {errors} to {table_id}")
    return errors
    


def stream_to_bq(some_dict, table_id, client):
    '''Stream a dictionary to BigQuery.

    Inputs
    ------
    some_dict : dict
        Dictionary of data to stream to BigQuery
    table_id : str
        BigQuery Table ID
    client : bigquery.Client()
        BigQuery Client object

    Returns
    -------
    errors : list
        List of errors when streaming to BigQuery
    
    '''

    # print(f"Streaming dict to bigquery: {some_dict}")
    new_dict = {}
    for key in some_dict.keys():
        if type(some_dict[key]) is datetime.datetime:
            new_dict[key] = some_dict[key].isoformat()
        else:
            new_dict[key] = some_dict[key]
    errors = client.insert_rows_json(table_id, [new_dict])
    if errors == []:
            pass
    else:
        print(f"Encountered errors while inserting rows: {errors} to {table_id}")
    return errors

def parents_to_list(metadata_result, brick_batch_uuid, parent_userid):
    metadata_result_list = []
    for username, metadata_old in metadata_result.items():
        metadata_temp = {}
        for key in metadata_old.keys():
            if type(metadata_old[key]) is datetime.datetime:
                metadata_temp[key] = metadata_old[key].isoformat() + 'Z'
            else:
                metadata_temp[key] = metadata_old[key]

        metadata_temp['brick_batch_uuid'] = brick_batch_uuid
        metadata_temp['parent_userid'] = parent_userid
        metadata_result_list.append(metadata_temp)

    return metadata_result_list

# OLD FOR BIGQUERY
# def new_children_to_list(metadata_result, last_child_uuid):
#     metadata_result_list = []
#     for username, metadata_old in metadata_result.items():
#         metadata_temp = {}
#         for key in metadata_old.keys():
#             if type(metadata_old[key]) is datetime.datetime:
#                 metadata_temp[key] = metadata_old[key].isoformat()
#             else:
#                 metadata_temp[key] = metadata_old[key]

#         metadata_temp['brick_batch_uuid'] = last_child_uuid
#         metadata_result_list.append(metadata_temp)

#     return metadata_result_list


def new_children_to_list(metadata_result, last_child_uuid):
    metadata_result_list = []
    for username, metadata_old in metadata_result.items():
        metadata_temp = {}
        for key in metadata_old.keys():
            metadata_temp[key] = metadata_old[key]

        metadata_temp['brick_batch_uuid'] = last_child_uuid
        metadata_result_list.append(metadata_temp)

    return metadata_result_list

def read_brick(inbound_brick, client, spanner_db):
    '''Read and direct an incoming brick.
    
    
    '''
    inbound_brick = transform_brick(inbound_brick)

    brick_attributes = inbound_brick['message']['attributes']
    brick_data = inbound_brick['message']['data']

    brick_type = brick_attributes['brick_type']
    brick_number = brick_attributes['brick_number']
    brick_batch_total = brick_attributes['brick_batch_total']
    brick_batch_uuid = brick_attributes['brick_batch_uuid']
    brick_origin = brick_attributes['brick_origin']
    
    # If the brick_type is 'parents', then it was sent from pagedispatch after scraping
    # twitter metadata to create the page tuples for get_fof_bs4_page_limit(). Each page
    # processed here contains the latest metadata for some subset of parents in 
    # last_n_friends
    if brick_type == 'parents':
        # First we extract the grandparent username, friends page and how many parents
        # metadata we expect to scrape from that friends page
        parent_metadata = brick_data
        username = parent_metadata['username']
        page = parent_metadata['page']
        num_friends_per_page = parent_metadata['num_friends_per_page']
        parent_userid = parent_metadata['userid']
        print(f"Received parents brick: {brick_data}")
        # Now we use the parameters to get the metadata of parents from this grandparent page
        metadata_result = get_fof_bs4_page_limit(username, page, num_friends_per_page, timeout_lim=20,metadata_requests=MetadataRequests().all_reqs)
        # We convert the parents metadata from this page to a list and add the 
        # brick_batch_uuid to the dictionary for each parent metadata dictionary.
        # The size of these batches is determined in pagedispatch/processgrandparents
        metadata_list = parents_to_list(metadata_result, brick_batch_uuid, parent_userid)
        # Now we write the parent metadata with the brick_batch_uuid (from pagedispatch/processgrandparents)
        # to the last_n_friends_temp table.
        try:
            if len(metadata_list) > 0:
                # OLD
                # stream_list_result = stream_list_to_bq(metadata_list, "sneakyscraper.scrape_whotwi.last_n_friends_temp", client)

                # NEW
                instance_id = 'toto'
                database_id = 'scrape_whotwi'   
                # stream_list_result = stream_to_last_n_friends_temp(metadata_list, instance_id, database_id)
                stream_list_result = stream_to_last_n_friends_temp(metadata_list, spanner_db)

                print(f"streamed to last_n_friends temp: {metadata_list}, {stream_list_result}")
        except Exception as e:
            print(f"problem streaming last_n_friends to spanner: {e}")
        # When we've scraped and written the latest metadata for a subset of parents,
        # 
        # table_id_brick_log = 'sneakyscraper.scrape_whotwi.brick_log'

        # test_stream_brick = stream_pubsub_record_to_bq(inbound_brick, table_id_brick_log, client)
        # instance_id = 'toto'
        # database_id = 'scrape_whotwi'
        spanner_response = stream_pubsub_record_to_spanner(inbound_brick, spanner_db)
        # spanner_response = stream_pubsub_record_to_spanner(inbound_brick, instance_id, database_id)
        print(f"response_from_spanner_parents: {spanner_response}")        # print(f"Result of streaming brick record: {test_stream_brick}")
        # print(f"Finished processing parent: {username} page: {page} metadata_list: {metadata_list}")


        # CHECK BATCH STATUS
        # After writing to brick_log, check to see if this was the last message
        # is_last_brick_message = check_brick_batch(inbound_brick, client)
        is_last_brick_message = check_brick_batch(inbound_brick, client, spanner_db)
        if is_last_brick_message is True:
            # time_to_wait = random.random()*(5)
            # sleep(time_to_wait)
            # SEND ANOTHER PUBSUB MESSAGE TO pagedispatch
            # print(f"This was the final message received! {inbound_brick}")
            
            # Testing:
            last_brick_message = [{'last_parent_uuid': inbound_brick['message']['attributes']['brick_batch_uuid']}]
            # print(f"Would be sending last_brick_message: {last_brick_message}")

            # Production:
            project_id = "sneakyscraper"
            topic_id = "pagedispatch"
            send_last_brick = dispatch_bricks(last_brick_message, project_id, topic_id, brick_origin='get_page.process_message.read_brick', brick_type='last_parent')
            print(f"this_was_the_final_message_received {inbound_brick}. Sending last_brick_message brick to pagedispatch: {last_brick_message}")
            message = "last_parent"
        else:
            # print("Still receiving bricks...")
            message = f"still_receiving_parents: {inbound_brick['message']['attributes']['brick_batch_uuid']} "
    elif brick_type == 'children':
        # Extract children
        children_metadata = brick_data
        username = children_metadata['username']
        page = children_metadata['page']
        if 'userid' in children_metadata:
            userid = children_metadata['userid']
            print(f"userid was in children_metadata: {username} --> {userid}")
        num_friends_per_page = children_metadata['num_friends_per_page']

        # Get metadata of parents from grandparent page
        metadata_new_account = get_fof_bs4_page_limit(username, page, num_friends_per_page, timeout_lim=20,metadata_requests=MetadataRequests().all_reqs)
         # Transform metadata to list and add char_ind and brick_batch_uuid
        metadata_new_account_list = new_children_to_list(metadata_new_account, brick_batch_uuid)
        # Write metadata_list to new_children_temp
        if len(metadata_new_account_list) > 0:
            print(f"new accounts_children: {metadata_new_account_list}")
            # OLD
            # stream_list_result = stream_list_to_bq(metadata_new_account_list, "sneakyscraper.scrape_whotwi.new_children_temp", client)

            try:
                print("attempting to write to new_children_temp on spanner")
                stream_list_result = stream_to_new_children_temp(metadata_new_account_list, children_metadata, spanner_db)
                print(f"stream_list_result writing to new_children_temp: {stream_list_result}")

            except Exception as e:
                print(f"problem writing to new_children_temp: {e}")

        # FINISHED PROCESSING
        # When finished processing, write to brick_log
        # TODO: Figure out where to read/write spanner acks
        spanner_response = stream_pubsub_record_to_spanner(inbound_brick, spanner_db)
        # spanner_response = stream_pubsub_record_to_spanner(inbound_brick, instance_id, database_id)
        print(f"response from spanner children: {spanner_response}")

        # CHECK BATCH STATUS
        # After writing to brick_log, check to see if this was the last message
        # is_last_brick_message = check_brick_batch(inbound_brick, client)
        is_last_brick_message = check_brick_batch(inbound_brick, client, spanner_db)
        if is_last_brick_message is True:
            # time_to_wait = random.random()*(5)
            last_child_brick = [{'last_child_uuid': inbound_brick['message']['attributes']['brick_batch_uuid']}]

            # Production
            project_id = "sneakyscraper"
            topic_id = "pagedispatch"
            send_last_brick = dispatch_bricks(last_child_brick, project_id, topic_id, brick_origin='get_page.process_message.read_brick', brick_type='last_child')
            print(f"Sent last_child_brick: {last_child_brick}")
            # print(f"this_was_the_final_message_received {inbound_brick}. Send last child brick to pagedispatch: {send_last_brick}")
            message = f"last_children_brick: {children_metadata}, brick: {inbound_brick}, metadata_new_account_list: {metadata_new_account_list}, is_last_brick_message: {is_last_brick_message}"
        else:
            # print("Still receiving bricks...")
            message = f"still_receiving_children: {children_metadata}, brick: {inbound_brick}, metadata_new_account_list: {metadata_new_account_list}, is_last_brick_message: {is_last_brick_message}"





        # print('Processing children brick...')
    elif brick_type == 'vertex_fof':
        vertex_fof_pubsub_spanner_response = stream_pubsub_record_to_spanner(inbound_brick, spanner_db)
        print(f"succesfully wrote vertex_fof_pubsub_spanner_response: {vertex_fof_pubsub_spanner_response}")
        # Extract children
        vertex_fof_metadata = brick_data
        username = vertex_fof_metadata['username']
        username_char_ind = vertex_fof_metadata['username_char_ind']
        # Get friends of children
        metadata_result = get_fof_bs4_page_limit(username, 1, 50, timeout_lim=20,metadata_requests=MetadataRequests().all_reqs, how='parent_account')
        num_batches_mf = len(metadata_result)
        if num_batches_mf > 20:
            num_batches_mf = 20
        # write friends of children to master_fof table
        table_id_master_friends = "sneakyscraper.scrape_whotwi.master_friends"
        stream_to_master_friends = batch_stream_bigquery(metadata_result, client, table_id_master_friends, num_batches=num_batches_mf) 
        try:
            stream_to_master_friends_spanner = stream_to_master_friends_temp(metadata_result, spanner_db)
            print(f"finished stream_to_master_friends_spanner: {stream_to_master_friends_spanner}")
        except Exception as e:
            print(f"problem with stream_to_master_friends_spanner: {e}")
        # send message to classifybricks
        project_id = "sneakyscraper"
        topic_id = "classifybricks"
        finished_fof_message = [{'username': username, 'username_char_ind': username_char_ind}]
        # def dispatch_bricks(list_of_bricks, project_id, topic_id, brick_origin='parents_test', brick_type='parents'):
        send_finished_fof = dispatch_bricks(finished_fof_message, project_id, topic_id, brick_origin='get_page.process_message.read_brick', brick_type='for_vertex_ai')
        # page = children_metadata['page']
        # num_friends_per_page = children_metadata['num_friends_per_page']
        message = finished_fof_message
    elif brick_type == 'getfriends':
        try:
            getfriends_pubsub_spanner_response = stream_pubsub_record_to_spanner(inbound_brick, spanner_db)
            print(f"succesfully wrote getfriends_pubsub_spanner_response: {getfriends_pubsub_spanner_response}")
            print("Trying brick_type == getfriends...")

            # {'username': 'ClawedCrypto',
            # 'page': 2,
            # 'num_pages': 7,
            # 'num_friends_per_page': 50,
            # 'metadata_dict': {'num_friends': 308,
            # 'num_followers': 148,
            # 'num_tweets': 18,
            # 'num_crush': 0,
            # 'num_crushed_on': 0,
            # 'creation_date': DatetimeWithNanoseconds(2022, 4, 18, 15, 49, 45, tzinfo=datetime.timezone.utc),
            # 'twitter_url': 'https://www.twitter.com/ClawedCrypto',
            # 'bio': 'We have our claws in everything crypto building the Web3 future | @SirDaveAnderson | @TheDonManNFT | Proud Partners of @ArtsyMonke | @BeaniezNFT | @SuperBeaniez',
            # 'location': '',
            # 'website': 'discord.gg/VvyKEt4Yaj',
            # 'list_name': 'Clawed Crypto',
            # 'errors': 0,
            # 'max_errors': 12,
            # 'username': 'ClawedCrypto',
            # 'userid': 1516081070524956674,
            # 'last_tweet': DatetimeWithNanoseconds(2022, 8, 13, 6, 4, 5, 728386, tzinfo=datetime.timezone.utc),
            # 'last_checked': DatetimeWithNanoseconds(2022, 8, 15, 6, 4, 5, 728620, tzinfo=datetime.timezone.utc),
            # 'parent_account': 'dubzyxbt',
            # 'username_char_ind': 3,
            # 'brick_batch_uuid': '09298ca7-9f34-4148-b40f-9099d2f30e0f',
            # 'account_type_str': 'project'}}

            get_friends_dict = brick_data
            username = get_friends_dict['username']
            page = get_friends_dict['page']
            # {'username': username, 'page': page, 'num_pages': num_pages-1, 'num_followers_per_page': num_followers_per_page}
            num_friends_per_page = get_friends_dict['num_friends_per_page']

            metadata_result = get_fof_bs4_page_limit(username, page, num_friends_per_page, timeout_lim=20,metadata_requests=MetadataRequests().all_reqs, how='parent_account')

            num_batches_mf = len(metadata_result)
            if num_batches_mf > 20:
                num_batches_mf = 20
            # write followers of accounts to master_fof table
            if 'one_off' in get_friends_dict.keys():
                print('Writing one_off results to graph_expanded')
                table_id_graph_expanded = "sneakyscraper.scrape_whotwi.graph_expanded"
                stream_to_graph_expanded = batch_stream_bigquery(metadata_result, client, table_id_graph_expanded, num_batches=num_batches_mf) 
                message = 'getfriends_oneoff_finished'
            else:
                table_id_master_friends = "sneakyscraper.scrape_whotwi.master_friends_expanded"
                stream_to_master_friends_expanded = batch_stream_bigquery(metadata_result, client, table_id_master_friends, num_batches=num_batches_mf) 
                message = 'getfriends_finished'
        except Exception as e:
            print(f"problem with getfriends: {e}")
            message = 'getfriends_finished'
            pass
    
    elif brick_type == 'getfollowers':
        print("Trying brick_type == getfollowers...")
        get_followers_dict = brick_data
        username = get_followers_dict['username']
        page = get_followers_dict['page']
        # {'username': username, 'page': page, 'num_pages': num_pages-1, 'num_followers_per_page': num_followers_per_page}
        num_followers_per_page = get_followers_dict['num_followers_per_page']

        metadata_result = get_followers_bs4_page_limit(username, page, num_followers_per_page, timeout_lim=20,metadata_requests=MetadataRequests().all_reqs)

        num_batches_mf = len(metadata_result)
        if num_batches_mf > 20:
            num_batches_mf = 20
        # write followers of accounts to master_fof table
        table_id_master_followers = "sneakyscraper.scrape_whotwi.master_followers"
        stream_to_master_followers = batch_stream_bigquery(metadata_result, client, table_id_master_followers, num_batches=num_batches_mf) 


        # WRITING master_followers to temporary spanner table for lookup for alerts
        try:
            master_followers_spanner = convert_dict_to_list_spanner(metadata_result)
            test_mf_spanner = stream_to_master_followers_temp(master_followers_spanner, spanner_db)
            print(f"result writing master_followers_to_spanner: {test_mf_spanner}")
        except Exception as e:
            print(f"problem with writing master_friends to Spanner: {e}")
            pass

        try:
            master_followers_spanner_2 = convert_dict_to_list_spanner(metadata_result)
            test_mf_spanner_2 = stream_to_master_followers_temp_2(master_followers_spanner_2, spanner_db)
            print(f"result writing master_followers_to_spanner_2: {test_mf_spanner_2}")
        except Exception as e:
            print(f"problem with writing master_friends to Spanner2: {e}")
            pass

        print("Succesfully finished getfollowers...")

        instance_id = 'toto'
        database_id = 'scrape_whotwi'
        spanner_response = stream_pubsub_record_to_spanner(inbound_brick, spanner_db)
        # spanner_response = stream_pubsub_record_to_spanner(inbound_brick, instance_id, database_id)
        print(f"response from spanner getfollowers: {spanner_response}")

        # CHECK BATCH STATUS
        # After writing to brick_log, check to see if this was the last message
        is_last_brick_message = check_brick_batch(inbound_brick, client, spanner_db)
        if is_last_brick_message is True:
            if 'metadata_dict' in get_followers_dict:
                if 'account_type_str' in get_followers_dict['metadata_dict']:
                    try:
                        parent_metadata = get_followers_dict['metadata_dict']
                        print(f"parent_metadata sending to ozdaobot for toto alerts: {parent_metadata}")
                        project_id = "sneakyscraper"
                        topic_id = "totoalerts"
                        test_dispatch_message = dispatch_bricks([parent_metadata], project_id, topic_id, brick_origin="done_classifying", brick_type="new_account")
                        print(f"succesfully read metadata from pubsub: {test_dispatch_message}")
                    except Exception as e:
                        print(f"Problem sending message to totoalerts subscription: {e}")

        message = 'getfollowers_finished'
    else:
        # print('Unknown brick type received. Returning 200.')
        message = 'unknown_brick_type'


    return message


def stream_to_master_friends_temp(metadata_result, database):
    insert_errors = []
    values_to_write = []
    for username, metadata in metadata_result.items():
        num_friends = metadata['num_friends']
        num_followers = metadata['num_followers']
        num_tweets = metadata['num_tweets']
        num_crush = metadata['num_crush']
        num_crushed_on = metadata['num_crushed_on']
        creation_date = metadata['creation_date']
        if type(creation_date) is str:
            creation_date = datetime.datetime.fromisoformat(creation_date)
        twitter_url = metadata['twitter_url']
        bio = metadata['bio']
        location = metadata['location']
        website = metadata['website']
        list_name = metadata['list_name']
        errors = metadata['errors']
        max_errors = metadata['max_errors']
        username = metadata['username']
        userid = metadata['userid']
        last_tweet = metadata['last_tweet']
        if type(last_tweet) is str:
            last_tweet = datetime.datetime.fromisoformat(last_tweet)
        last_checked = metadata['last_checked']
        if type(last_checked) is str:
            last_checked = datetime.datetime.fromisoformat(last_checked)
        parent_account = metadata['parent_account']
        username_char_ind = metadata['username_char_ind']
        values_temp = [num_friends, num_followers, num_tweets, num_crush, num_crushed_on,
             creation_date, twitter_url, bio, location, website, list_name,
             errors, max_errors, username, userid, last_tweet, last_checked,
             parent_account, username_char_ind]
        values_to_write.append(values_temp)
    
    try:
        with database.batch() as batch:
            batch.insert_or_update(
                table="master_friends_temp",
                columns=("num_friends", "num_followers", "num_tweets", "num_crush", "num_crushed_on",
                     "creation_date", "twitter_url", "bio", "location", "website", "list_name",
                     "errors", "max_errors", "username", "userid", "last_tweet", "last_checked",
                     "parent_account", "username_char_ind"),
                values = values_to_write,
            )
    except GoogleAPICallError as e:
        insert_errors.append(e.message)
        pass
    
    return insert_errors


def stream_to_new_children_temp(metadata_new_account_list, children_metadata, database):
    insert_errors = []
    values_to_write = []
    for metadata in metadata_new_account_list:
        username = metadata['username']
        userid = metadata['userid']
        list_name = metadata['list_name']
        
        num_friends = metadata['num_friends']
        num_followers = metadata['num_followers']
        num_tweets = metadata['num_tweets']
        num_crush = metadata['num_crush']
        
        num_crushed_on = metadata['num_crushed_on']
        creation_date = metadata['creation_date']
        twitter_url = metadata['twitter_url']
        bio = metadata['bio']
        location = metadata['location']
        website = metadata['website']
        
        last_tweet = metadata['last_tweet']
        last_checked = metadata['last_checked']
        parent_account = metadata['parent_account']
        errors = metadata['errors']
        
        max_errors = metadata['max_errors']
        username_char_ind = metadata['username_char_ind']
        brick_batch_uuid = metadata['brick_batch_uuid']

        parent_userid = 0
        if 'userid' in children_metadata:
            parent_userid = children_metadata['userid']
        
        values_temp = [username, userid, list_name,
                       num_friends, num_followers, num_tweets, num_crush, 
                       num_crushed_on, creation_date, twitter_url, bio, 
                       location, website, last_tweet, last_checked,
                       parent_account, errors, max_errors, 
                       username_char_ind, brick_batch_uuid, parent_userid]
        values_to_write.append(values_temp)
    
    try:
        with database.batch() as batch:
            batch.insert_or_update(
                table="new_children_temp",
                columns=("username", "userid", "list_name",
                         "num_friends", "num_followers", "num_tweets", "num_crush", 
                         "num_crushed_on", "creation_date", "twitter_url", "bio", 
                         "location", "website", "last_tweet", "last_checked",
                         "parent_account", "errors", "max_errors",
                         "username_char_ind", "brick_batch_uuid", "parent_userid"),
                values = values_to_write,
            )
    except GoogleAPICallError as e:
        insert_errors.append(e.message)
        print(f"problem with stream_to_new_children_temp: {e}")
        pass
    
    return insert_errors


# Function to write master_followers to temporary table on spanner
def stream_to_master_followers_temp(master_followers_list, database):
    insert_errors = []
    values_to_write = []
    for metadata in master_followers_list:
        num_friends = metadata['num_friends']
        num_followers = metadata['num_followers']
        num_tweets = metadata['num_tweets']
        username = metadata['username']
        last_checked = metadata['last_checked']
        parent_account = metadata['parent_account']
        values_temp = [username, parent_account, num_followers, num_friends,
                         num_tweets, last_checked]
        values_to_write.append(values_temp)
    
    try:
        with database.batch() as batch:
            batch.insert_or_update(
                table="master_followers_temp",
                columns=("username", "parent_account", "num_followers", "num_friends",
                         "num_tweets", "last_checked"),
                values = values_to_write,
            )
    except GoogleAPICallError as e:
        insert_errors.append(e.message)
        pass
    
    return insert_errors

def stream_to_master_followers_temp_2(master_followers_list, database):
    insert_errors = []
    values_to_write = []
    for metadata in master_followers_list:
        num_friends = metadata['num_friends']
        num_followers = metadata['num_followers']
        num_tweets = metadata['num_tweets']
        username = metadata['username']
        last_checked = metadata['last_checked']
        parent_account = metadata['parent_account']
        userid = metadata['userid']
        values_temp = [username, parent_account, userid, num_followers, num_friends,
                         num_tweets, last_checked]
        values_to_write.append(values_temp)
    
    try:
        with database.batch() as batch:
            batch.insert_or_update(
                table="master_followers_temp_2",
                columns=("username", "parent_account", "userid", "num_followers", 
                         "num_friends", "num_tweets", "last_checked"),
                values = values_to_write,
            )
    except GoogleAPICallError as e:
        insert_errors.append(e.message)
        pass
    
    return insert_errors


# def stream_to_last_n_friends_temp(last_n_friends_list, instance_id, database_id):
def stream_to_last_n_friends_temp(last_n_friends_list, database):
    insert_errors = []
    # spanner_client = spanner.Client()
    # instance = spanner_client.instance(instance_id)
    # database = instance.database(database_id)
    values_to_write = []
    for metadata in last_n_friends_list:
        num_friends = metadata['num_friends']
        num_followers = metadata['num_followers']
        num_tweets = metadata['num_tweets']
        num_crush = metadata['num_crush']
        num_crushed_on = metadata['num_crushed_on']
        creation_date = metadata['creation_date']
        twitter_url = metadata['twitter_url']
        bio = metadata['bio']
        location = metadata['location']
        website = metadata['website']
        list_name = metadata['list_name']
        errors = metadata['errors']
        max_errors = metadata['max_errors']
        username = metadata['username']
        userid = metadata['userid']
        last_tweet = metadata['last_tweet']
        last_checked = metadata['last_checked']
        parent_account = metadata['parent_account']
        username_char_ind = metadata['username_char_ind']
        brick_batch_uuid = metadata['brick_batch_uuid']
        parent_userid = metadata['parent_userid']
        print(f'Got to parent_userid in stream_last_n_friends_temp: {parent_account} --> {parent_userid}')
        values_temp = [num_friends, num_followers, num_tweets, num_crush, num_crushed_on,
             creation_date, twitter_url, bio, location, website, list_name,
             errors, max_errors, username, userid, last_tweet, last_checked,
             parent_account, username_char_ind, brick_batch_uuid, parent_userid]
        values_to_write.append(values_temp)
    
    try:
        with database.batch() as batch:
            batch.insert_or_update(
                table="last_n_friends_temp",
                columns=("num_friends", "num_followers", "num_tweets", "num_crush", "num_crushed_on",
                     "creation_date", "twitter_url", "bio", "location", "website", "list_name",
                     "errors", "max_errors", "username", "userid", "last_tweet", "last_checked",
                     "parent_account", "username_char_ind", "brick_batch_uuid", "parent_userid"),
                values = values_to_write,
            )
    except GoogleAPICallError as e:
        insert_errors.append(e.message)
        pass
    
    return insert_errors

def return_inputs_fof_parallel(usernames_metadata):
    '''Returns a list of tuples for use in parallel calls to get_fof_bs4_page

    Inputs
    ------
    usernames_metadata : dict
        Parent account usernames with friend limit as num_friends

    Returns
    -------
    page_tuples : list
        List of tuples where list[entry][0] is username and list[entry][1] is 
        a whotwi page number
    
    
    '''
    # Gather metadata to create page counts, etc
    # usernames_metadata = return_metadata_fof_parallel(usernames, friend_limit)
    page_tuples = []
    for username in usernames_metadata.keys():
        num_friends = usernames_metadata[username]['num_friends']
        num_pages = return_num_pages(num_friends)
        if num_pages > 101:
            num_pages = 101
        for page in range(1, num_pages):
            page_tuples.append((username, page))



    return page_tuples


def return_num_pages(num_friends):
    ''' Function to return valid num_pages for range() calls when scraping fof

    Inputs
    ------
    num_friends : int
        Number of friends

    Returns
    -------
    num_pages : int
        Number of pages + 1 to retrieve given num_friends
    
    '''
    if num_friends%50 == 0:
        adder = 1
    else:
        adder = 2
        
    num_pages = num_friends//50 + adder
    return num_pages

def base_sizer(num_els, num_splits):
    '''Function to return size of lists to chunk.
    
    Inputs
    ------
    num_els : int
        Number of elements in the list or keys in the dictionary
        that is being chunked.
    num_splits : int
        Number of chunks to split the dictionary or list into.
        
    Returns
    -------
    base_size : int
        Length of each chunk except for the last chunk
    num_splits : int
        Modified num_splits to account for input errors
    
    '''
    if num_splits <= 0:
        num_splits = 1
    if num_splits >= num_els:
        base_size = 1
        num_splits = num_els
    elif num_splits < num_els:
        if num_els % num_splits == 0:
            base_size = num_els//num_splits
        else:
            base_size = num_els//num_splits + 1
    return (base_size, num_splits)

def base_indexer(num_els, num_splits):
    '''Function to return indices of slices for chunking dicts/lists
    
    Inputs
    ------
    num_els : int
        Number of elements in the list or keys in the dictionary
        that is being chunked.
    num_splits : int
        Number of chunks to split the dictionary or list into.
    
    Returns
    -------
    indices : tuple
        Tuple of indices. indices[0] < indices[1]
    
    '''
    base_size, num_splits = base_sizer(num_els, num_splits)
    indices = []
    start_idx = 0
    while len(indices) < num_splits:
        end_idx = start_idx + base_size
        idx_tuple = (start_idx, end_idx)
        indices.append(idx_tuple)
        start_idx = end_idx
    return indices

def dict_slice_by_idx(some_dict, idxs):
    '''Function to slice a dictionary by index.
    
    Inputs
    ------
    idxs : tuple
        Tuple of indices. idxs[0] < idxs[1]
    some_dict : dict
        Dictionary to take slice of
        
    Returns
    -------
    sub_dict : dict
        Dictionary sliced from some_dict
    
    '''
    sub_dict = {x[0]: x[1] for x in list(some_dict.items())[idxs[0]:idxs[1]]}
    return sub_dict

def list_chunker(some_list, num_splits):
    '''Function to split a list into num_splits chunks
    
    Inputs
    ------
    some_list : list
        List to split into chunks
    num_splits : int
        Number of chunks to split some_list into
        
    Returns
    -------
    chunked_dict : list
        List of sub lists of some_list. num_splits elements long.
    
    '''
    num_els = len(some_list)
    indices = base_indexer(num_els, num_splits)
    chunked_list = []
    for idxs in indices:
        sub_list = some_list[idxs[0]:idxs[1]]
        if len(sub_list) > 0:
            chunked_list.append(sub_list)

    return chunked_list

def dict_chunker(some_dict, num_splits):
    '''Function to split a dictionary into num_splits chunks
    
    Inputs
    ------
    some_dict : dict
        Dictionary to split into chunks
    num_splits : int
        Number of chunks to split some_dict into
        
    Returns
    -------
    chunked_dict : list
        List of sub dictionaries of some_dict. num_splits elements long.
    
    '''
    num_els = len(some_dict)
    indices = base_indexer(num_els, num_splits)
    chunked_dict = []
    for idxs in indices:
        sub_dict = dict_slice_by_idx(some_dict, idxs)
        if len(sub_dict) > 0:
            chunked_dict.append(sub_dict)

    return chunked_dict

def return_chunks(data, num_splits):
    '''Function to split up a dictionary or list into a list of sub dicts or sub lists.
    
    Inputs
    ------
    data : dict or list
        Dictionary or list of items
    num_splits : int
        Number of times to split the dictionary or list
    
    Returns
    -------
    chunked_data : list
        List of sub dicts or sub lists of data

    '''
    if type(data) is dict:
        chunked_data = dict_chunker(data, num_splits)
    if type(data) is list:
        chunked_data = list_chunker(data, num_splits)

    return chunked_data

def convert_dict_to_strs(flattened_fof_dict):
    '''Convert a dictionary to list of dicts and convert datetime objects to strings.

    Inputs
    ------
    flattened_fof_dict : dict
        Flattened dictionary of user metadata (each key is a username, each value is a dict of metadata)
    
    Returns
    -------
    list_of_dicts : list
        List of dicts for streaming to BigQuery
    
    '''

    list_of_dicts = []
    for key in flattened_fof_dict.keys():
        some_dict = flattened_fof_dict[key]
        new_dict = {}
        for key in some_dict.keys():
            if type(some_dict[key]) is datetime.datetime:
                new_dict[key] = some_dict[key].isoformat()
            elif type(some_dict[key]) is DatetimeWithNanoseconds:
                new_dict[key] = some_dict[key].isoformat()
            else:
                new_dict[key] = some_dict[key]
        list_of_dicts.append(new_dict)
    return list_of_dicts


def convert_dict_to_list_spanner(flattened_fof_dict):
    '''Convert a dictionary to list of dicts and convert datetime objects to strings.

    Inputs
    ------
    flattened_fof_dict : dict
        Flattened dictionary of user metadata (each key is a username, each value is a dict of metadata)
    
    Returns
    -------
    list_of_dicts : list
        List of dicts for streaming to BigQuery
    
    '''

    list_of_dicts = []
    for key in flattened_fof_dict.keys():
        some_dict = flattened_fof_dict[key]
        new_dict = {}
        for key in some_dict.keys():
            new_dict[key] = some_dict[key]
        list_of_dicts.append(new_dict)
    return list_of_dicts


def batch_stream_bigquery(flattened_fof_dict, client, table_id, num_batches=20):
    '''Stream data to BigQuery in batches.

    Inputs
    ------
    flattened_fof_dict : dict
        Flattened dictionary of user metadata (each key is a username, each value is a dict of metadata)
    client : bigquery.Client()
        BigQuery Client object
    table_id : str
        BigQuery Table ID
    num_batches : int
        Approximate number of batches to split the list_of_dicts into for streaming

    Returns
    -------
    errors : list
        List of errors thrown when trying to stream JSON data to BigQuery
    
    '''

    # print(f"Stream to BQ flattened_fof_dict: {flattened_fof_dict}")
    list_of_dicts = convert_dict_to_strs(flattened_fof_dict)
    # print(f'Stream to BQ length of list_of_dicts: {len(list_of_dicts)}')
    # print(f"Stream to BQ list_of_dicts.keys(): {list_of_dicts}")
    if num_batches > len(list_of_dicts):
        num_batches = len(list_of_dicts)
    if num_batches == 0:
        num_batches = 1
    
    if len(list_of_dicts) > 0:
    # print(f"Stream to BQ num_batches: {num_batches}")
        chunked_list_of_dicts = return_chunks(list_of_dicts, num_batches)
        # print(f"Stream to BQ chunked_list_of_dicts: {chunked_list_of_dicts}")
        # print(f"Stream to BQ len(chunked_list_of_dicts): {len(chunked_list_of_dicts)}")
        for sublist in chunked_list_of_dicts:
            errors = client.insert_rows_json(table_id, sublist)  # Make an API request.
            if errors == []:
                pass
            else:
                print(f"Encountered errors while inserting rows: {errors} to {table_id}")
    else:
        errors = ['nothing to write']
    return errors