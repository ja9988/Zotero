from external_funcs import MetadataRequests, check_twitter_metadata
import base64
import json
from twitter_api_tools import get_metadata_tapi
from send_brick import dispatch_bricks
import datetime
import string
import wordninja
import os
import requests
import time
import traceback
from google.cloud import spanner
from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from getfriends_calcs import return_inputs_get_friends_parallel

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


# def stream_pubsub_record_to_spanner(brick_json, instance_id, database_id):
#     if type(brick_json['message']['publish_time']) is datetime.datetime:
#         time_received = brick_json['message']['publish_time'].isoformat() + 'Z'
#     else:
#         time_received = brick_json['message']['publish_time']

#     brick_batch_uuid = brick_json['message']['attributes']['brick_batch_uuid']
#     brick_batch_total = brick_json['message']['attributes']['brick_batch_total']
#     brick_number = brick_json['message']['attributes']['brick_number']
#     brick_origin = brick_json['message']['attributes']['brick_origin']
#     brick_type = brick_json['message']['attributes']['brick_type']
#     pubsub_message_id = brick_json['message']['message_id']
#     pubsub_message_id_hash = hash(pubsub_message_id)

    
#     spanner_client = spanner.Client()
#     instance = spanner_client.instance(instance_id)
#     database = instance.database(database_id)

#     with database.batch() as batch:
#         batch.insert(
#             table="brick_log",
#             columns=("time_received", "brick_batch_uuid", "brick_batch_total", "brick_number",
#                     "brick_origin", "brick_type", "pubsub_message_id", "pubsub_message_id_hash"),
#             values=[
#                 (time_received, brick_batch_uuid, brick_batch_total, brick_number, 
#                  brick_origin, brick_type, pubsub_message_id, pubsub_message_id_hash),
#             ],
#         )

#     return "pass"

def revbits(x):
    return int(bin(x)[2:].zfill(64)[::-1], 2)
    
# # def stream_pubsub_record_to_spanner(brick_json, instance_id, database_id):
# def stream_pubsub_record_to_spanner(brick_json, database):
#     if type(brick_json['message']['publish_time']) is datetime.datetime:
#         time_received = brick_json['message']['publish_time'].isoformat() + 'Z'
#     else:
#         time_received = brick_json['message']['publish_time']

#     brick_batch_uuid = brick_json['message']['attributes']['brick_batch_uuid']
#     brick_batch_total = brick_json['message']['attributes']['brick_batch_total']
#     brick_number = brick_json['message']['attributes']['brick_number']
#     brick_origin = brick_json['message']['attributes']['brick_origin']
#     brick_type = brick_json['message']['attributes']['brick_type']
#     pubsub_message_id = brick_json['message']['message_id']
#     # pubsub_message_id_hash = hash(pubsub_message_id)
#     pubsub_message_id_reverse = str(revbits(int(pubsub_message_id)))

    
#     # spanner_client = spanner.Client()
#     # instance = spanner_client.instance(instance_id)
#     # database = instance.database(database_id)

#     with database.batch() as batch:
#         batch.insert(
#             table="brick_log",
#             columns=("time_received", "brick_batch_uuid", "brick_batch_total", "brick_number",
#                     "brick_origin", "brick_type", "pubsub_message_id", "pubsub_message_id_reverse"),
#             values=[
#                 (time_received, brick_batch_uuid, brick_batch_total, brick_number, 
#                  brick_origin, brick_type, pubsub_message_id, pubsub_message_id_reverse),
#             ],
#         )

#     return "pass"
    
def decode_data_dict(data_dict):
    '''Decode the base64 encoded dictionary that makes up a TOTO brick message.
    
    Inputs
    ------
    data_dict  : base64 encoded string
        Data part of a pub/sub message

    Returns
    -------
    the_dict : dict
        Decoded data dict from brick message. Depending
        on the TOTO service this can contain page tuples,
        usernames, or anything really that is being passed
        to a different service via pub/sub
    
    '''
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

def stream_last_parent_log(inbound_brick, decoded_last_parent_brick, client):
    '''Straem last_parent message to table for checking and preventing duplicates
    
    
    '''
    last_parent_record = {}
    last_parent_record['time_received'] = inbound_brick['message']['publish_time']
    last_parent_record['brick_batch_uuid'] = inbound_brick['message']['attributes']['brick_batch_uuid']
    last_parent_record['brick_batch_total'] = inbound_brick['message']['attributes']['brick_batch_total']
    last_parent_record['brick_number'] = inbound_brick['message']['attributes']['brick_number']
    last_parent_record['brick_origin'] = inbound_brick['message']['attributes']['brick_origin']
    last_parent_record['last_parent_uuid'] = decoded_last_parent_brick['last_parent_uuid']
    last_parent_record['pubsub_message_id'] = inbound_brick['message']['message_id']

    errors = stream_to_bq(last_parent_record, 'sneakyscraper.scrape_whotwi.last_parent_log', client)

    return errors

def stream_last_child_log(inbound_brick, decoded_last_child_brick, client):
    '''Stream last_child message to table for checking and preventing duplicates
    
    
    '''
    last_child_record = {}
    last_child_record['time_received'] = inbound_brick['message']['publish_time']
    last_child_record['brick_batch_uuid'] = inbound_brick['message']['attributes']['brick_batch_uuid']
    last_child_record['brick_batch_total'] = inbound_brick['message']['attributes']['brick_batch_total']
    last_child_record['brick_number'] = inbound_brick['message']['attributes']['brick_number']
    last_child_record['brick_origin'] = inbound_brick['message']['attributes']['brick_origin']
    last_child_record['last_child_uuid'] = decoded_last_child_brick['last_child_uuid']
    last_child_record['pubsub_message_id'] = inbound_brick['message']['message_id']

    errors = stream_to_bq(last_child_record, 'sneakyscraper.scrape_whotwi.last_child_log', client)

    return errors

# OLD
# def get_for_master_temp(last_child_uuid, client):
#     sql = f"""
#     SELECT *
#     FROM sneakyscraper.scrape_whotwi.for_master_temp
#     WHERE
#     brick_batch_uuid = '{last_child_uuid}';
#     """
#     try:
#         query_job = client.query(sql)
#         records = [dict(row) for row in query_job]
#     except Exception as e:
#         records = []
#         print(f"Error with check_pubsub_message_id: {e}")
#         pass

#     return records

# NEW
# def get_for_master_temp(last_child_uuid, instance_id, database_id):
def get_for_master_temp(last_child_uuid, database):
    
    # spanner_client = spanner.Client()
    # instance = spanner_client.instance(instance_id)
    # database = instance.database(database_id)
    results_list = []
    
    # Execute SQL query
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            "SELECT * "
            "FROM for_master_temp "
            "WHERE brick_batch_uuid = @last_child_uuid;",
            params={"last_child_uuid": last_child_uuid},
            param_types={"last_child_uuid": spanner.param_types.STRING},
        )
        # Store values in a list
        for row in results:
            results_list.append(row)

    # Get the schema field names
    fields_queried = list(x.name for x in results.metadata.row_type.fields)
    
    # Label the returned values using the schema
    result_dicts = []
    for result in results_list:
        result_dict = {}
        for field_name, value in zip(fields_queried, result):
            result_dict[field_name] = value
        result_dicts.append(result_dict)

    return result_dicts

# def get_user_from_master_temp(username, client):
#     sql = f"""
#     SELECT *
#     FROM sneakyscraper.scrape_whotwi.for_master_temp
#     WHERE
#     username = '{username}'
#     LIMIT 1;
#     """
#     try:
#         query_job = client.query(sql)
#         records = [dict(row) for row in query_job]
#     except Exception as e:
#         records = []
#         print(f"Error with check_pubsub_message_id: {e}")
#         pass

#     return records

# def get_user_from_master_temp(username, instance_id, database_id):
def get_user_from_master_temp(username, database):
    
    # spanner_client = spanner.Client()
    # instance = spanner_client.instance(instance_id)
    # database = instance.database(database_id)
    results_list = []

    # Execute SQL query
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            "SELECT * "
            "FROM for_master_temp@{FORCE_INDEX=master_temp_by_username} "
            "WHERE username = @username "
            "LIMIT 1;",
            params={"username": username},
            param_types={"username": spanner.param_types.STRING},
        )
        # Store values in a list
        for row in results:
            results_list.append(row)

    # Get the schema field names
    fields_queried = list(x.name for x in results.metadata.row_type.fields)
    
    # Label the returned values using the schema
    result_dicts = []
    for result in results_list:
        result_dict = {}
        for field_name, value in zip(fields_queried, result):
            result_dict[field_name] = value
        result_dicts.append(result_dict)

    return result_dicts

def get_fof_for_vertex(username, client):
    sql = f"""SELECT
    *
    FROM
    `sneakyscraper.scrape_whotwi.master_friends`
    WHERE
    parent_account = '{username}'"""
    try:
        query_job = client.query(sql)
        records = [dict(row) for row in query_job]
    except Exception as e:
        records = []
        print(f"Error with get_fof_for_vertex: {e}")
        pass

    return records


# COMPLETE - used in classifybricks
def get_fof_for_vertex_spanner(username_parent, database):
    results_list = []
    
    # Execute SQL query
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            "SELECT * "
            "FROM master_friends_temp@{FORCE_INDEX=master_friends_by_parent} "
            "WHERE parent_account = @username_parent;",
            params={"username_parent": username_parent},
            param_types={"username_parent": spanner.param_types.STRING},
        )
        # Store values in a list
        for row in results:
            results_list.append(row)

    # Get the schema field names
    fields_queried = list(x.name for x in results.metadata.row_type.fields)
    
    # Label the returned values using the schema
    result_dicts = []
    for result in results_list:
        result_dict = {}
        for field_name, value in zip(fields_queried, result):
            result_dict[field_name] = value
        result_dicts.append(result_dict)

    return result_dicts


def transform_for_master(classified_for_master):
    dict_for_master = {}
    for username, metadata in classified_for_master.items():
        for_master_dict = {}
        for_master_dict['num_friends'] = metadata['num_friends']
        for_master_dict['num_followers'] = metadata['num_followers']
        for_master_dict['num_tweets'] = metadata['num_tweets']
        for_master_dict['num_crush'] = metadata['num_crush']
        for_master_dict['num_crushed_on'] = metadata['num_crushed_on']
        for_master_dict['creation_date'] = metadata['creation_date']
        for_master_dict['twitter_url'] = metadata['twitter_url']
        for_master_dict['bio'] = metadata['bio']
        for_master_dict['location'] = metadata['location']
        for_master_dict['website'] = metadata['website']
        for_master_dict['list_name'] = metadata['list_name']
        for_master_dict['errors'] = metadata['errors']
        for_master_dict['max_errors'] = metadata['max_errors']
        for_master_dict['username'] = metadata['username']
        for_master_dict['userid'] = metadata['userid']
        for_master_dict['last_tweet'] = metadata['last_tweet']
        for_master_dict['last_checked'] = metadata['last_checked']
        for_master_dict['parent_account'] = metadata['parent_account']
        for_master_dict['account_type_str'] = metadata['account_type_str']
        for_master_dict['username_char_ind'] = metadata['username_char_ind']
        dict_for_master[username] = for_master_dict
    return dict_for_master

def transform_for_master_single(classified_for_vertex):
    for_master_dict_dict = {}
    for_master_dict = {}
    for_master_dict['num_friends'] = classified_for_vertex['num_friends']
    for_master_dict['num_followers'] = classified_for_vertex['num_followers']
    for_master_dict['num_tweets'] = classified_for_vertex['num_tweets']
    for_master_dict['num_crush'] = classified_for_vertex['num_crush']
    for_master_dict['num_crushed_on'] = classified_for_vertex['num_crushed_on']

    if type(classified_for_vertex['creation_date']) is datetime.datetime:
        for_master_dict['creation_date'] = classified_for_vertex['creation_date'].isoformat()
    elif type(classified_for_vertex['creation_date']) is DatetimeWithNanoseconds:
        for_master_dict['creation_date'] = classified_for_vertex['creation_date'].isoformat()
    elif type(classified_for_vertex['creation_date']) is str:
        for_master_dict['creation_date'] = classified_for_vertex['creation_date']

    # for_master_dict['creation_date'] = classified_for_vertex['creation_date']
    for_master_dict['twitter_url'] = classified_for_vertex['twitter_url']
    for_master_dict['bio'] = classified_for_vertex['bio']
    for_master_dict['location'] = classified_for_vertex['location']
    for_master_dict['website'] = classified_for_vertex['website']
    for_master_dict['list_name'] = classified_for_vertex['list_name']
    for_master_dict['errors'] = classified_for_vertex['errors']
    for_master_dict['max_errors'] = classified_for_vertex['max_errors']
    for_master_dict['username'] = classified_for_vertex['username']
    for_master_dict['userid'] = classified_for_vertex['userid']
    if type(classified_for_vertex['last_tweet']) is datetime.datetime:
        for_master_dict['last_tweet'] = classified_for_vertex['last_tweet'].isoformat()
    elif type(classified_for_vertex['last_tweet']) is DatetimeWithNanoseconds:
        for_master_dict['last_tweet'] = classified_for_vertex['last_tweet'].isoformat()
    elif type(classified_for_vertex['last_tweet']) is str:
        for_master_dict['last_tweet'] = classified_for_vertex['last_tweet']

    
    
    # for_master_dict['last_tweet'] = classified_for_vertex['last_tweet']

    if type(classified_for_vertex['last_checked']) is datetime.datetime:
        for_master_dict['last_checked'] = classified_for_vertex['last_checked'].isoformat()
    elif type(classified_for_vertex['last_checked']) is DatetimeWithNanoseconds:
        for_master_dict['last_checked'] = classified_for_vertex['last_checked'].isoformat()
    elif type(classified_for_vertex['last_checked']) is str:
        for_master_dict['last_checked'] = classified_for_vertex['last_checked']


    # for_master_dict['last_checked'] = classified_for_vertex['last_checked']
    for_master_dict['parent_account'] = classified_for_vertex['parent_account']
    for_master_dict['account_type_str'] = classified_for_vertex['account_type_str']
    for_master_dict['username_char_ind'] = classified_for_vertex['username_char_ind']
    for_master_dict_dict[for_master_dict['username']] = for_master_dict
    return for_master_dict_dict

def package_vertex_for_getpage(accounts_for_vertex):
    vertex_bricks = []
    for username in accounts_for_vertex.keys():
        username_char_ind = accounts_for_vertex[username]['username_char_ind']
        vertex_brick = {'username': username, 'username_char_ind': username_char_ind}
        vertex_bricks.append(vertex_brick)
    return vertex_bricks

def package_getfriends_for_getpage(accounts_for_vertex):
    getfriends_bricks = []
    
    for username in accounts_for_vertex.keys():
        if accounts_for_vertex[username]['num_friends'] > 50:
            getfriends_inputs = return_inputs_get_friends_parallel(accounts_for_vertex[username], 2, 500)
            if len(getfriends_inputs) > 0:
                for getfriends_brick in getfriends_inputs:
                    getfriends_brick_temp = {}
                    getfriends_bricks.append(getfriends_brick)
                
    return getfriends_bricks

def read_brick(inbound_brick, client, spanner_db):
    '''Read and direct an incoming brick.
    
    
    '''
    inbound_brick = transform_brick(inbound_brick)
    print(f'transformed brick: {inbound_brick}')
    brick_attributes = inbound_brick['message']['attributes']
    brick_data = inbound_brick['message']['data']
    brick_type = brick_attributes['brick_type']
    stream_pubsub_spanner_result = stream_pubsub_record_to_spanner(inbound_brick, spanner_db)
    print(f"stream_pubsub_spanner_result: {stream_pubsub_spanner_result}")
    if brick_type == 'for_master_temp_sort':
        # print('Grandparents brick received. Run grandparents logic.')
        decoded_for_master_temp_sort = brick_data
        last_child_uuid = decoded_for_master_temp_sort['last_child_uuid']
        print(f'for_master_temp_sort_received_were: {decoded_for_master_temp_sort}')

        # get master_temp
        # OLD
        # for_master_temp_list = get_for_master_temp(last_child_uuid, client)
        # NEW
    
        # for_master_temp_list = get_for_master_temp(last_child_uuid, instance_id, database_id)
        for_master_temp_list = get_for_master_temp(last_child_uuid, spanner_db)

        print(f"for_master_temp_list: {for_master_temp_list}")
        for_master_temp_dict = bq_list_to_dict(for_master_temp_list)
        print(f"for_master_temp_dict: {for_master_temp_dict}")
        classified_accounts = check_accounts(for_master_temp_dict, 'names_db.txt')
        accounts_classified = classified_accounts['get_metadata_dict']
        print(f"accounts_classified: {accounts_classified}")
        # split into accounts that passed filter and accounts that didnt
        accounts_for_vertex, classified_for_master = filt_accounts_by_metadata(accounts_classified)
        print(f"accounts_for_vertex: {accounts_for_vertex}")
        print(f"classified_for_master: {classified_for_master}")
        # write classified_for_master to master
        classified_for_master_clean = transform_for_master(classified_for_master)
        table_id_master_friends = "sneakyscraper.scrape_whotwi.master_clean"
        if len(classified_for_master_clean) > 0:
            stream_to_master = batch_stream_bigquery(classified_for_master_clean, client, table_id_master_friends) 
        


        # send accounts_for_vertex to getpage
        if len(accounts_for_vertex) > 0:
            # Send bricks to get friends for vertex fof classification
            vertex_bricks = package_vertex_for_getpage(accounts_for_vertex)
            project_id = "sneakyscraper"
            topic_id = "getpage"
            # Testing
            print(f"Would be dispatching vertex_fof vertex_bricks: {vertex_bricks}")

            # def dispatch_bricks(list_of_bricks, project_id, topic_id, brick_origin='parents_test', brick_type='parents'):
            dispatch_vertex_fof = dispatch_bricks(vertex_bricks, project_id, topic_id, 'classifybricks', 'vertex_fof')
            print(f"dispatched vertex_fof bricks: {vertex_bricks}, {dispatch_vertex_fof}")

            # Send more bricks to get the rest of the friends for graph database
            try:
                print("trying to package package_getfriends_for_getpage")
                getfriends_bricks = package_getfriends_for_getpage(accounts_for_vertex)
                project_id = "sneakyscraper"
                topic_id = "getpage"
                print(f"would be dispatching getfriends_bricks: {getfriends_bricks}")
                dispatch_getfriends = dispatch_bricks(getfriends_bricks, project_id, topic_id, 'classifybricks', 'getfriends')
                print(f"dispatched getfriends bricks: {getfriends_bricks}, {dispatch_getfriends}")
            except Exception as e:
                print(f"problem with getfriends_bricks: {e}")
                pass

            # dispatch_getfollowers_temp = dispatch_bricks(getfriends_bricks, project_id, topic_id, brick_origin='classifybricks', brick_type='getfriends')


        # sstatus = process_grandparents(decoded_for_master_temp_sort)
        # print(f"grandparents_processed_wer: {sstatus}")
        message = 'for_master_temp_sort'
    elif brick_type == 'for_vertex_ai':
        print('for_vertex_ai brick received. Run parents logic.')
        decoded_for_vertex_ai = brick_data
        username = decoded_for_vertex_ai['username']
        username_char_ind = decoded_for_vertex_ai['username_char_ind']

        # pull user metadata from for_master_temp
        # OLD
        # user_metadata = get_user_from_master_temp(username, client)

        # NEW
        instance_id = 'toto'
        database_id = 'scrape_whotwi'
        # user_metadata = get_user_from_master_temp(username, instance_id, database_id)
        user_metadata = get_user_from_master_temp(username, spanner_db)
        print(f"user_metadata: {user_metadata}")
        # pull fof from master_friends
        # fof_for_vertex = get_fof_for_vertex(username, client)
        fof_for_vertex = get_fof_for_vertex_spanner(username, spanner_db)
        print(f"username: {username}, fof_for_vertex: {fof_for_vertex}")

        # package things for Vertex AI
        # fof_for_vertex is a list of dicts
        test_vertex = convert_dict_to_vertex(fof_for_vertex, user_metadata, keys_to_keep=None)

        try:
            test_response = classify_users_ml(project='sneakyscraper', endpoint_id='865403611989934080', instances=test_vertex)
            print(f"vertex ai: test_response: {test_response}")
        except Exception as e:
            print(f"problem with classify_users_ml: {e}")

        for ml_classification in test_response:
            username = list(ml_classification.keys())[0]
            account_type_str_vertex = list(ml_classification.values())[0]


            old_metadata = user_metadata[0].copy()
            old_metadata['account_type_str'] = account_type_str_vertex

        
        for key in old_metadata.keys():
            if type(old_metadata[key]) is datetime.datetime:
                old_metadata[key] = old_metadata[key].isoformat()
            elif type(old_metadata[key]) is DatetimeWithNanoseconds:
                old_metadata[key] = old_metadata[key].isoformat()


        print(f'for_vertex_ai received were: {decoded_for_vertex_ai}')
        print(f'vertex_predictions: {old_metadata}')
               
        # Dispatch bricks to pagedispatch to get the followers

        if old_metadata['account_type_str'] in ['Crypto Project', 'NFT Project', 'Fund/DAO', 'Crypto Person', 'Non-Crypto Project', 'Non-Crypto Person']:

            try:
                new_old_metadata = old_metadata.copy()
                for key in new_old_metadata.keys():
                    if type(new_old_metadata[key]) is datetime.datetime:
                        new_old_metadata[key] = new_old_metadata[key].isoformat()
                    elif type(new_old_metadata[key]) is DatetimeWithNanoseconds:
                        new_old_metadata[key] = new_old_metadata[key].isoformat()

                project_id = "sneakyscraper"
                topic_id = "pagedispatch"
                dispatch_followers = dispatch_bricks([new_old_metadata], project_id, topic_id, brick_origin="done_classifying", brick_type="dispatch_alert_followers")
                print(f"succesfully dispatched getfollowers message from classifybricks: {dispatch_followers}, new_old_metadata: {new_old_metadata}")
            except Exception as e:

                # Problem sending message to totoalerts subscription: Object of 
                # type DatetimeWithNanoseconds is not JSON serializable


                print(f"Problem sending message to totoalerts subscription: {e}")
                pass


        # Try writing classification for master to master_test
        try:
            # Traceback (most recent call last): File "/app/process_message.py", line 471, 
            # in read_brick classified_for_master_clean = transform_for_master_single(new_old_meta
            # data) UnboundLocalError: local variable 'new_old_metadata' referenced before assign
            # ment
            classified_for_master_clean = transform_for_master_single(old_metadata)
            print(f"classified_for_master_clean: {classified_for_master_clean}")
            # table_id_master_test = "sneakyscraper.scrape_whotwi.master_test"
            table_id_master_test = "sneakyscraper.scrape_whotwi.master_clean"
            if len(classified_for_master_clean) > 0:
                stream_to_master = batch_stream_bigquery(classified_for_master_clean, client, table_id_master_test) 
            print(f"succesfully wrote vertex_classified account to master: {old_metadata}, {stream_to_master}")
        except Exception as error:
            print(f"Problem writing vertex classified to master_test")
            full_stack_trace = ''.join(traceback.format_exception(None, error, error.__traceback__))
            print(full_stack_trace)
            pass

        message = 'for_vertex_ai'

    else:
        # print('unknown_brick_type_received. Returning 200.')
        message = "unknown_brick_type_received. Returning 200."


    return message

# def revbits(x):
#     return int(bin(x)[2:].zfill(64)[::-1], 2)

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



def send_tg_msgs_with_url_buttons(token, chat_id, text):
    data = {'chat_id': chat_id, 
            'text': text,
           'parse_mode': 'HTML',
           }
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    response = requests.post(url, data=data)
    json_response = json.loads(response.text)
    time.sleep(2.0)
    return json_response


#-------------------------------------- Vertex ML ---------------------------
def vertex_predict(project, endpoint_id, instances, credentials=None, location=None, api_endpoint=None):
    '''
    
    Inputs
    ------
    project : str
        String identifier of GCP project
    endpoint_id : str
        String identifier of Vertex AI endpoint
    instances : dict
        Dictionary with "instances" key and list of dicts as value
    credentials: google.oauth2.service_account.Credentials
        Google Cloud Credentials
    location : str
        Vertex AI location/region
    api_endpoint : str
        Vertex AI API endpoint URL
        
    Returns
    -------
    predictions : proto.marshal.collections.repeated.RepeatedComposite
        Predictions from Vertex AI prediction API endpoint
    
    '''
    print("Inside vertex_predict")
    from google.cloud import aiplatform


    if api_endpoint is None:
        api_endpoint = "us-central1-aiplatform.googleapis.com"
    if location is None:
        location = "us-central1"
    print("vertex_pridct api_endpoint: {api_endpoint}, location: {location}")
    # API Reference: https://googleapis.dev/python/aiplatform/latest/aiplatform_v1/prediction_service.html
    # The AI Platform services require regional API endpoints.
    client_options = {"api_endpoint": api_endpoint}
    # Initialize client that will be used to create and send requests.
    # This client only needs to be created once, and can be reused for multiple requests.
    if credentials is not None:
        print("Vertex client with credentials")
        vclient = aiplatform.gapic.PredictionServiceClient(client_options=client_options, credentials=credentials)
    else:
        print("Vertex client without credentials")
        vclient = aiplatform.gapic.PredictionServiceClient(client_options=client_options)
    # The format of each instance should conform to the deployed model's prediction input schema.
    instances = instances['instances']
    endpoint = vclient.endpoint_path(
        project=project, location=location, endpoint=endpoint_id
    )
    response = vclient.predict(
        endpoint=endpoint, instances=instances,
    )
    predictions = response.predictions
    return predictions

def classify_users_ml(project, endpoint_id, instances, location=None, api_endpoint=None, credentials=None):
    '''
    
    Inputs
    ------
    project : str
        String identifier of GCP project
    endpoint_id : str
        String identifier of Vertex AI endpoint
    instances : dict
        Dictionary with "instances" key and list of dicts as value
    credentials: google.oauth2.service_account.Credentials
        Google Cloud Credentials
    location : str
        Vertex AI location/region
    api_endpoint : str
        Vertex AI API endpoint URL
        
    Returns
    -------
    list_of_predictions : list
        List of predicted user classifications.
    
    
    '''
    print("Inside classify_users_ml")
    
    if api_endpoint is None:
        api_endpoint = "us-central1-aiplatform.googleapis.com"
    if location is None:
        location = "us-central1"
    
    # Get predictions from Vertex AI API
    try:
        predictions = vertex_predict(project=project, endpoint_id=endpoint_id, 
                                 instances=instances, credentials=credentials,
                                location=location, api_endpoint=api_endpoint)
    except Exception as e:
        print(f'problem with classify_users_ml: {e}')
    # package predictions into a dictionary
    list_of_predictions = []
    for entry in predictions:
        temp_dict = {}
        for dict_items in entry.items():
            temp_dict[dict_items[0]] = dict_items[1]
        list_of_predictions.append(temp_dict)
    print(f"list_of_predictions classify_users_ml: {list_of_predictions}")
    return list_of_predictions

def convert_dict_to_vertex(fof_metadata, parent_metadata, keys_to_keep=None):
    '''
    
    Input
    -----
    fof_metadata : list of dicts (from bq)
        Friends of parents dictionary. Friends of friends (not flattened)
        metadata dictionary containing usernames of parents as keys and
        dictionaries of their children accounts metadata. The format should
        match the output format of get_fof_parallel().
    parent_metadata : list of dicts (hopefully just one)
        Dictionary of user metadata where keys are usernames and values are dictionaries of metadata

    Returns
    -------
    dict_for_vertex : dict
        Dictionary of metadata to send to Vertex API prediction endpoint.

    '''

    if keys_to_keep is None:
        keys_to_keep = ['username', 'list_name', 'bio', 'website', 'username_list_name_bio_website', 'fof_text']

    parent_metadata_ulbw = add_ulbw_str(parent_metadata[0])
    parent_w_fof = merge_metadata_fof_total(fof_metadata, parent_metadata_ulbw)

    vertex_metadata = []
    parent_metadata_single = parent_w_fof.copy()
    vertex_metadata_parent = {}  
    for key in keys_to_keep:
        vertex_metadata_parent[key] = parent_metadata_single[key]
    vertex_metadata.append(vertex_metadata_parent)

    dict_for_vertex = {'instances': vertex_metadata}

    return dict_for_vertex

def add_ulbw_str(parent_metadata, keys_to_add=None, separator=None, new_key=None):
    '''Add username_list_name_bio_website key to a dictionary.
    
    Inputs
    ------
    parent_metadata : dict
        Dictionary of user metadata where keys are usernames and values are 
        dictionaries of metadata.
    keys_to_add : list
        List of string identifiers of dictionary keys to add together
    separator : str
        String to add between added keys
    new_key : str
        Name of the new key in the dictionary (username_list_name_bio_website)

    Returns
    -------
    dict_with_ulbw : dict
        Dictionary of user metadata where keys are usernames and values are 
        dictionaries of metadata that now have the ulbw key.
    
    '''

    if keys_to_add is None:
        keys_to_add = ['username', 'list_name', 'bio', 'website']
    if separator is None:
        separator = ' '
    if new_key is None:
        new_key = 'username_list_name_bio_website'

    dict_with_ulbw = parent_metadata.copy()
    ulbw = ''
    for key in keys_to_add:
        ulbw += dict_with_ulbw[key] + separator
    ulbw = ulbw[:-1]
    dict_with_ulbw[new_key] = ulbw

    return dict_with_ulbw

def merge_metadata_fof_total(fof_lod, user_metadata):
    '''Return fof feature for crypto classifier.
    
    Inputs
    ------
    fof_lod : list of dicts
        List of dictionaries of friends of friends. Each dict is a metadata
        dictionary of profile data from BQ.
    user_metadata : list of dict (possibly more than 1 but should be only 1)
        Dictionary of metadata of "parents". Key is username, values are
        metadata for username.
        
    Returns
    -------
    merged_dicts : dict
        Dictionary containing user's metadata + fof_metadata feature
        as merged_dicts['username']['fof_text']
    
    '''
    
    fof_text = return_fof_ml_str(fof_lod)
    merged_dicts = user_metadata.copy()
    # for key in metadata_dict.keys():
    #     metadata_dict_temp = metadata_dict[key].copy()
    #     if key in fof_total.keys():
    #         fof_total_temp = fof_total[key]
    #     else:
    #         # sometimes get_fof doesn't work on all accounts
    #         fof_total_temp = {'username': key, 'fof_text': ' '}
    #     metadata_dict_temp.update(fof_total_temp)
    #     merged_dicts[key] = metadata_dict_temp
        
    merged_dicts['fof_text'] = fof_text

    return merged_dicts

def return_fof_ml_str(fof_lod):
    '''Return fof feature str for ML.
    
    Inputs
    ------
    fof_lod : list of dicts
        List of dictionaries of friends of friends. Each dict is a metadata
        dictionary of profile data from BQ.
        
    Returns
    -------
    fof_text : str
        fof_text for Vertex AI
    
    '''

    fof_text = ''
    for fof in fof_lod:
        temp_str = fof['username'] + ' ' + fof['list_name'] + ' ' + fof['bio'] + ' ' + fof['website']
        fof_text = fof_text + temp_str


    return fof_text

def package_friend_changes(friend_changes_dict, decoded_last_parent_brick):
    last_parent_uuid = decoded_last_parent_brick['last_parent_uuid']
    all_pages_temp = []
    for username in friend_changes_dict.keys():
        # pages_temp = []
        num_friends = friend_changes_dict[username]['num_friends']
        num_pages = return_num_pages(num_friends)
        if num_pages > 101:
            num_pages = 101
        for page in range(1, num_pages):
            num_friends_per_page = friends_per_page(page, num_friends)


            temp_dict = {'username': username, 'page': page, 'num_pages': num_pages-1, 
            'num_friends_per_page': num_friends_per_page, 'last_parent_uuid': last_parent_uuid}
            # pages.append((username, page))
            # pages_temp.append(temp_dict)

            all_pages_temp.append(temp_dict)


    return all_pages_temp

def filt_accounts_by_metadata(flattened_dict, filt_props=None):
    '''Return accounts in a dictionary filtered by filt_props.

    Inputs
    ------
    flattened_dict : dict
        Dictionary of user metadata where keys are usernames and values are dictionaries of metadata
    filt_props : dict
        Dictionary of properties to filter by (values must be <= to these values)

    Returns
    -------
    pass_filt_dict : dict
        Dictionary of user metadata that meets the conditions in filt_props
    fail_filt_dict : dict
        Dictionary of user metadata that didn't meet the conditions in filt props

    '''
    if filt_props is None:
        filt_props = {'num_followers': 20000000, 'num_tweets': 20000000, 'num_friends': 20000000}
    pass_filt_dict = {}
    fail_filt_dict = {}
    pass_thresh = len(filt_props)
    for username in flattened_dict.keys():
        account_f = flattened_dict[username]
        account_thresh = 0
        for condition, value in filt_props.items():
            if account_f[condition] <= value:
                account_thresh += 1
        if account_thresh == pass_thresh:
            pass_filt_dict[username] = account_f
        elif account_thresh != pass_thresh:
            fail_filt_dict[username] = account_f
            
    return pass_filt_dict, fail_filt_dict


def bq_list_to_dict(bq_list):
    '''Convert a BigQuery result list to a dictionary
    
    '''
    bq_dict = {}
    for sub_dict in bq_list:
        username = sub_dict['username']
        bq_dict[username] = sub_dict

    return bq_dict
        

def process_last_parent(decoded_last_parent, client):
    # Get UUID of the batch that is finishing
    last_parent_uuid = decoded_last_parent['last_parent_uuid']
    # Get parent metadata scraped in last batch
    last_n_friends_new_ = get_parent_batch(last_parent_uuid, client)
    last_n_friends_new = bq_list_to_dict(last_n_friends_new_)
    # Get last_n_friends from main table
    last_n_friends_recent = get_last_n_friends_bq_dict(client)
    # last_n_friends_recent = bq_list_to_dict(last_n_friends_recent_)

    # Compare new results with recent results
    changes_to_ship = detect_changes_dict(last_n_friends_new, last_n_friends_recent)


    return changes_to_ship

def detect_changes_dict(last_n_friends_dict, last_n_friends_bq_dict):
    '''Detect changes between BigQuery and Whotwi to target children accounts to scrape.

    Inputs
    ------
    last_n_friends_dict : dict
        Dictionary of metadata recently scraped from whotwi
    last_n_friends_bq_dict : dict
        Dictionary of metadata only containing num_friends from BigQuery.
    
    Returns
    -------
    changes_dict : dict
        Dictionary of metadata where keys are usernames and num_friends field in
        the values dictionary is the number of new friends added by that username
        key.
    
    '''
    # print(f"Calling detect_changes_dict with lastnfriends {len(last_n_friends_dict)} and lastnfriendsbq {len(last_n_friends_bq_dict)}")
    changes_dict = {}
    for username in last_n_friends_dict.keys():
        if username in last_n_friends_bq_dict.keys():
            num_friends_new = last_n_friends_dict[username]['num_friends']
            num_friends_old = last_n_friends_bq_dict[username]['num_friends']
            diff_num_friends = num_friends_new-num_friends_old
            if diff_num_friends > 0 and num_friends_old > 0:
                changes_dict[username] = {'num_friends': diff_num_friends}
        elif username not in last_n_friends_bq_dict.keys():
            changes_dict[username] = {'num_friends': 2}
    return changes_dict

def get_last_n_friends_bq_dict(client):
    '''Returns dictionary of latest parent account metadata (children of grandparent accounts) from BQ.

    Inputs
    ------
    client : bigquery.Client()
        BigQuery Client object

    Returns
    -------
    last_n_friends_recent : dict
        Dictionary of latest metadata of parent accounts
    
    '''

    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=2)
    tomorrow = today + datetime.timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    tomorrow_str = tomorrow.strftime("%Y-%m-%d")
    sql = f"""WITH UNIQUE_USERNAMES AS(
    SELECT username, num_friends, last_checked
    FROM sneakyscraper.scrape_whotwi.last_n_friends
    WHERE 
    last_checked BETWEEN TIMESTAMP('{yesterday_str}')
    AND TIMESTAMP('{tomorrow_str}')),
    RANKED_MESSAGES AS( 
    SELECT *, ROW_NUMBER() OVER (PARTITION BY username ORDER BY last_checked DESC) AS rn
    FROM UNIQUE_USERNAMES)
    SELECT * EXCEPT (rn) FROM RANKED_MESSAGES WHERE rn = 1;"""
    query_job = client.query(sql)
    last_n_friends_recent_list = [dict(row) for row in query_job]
    last_n_friends_recent = {}
    for entry in last_n_friends_recent_list:
        last_n_friends_recent[entry['username']] = entry
    return last_n_friends_recent


def get_parent_batch(last_parent_uuid, client):
    sql = f"""
    SELECT *
    FROM sneakyscraper.scrape_whotwi.last_n_friends_temp
    WHERE
    brick_batch_uuid = '{last_parent_uuid}';
    """
    try:
        query_job = client.query(sql)
        records = [dict(row) for row in query_job]
    except Exception as e:
        records = []
        print(f"Error with check_pubsub_message_id: {e}")
        pass

    return records

def get_children_batch(last_child_uuid, client):
    sql = f"""
    SELECT *
    FROM sneakyscraper.scrape_whotwi.new_children_temp
    WHERE
    brick_batch_uuid = '{last_child_uuid}';
    """
    try:
        query_job = client.query(sql)
        records = [dict(row) for row in query_job]
    except Exception as e:
        records = []
        print(f"Error with check_pubsub_message_id: {e}")
        pass

    return records


def check_last_parent_uuid(last_parent_uuid, client):
    sql = f"""SELECT COUNT(*) as num_messages
    FROM sneakyscraper.scrape_whotwi.last_parent_log
    WHERE 
    last_parent_uuid = '{last_parent_uuid}';"""
    try:
        query_job = client.query(sql)
        records = [dict(row) for row in query_job]
    except Exception as e:
        records = []
        print(f"Error with check_pubsub_message_id: {e}")
        pass

    return records


def check_last_child_uuid(last_child_uuid, client):
    sql = f"""SELECT COUNT(*) as num_messages
    FROM sneakyscraper.scrape_whotwi.last_child_log
    WHERE 
    last_child_uuid = '{last_child_uuid}';"""
    try:
        query_job = client.query(sql)
        records = [dict(row) for row in query_job]
    except Exception as e:
        records = []
        print(f"Error with check_pubsub_message_id: {e}")
        pass

    return records

def check_last_parent(decoded_last_parent, client):
    last_parent_uuid = decoded_last_parent['last_parent_uuid']
    last_parent_uuid_count = check_last_parent_uuid(last_parent_uuid, client)
    last_parent_count = last_parent_uuid_count[0]['num_messages']


    if last_parent_count == 0:
        is_first_last_parent = True
    else:
        is_first_last_parent = False

    return is_first_last_parent

def check_last_child(decoded_last_child, client):
    last_child_uuid = decoded_last_child['last_child_uuid']
    last_child_uuid_count = check_last_child_uuid(last_child_uuid, client)
    last_child_count = last_child_uuid_count[0]['num_messages']


    if last_child_count == 0:
        is_first_last_child = True
    else:
        is_first_last_child = False

    return is_first_last_child

    
    


def process_grandparents(decoded_grandparents):
    # Metadata to request from Twitter API
    metadata_requests = MetadataRequests().all_reqs
    # Twitter API Token
    bearer_token = os.environ.get('twitter_api_bearer_staging')
    # Get Parent metadata using the Twitter API V2
    twitter_metadata = get_metadata_tapi(decoded_grandparents, metadata_requests, bearer_token)
    print(f"Twitter metadata of grandparent accounts has length: {len(twitter_metadata)}")

    # Validate the Twitter API Metadata
    twitter_metadata_check = check_twitter_metadata(twitter_metadata, decoded_grandparents)
    if len(twitter_metadata_check) == 0:
        print('Twitter metadata was healthy and will be used to speed up crawling')
        print(twitter_metadata)
        fof_parent_input = twitter_metadata
    else:
        print('Unfortunately Twitter couldnt be scraped, proceeding with whotwi...')
        # fof_parent_input = decoded_grandparents
        # send another brick out

    # Generate brick messages for parent accounts
    username_page_tuples = return_inputs_fof_parallel(fof_parent_input)
    # Print out the tuples
    print(f"username page tuples are: {username_page_tuples}")

    try:
        project_id = "sneakyscraper"
        # topic_id = "pagedispatch"
        topic_id = "getpage"
        print("Trying gen_publisher_and_path")
        max_els = 20
        brick_batches = list_to_lol(username_page_tuples, max_els)
        print(f"number of brick_batches: {len(brick_batches)}")
        for brick_batch in brick_batches:
            dispatch_test = dispatch_bricks(brick_batch, project_id, topic_id)
            print(f"Tried to dispatch bricks: {dispatch_test}")

    except Exception as e:
        print(f"Problem with pub/sub gen_publisher_and_path! {e}")
        pass

    return "Sucess"


def return_inputs_fof_parallel(usernames_metadata):
    '''Returns a list of tuples for use in parallel calls to get_fof_bs4_page

    Inputs
    ------
    usernames_metadata : dict
        Parent account usernames with friend limit as num_friends

    Returns
    -------
    pages : list
        List of dictionaries where dict['username] is username and dict['page'] is 
        a whotwi page number
    
    
    '''
    # Gather metadata to create page counts, etc
    # usernames_metadata = return_metadata_fof_parallel(usernames, friend_limit)
    all_pages_temp = []
    for username in usernames_metadata.keys():
        # pages_temp = []
        num_friends = usernames_metadata[username]['num_friends']
        num_pages = return_num_pages(num_friends)
        if num_pages > 101:
            num_pages = 101
        for page in range(1, num_pages):
            num_friends_per_page = friends_per_page(page, num_friends)


            temp_dict = {'username': username, 'page': page, 'num_pages': num_pages-1, 'num_friends_per_page': num_friends_per_page}
            # pages.append((username, page))
            # pages_temp.append(temp_dict)

            all_pages_temp.append(temp_dict)


    return all_pages_temp

def list_to_lol(some_list, max_els):
    new_list = []
    if len(some_list) > max_els:
        for i in range(0,len(some_list),max_els):
            new_list.append(some_list[i:i+max_els])
    else:
        new_list = [some_list]
        
    return new_list 


def friends_per_page(page, num_friends):
    '''Calculate how many friends are expected per page
    
    '''
    max_friends = page*50
    if max_friends <= num_friends:
        num_friends_per_page = 50
    elif max_friends > num_friends:
        num_friends_per_page =  50 -(max_friends - num_friends)
        
    if num_friends_per_page < 0:
        num_friends_per_page = 0

    return num_friends_per_page

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

def usernames_str_check_master(flattened_dict):
    '''Return username string for BigQuery SQL query.

    Inputs
    ------
    flattened_dict : dict
        Dictionary of user metadata

    Returns
    -------
    usernames_str : str
        Substring for BigQuery SQL query
    
    '''
    usernames_str = ''
    for i, key in enumerate(flattened_dict):
        if i < (len(flattened_dict)-1):
            usernames_str += f"'{key}',"
        else:
            usernames_str += f"'{key}'"
    usernames_str = '(' + usernames_str + ')'
    return usernames_str

def char_inds_check_master(flattened_dict):
    '''Return character indices string for BigQuery SQL query.

    Inputs
    ------
    flattened_dict : dict
        Dictionary of user metadata

    Returns
    -------
    char_inds_str : str
        Substring for BigQuery SQL query
    
    '''
    
    usernames = list(flattened_dict.keys())
    char_inds = []
    for username in usernames:
        char_ind = flattened_dict[username]['char_ind']
        char_inds.append(char_ind)
    char_inds = list(set(char_inds))

    char_inds_str = ''
    for i in range(len(char_inds)):
        key = char_inds[i]
        if i < (len(char_inds)-1):
            char_inds_str += f"{key}, "
        else:
            char_inds_str += f"{key}"
    char_inds_str = '(' + char_inds_str + ')'


    return char_inds_str

def check_master(flattened_dict, client):
    '''Check master for usernames.

    Inputs
    ------
    flattened_dict : dict
        Dictionary of usernames we are checking
    client : bigquery.Client()
        BigQuery Client object

    Returns
    -------
    records : list
        List of records (each record is a dictionary) from master
    
    '''
    if len(flattened_dict) > 0:
        usernames = usernames_str_check_master(flattened_dict)
        numbers = char_inds_check_master(flattened_dict)
        sql = f"""WITH PARTITIONED_USERNAMES AS(
        SELECT username
        FROM sneakyscraper.scrape_whotwi.master_clean
        WHERE 
        username_char_ind in {numbers}),
        RANKED_MESSAGES AS( 
        SELECT * 
        FROM PARTITIONED_USERNAMES
        WHERE
        username IN {usernames})
        SELECT * FROM RANKED_MESSAGES;"""
        # print(sql)
        try:
            query_job = client.query(sql)
            records = [dict(row) for row in query_job]
        except Exception as e:
            records = []
            print(f"Error with check_master: {e}")
            pass
    else:
        records = []
    return records

def new_accounts(new_friends_flat, records_from_master):
    '''Function to return new accounts that weren't in master.

    Inputs
    ------
    new_friends_flat : dict
        Dictionary of user metadata from scraping whotwi for children's new friends
    records_from_master : list
        Records returned from BigQuery (list of dicts)

    Returns
    -------
    new_friends_dict : dict
        Dictionary of metadata for accounts not in master
    
    '''

    just_checked_list = list(new_friends_flat.keys())
    old_friends_list = [x['username'] for x in records_from_master]
    new_friends_list = list(set(just_checked_list) - set(old_friends_list))
    already_friends_list = list(set(old_friends_list) - set(new_friends_list))
    # old_friends_dict = bqlod_to_dict(records_from_master)

    if len(new_friends_list) > 0:
        new_friends_dict = {username: new_friends_flat[username] for username in new_friends_list}
    else:
        new_friends_dict = {}
    try:
        if len(already_friends_list) > 0:
            already_friends_dict = {username: new_friends_flat[username] for username in already_friends_list}
        else:
            already_friends_dict = {}
    except Exception as e:
        print(f'Failed to create already friends {e}')
        already_friends_dict = {}
    
    return new_friends_dict, already_friends_dict

def check_accounts(get_metadata_dict, names_db):
    '''
    
    Inputs
    ------
    get_metadata_dict : dict
        Dictionary of metadata from get_metadata (or fof transformed)
    names_db : str
        Path to plain text file of names
    
    Returns
    -------
    classified_accounts : dict
        Dictionary of classified accounts metadata
    
    
    '''
    project_accounts = {}
    people_accounts = {}
    crypto_people_accounts = {}
    nft_accounts = {}
    undetermined_accounts = {}
    for username in get_metadata_dict.keys():
        metadata = get_metadata_dict[username]
        username_check = check_account(username, metadata, names_db)
        
        if username_check['account_type_str'] == 'project':
            metadata['account_type_str'] = 'project'
            project_accounts[username] = metadata
            get_metadata_dict[username]['account_type_str'] = 'project'
        if username_check['account_type_str'] == 'person':
            metadata['account_type_str'] = 'person'
            people_accounts[username] = metadata
            get_metadata_dict[username]['account_type_str'] = 'person'
        if username_check['account_type_str'] == 'crypto_person':
            metadata['account_type_str'] = 'crypto_person'
            crypto_people_accounts[username] = metadata
            get_metadata_dict[username]['account_type_str'] = 'crypto_person'
        if username_check['account_type_str'] == 'nft':
            metadata['account_type_str'] = 'nft'
            nft_accounts[username] = metadata
            get_metadata_dict[username]['account_type_str'] = 'nft'
        if username_check['account_type_str'] == 'undetermined':
            metadata['account_type_str'] = 'undetermined'
            undetermined_accounts[username] = metadata
            get_metadata_dict[username]['account_type_str'] = 'undetermined'


    classified_accounts = {}
    classified_accounts['projects'] = project_accounts
    classified_accounts['people'] = people_accounts
    classified_accounts['nfts'] = nft_accounts
    classified_accounts['crypto_people'] = crypto_people_accounts
    classified_accounts['undetermined'] = undetermined_accounts
    classified_accounts['get_metadata_dict'] = get_metadata_dict
    # now should work
    return classified_accounts

def check_account(username, metadata, names_db):
    '''
    
    Inputs
    ------
    username : str
        String of twitter handle
    metadata : dict
        Dictionary of metadata associated with username
    names_db : str
        Path of first names database

    Returns
    -------
    account_type : str
        Type of account - can be 'project', 'person', 'crypto_person' or 'undetermined'
    
    '''
    crypto_count = 0
    person_count = 0
    nft_count = 0
    account_type = {}
    bio = metadata['bio']
    list_name = metadata['list_name']
    table = str.maketrans(dict.fromkeys(string.punctuation))


    parsed_username = parse_name(username)
    username_crypto_check = check_against(parsed_username, against='crypto_words')
    username_name_check = check_against(parsed_username, against=names_db)
    username_nft_check = check_against(parsed_username, against='nft_words')
    crypto_count += username_crypto_check
    person_count += username_name_check
    nft_count += username_nft_check
    account_type['username_crypto_check'] = username_crypto_check
    account_type['username_name_check'] = username_name_check
    account_type['username_nft_check'] = username_nft_check
    if len(bio) > 0:
        bio = bio.lower()
        bio = bio.translate(table)  
        parsed_bio = parse_name(bio)
        bio_crypto_check = check_against(parsed_bio, against='crypto_words')
        bio_people_check = check_against(parsed_bio, against='people_words')
        bio_nft_check = check_against(parsed_bio, against='nft_words')
        crypto_count += bio_crypto_check
        person_count += bio_people_check
        nft_count += bio_nft_check
        account_type['bio_crypto_check'] = bio_crypto_check
        account_type['bio_people_check'] = bio_people_check
        account_type['bio_nft_check'] = bio_nft_check
    if len(list_name) > 0:
        parsed_list_name = parse_name(list_name)
        list_name_crypto_check = check_against(parsed_list_name, against='crypto_words')
        list_name_name_check = check_against(parsed_list_name, against=names_db)
        list_name_nft_check = check_against(parsed_list_name, against='nft_words')
        crypto_count += list_name_crypto_check
        person_count += list_name_name_check
        nft_count += list_name_nft_check
        account_type['list_name_crypto_check'] = list_name_crypto_check
        account_type['list_name_name_check'] = list_name_name_check
        account_type['list_name_nft_check'] = list_name_nft_check
    
    account_type['crypto_count'] = crypto_count
    account_type['person_count'] = person_count
    account_type['nft_count'] = nft_count

    # if crypto_count > 0 and person_count == 0:
    #     account_type_str = 'project'
    # if crypto_count == 0 and person_count > 0:
    #     account_type_str = 'person'
    # if crypto_count > 0 and person_count > 0:
    #     account_type_str = 'crypto_person'
    # if crypto_count == 0 and person_count == 0:
    #     account_type_str = 'undetermined'

    if crypto_count > 0:
        if person_count == 0 and nft_count < crypto_count:
            account_type_str = 'project'
        elif nft_count > person_count or nft_count > crypto_count:
            account_type_str = 'nft'
        elif person_count > 0:
            account_type_str = 'crypto_person'
        else:
            account_type_str = 'undetermined'
    elif crypto_count == 0:
        if nft_count > person_count:
            account_type_str = 'nft'
        elif person_count > nft_count:
            account_type_str = 'person'
        else:
            account_type_str = 'undetermined'
        
    
    account_type['account_type_str'] = account_type_str


    return account_type

def parse_name(name, min_len=3):
    '''Function to split name without spaces into list of substrings
    using wordninja.
    
    Inputs
    ------
    name : str
        username or list_name string to parse
    min_len : int
        Minimum length of substrings to return

    Returns
    -------
    parsed_name : list
        List of parsed username or list_name substrings
    
    '''

    parsed_name = wordninja.split(name)
    parsed_name = [x.lower() for x in parsed_name if len(x) >= min_len]
    # if word ninja doesn't return anything, simply return name in a list
    if not parsed_name:
        parsed_name = [name.lower()]

    return parsed_name

def check_against(parsed_name, against='crypto_words', force=None):
    '''Function to check a parsed_name against different collections of words.

    Inputs
    ------
    parsed_name : list or str
        List of substrings that have been parsed from username/list_name/bio
    against : str
        Indicate word collection. If not 'crypto_words' or 'people_words'
        we assume that against is a path name to a plain text file.

    Returns
    -------
    bool_check : bool
        Boolean indicating if parsed_name substrings in against
    
    
    
    '''

    crypto_words = ['dao', 'finance', 'crypto', 'protocol',
                    'defi', 'swap', 'capital', 'currency',
                    'exchange', 'dex', 'xyz', 'chain', 'liquidity',
                    'markets', 'gaming', 'ether', 'ethereum', 'labs', 'solana',
                    'Play-to-Earn', 'play', 'game']
    people_words = ['i', 'am', 'my', 'mine', 'she/her', 'he/him', 'me', 'mom', 
                       'dad', 'father', 'mother', 'alum', 'alumni', 'almnus',
                      'trader', 'blogger', 'degen', 'doctor', 'lawyer', 'citizen',
                      '(3,3)', 'ceo', 'developer', 'dev', 'etc', 'partner', 'husband',
                      'wife', 'analyst', 'scientist', 'engineer', 'tweeting', 'myself',
                      'studying', 'learning', 'passionate', 'enthusiast', 'phd', 'producer',
                      'writer', 'podcast', 'director', 'manager', 'interested',
                      'fiend', 'philosopher', 'artist', 'critic', 'fabulist', 'journalist',
                      'ape', 'student', 'im', 'musician', 'professor', 'intern', 'anon', '0x']
    nft_words = ['generative', 'nft', '8888', '10,000', '10000', '8,888', 'nfts', 'club',
                'collectibles', 'collectible', 'unique', 'algo-generated', 'art', 'artist',
                'metaplex', 'opensea', '6969', '7777', '3333', 'algogenerated','collection',
                'universe', 'generating', 'algogenerating', 'algogenerated', '3000', '3,000',
                '5000', '5,000']
    if against == 'crypto_words':
        bag_of_words = crypto_words
    elif against == 'people_words':
        bag_of_words = people_words
    elif against == 'nft_words':
        bag_of_words = nft_words
    else:
        with open(against) as f:
            bag_of_words = f.read().splitlines()
            bag_of_words = [x.lower() for x in bag_of_words]
    if type(parsed_name) is str:
        parsed_name = [parsed_name.lower()]

    if against == 'crypto_words' or against == 'nft_words':
        bool_sum = _loose_check(parsed_name, bag_of_words)
    else:
        bool_sum = _strict_check(parsed_name, bag_of_words)
    
    if force is not None:
        if force == 'strict':
            bool_sum = _strict_check(parsed_name, bag_of_words)
        if force == 'loose':
            bool_sum = _loose_check(parsed_name, bag_of_words)

    return bool_sum
        
def _loose_check(parsed_name, bag_of_words):
    '''Check loosely for any substring in parsed name against the
    bag_of_words. Will return match with any substring. Useful for checking
    against crypto words.

    Inputs
    ------
    parsed_name : list
        List of strings that we are checking
    bag_of_words : list
        List of strings of words that we are checking against loosely

    Returns
    -------
    bool_sum : int
        Number of matches
    
    '''
    bool_vals = []
    for substring in parsed_name:
        bool_val = any(x in substring.lower() for x in bag_of_words)
        bool_vals = bool_vals + [bool_val]
    bool_sum = sum(bool_vals)
    return bool_sum

def _strict_check(parsed_name, bag_of_words):
    '''Check stricly for any substring in parsed name against the
    bag_of_words. Requires exact match. Useful for looking at exact
    matches to names or people words.

    Inputs
    ------
    parsed_name : list
        List of strings that we are checking
    bag_of_words : list
        List of strings of words that we are checking against strictly

    Returns
    -------
    bool_sum : int
        Number of matches
    
    '''
    bool_vals = []
    for substring in parsed_name:
        bool_val = any(x == substring.lower() for x in bag_of_words)
        bool_vals = bool_vals + [bool_val]
    bool_sum = sum(bool_vals)
    return bool_sum


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
    return errors