from external_funcs import MetadataRequests
import base64
import json
from twitter_api_tools import get_metadata_tapi, check_twitter_metadata
from send_brick import dispatch_bricks
import datetime
import os
from google.cloud import spanner
from google.api_core.exceptions import GoogleAPICallError
from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from cm_testing import get_new_accounts_temp
from one_off_funcs import return_one_off_bricks
import random

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

# COMPLETE
def revbits(x):
    return int(bin(x)[2:].zfill(64)[::-1], 2)

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
    pubsub_message_id_reverse = str(revbits(int(pubsub_message_id)))
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

# COMPLETE
# def stream_to_for_master_temp(for_master_temp_dict, instance_id, database_id):
def stream_to_for_master_temp(for_master_temp_dict, database):
    insert_errors = []
    values_to_write = []
    for username, metadata in for_master_temp_dict.items():
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
        values_temp = [num_friends, num_followers, num_tweets, num_crush, num_crushed_on,
             creation_date, twitter_url, bio, location, website, list_name,
             errors, max_errors, username, userid, last_tweet, last_checked,
             parent_account, username_char_ind, brick_batch_uuid]
        values_to_write.append(values_temp)
    
    try:
        with database.batch() as batch:
            batch.insert_or_update(
                table="for_master_temp",
                columns=("num_friends", "num_followers", "num_tweets", "num_crush", "num_crushed_on",
                     "creation_date", "twitter_url", "bio", "location", "website", "list_name",
                     "errors", "max_errors", "username", "userid", "last_tweet", "last_checked",
                     "parent_account", "username_char_ind", "brick_batch_uuid"),
                values = values_to_write,
            )
    except GoogleAPICallError as e:
        insert_errors.append(e.message)
        pass
    
    return insert_errors
    
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
    '''Stream last_parent message to table for checking and preventing duplicates
    
    
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

def read_brick(inbound_brick, client, spanner_db):
    '''Read and direct an incoming brick.

    Inputs
    ------
    inbound_brick : dict
        Dictionary created from a pub/sub message JSON
        posted to a toto microservice.
    client : bigquery.Client()
        BigQuery Client object
    
    '''

    inbound_brick = transform_brick(inbound_brick)

    brick_attributes = inbound_brick['message']['attributes']
    brick_data = inbound_brick['message']['data']

    brick_type = brick_attributes['brick_type']
    brick_number = brick_attributes['brick_number']
    brick_batch_total = brick_attributes['brick_batch_total']
    brick_batch_uuid = brick_attributes['brick_batch_uuid']
    brick_origin = brick_attributes['brick_origin']
    

    # If the brick originates from Google cloud scheduler it will have a 
    # brick_type of 'grandparents_list'
    if brick_type == 'grandparents_list':
        # Process a list of grandparents received from pub/sub (via cloud scheduler)
        print(f'grandparents_received_were: {brick_data}')
        grandparents_status = process_grandparents(inbound_brick, spanner_db)
        print(f"grandparents_status: {grandparents_status}")
        message = 'grandparents'

    elif brick_type == 'parents_one_off':
        print(f'parents_one_off: {brick_data}')

        # brick deduplication
        try:
            parents_one_off_spanner = stream_pubsub_record_to_spanner(inbound_brick, spanner_db)
            print(f"succesfuly wrote parents_one_off_spanner to spanner: {parents_one_off_spanner}")
        except Exception as e:
            print(f"problem writing parents_one_off_spanner to spanner: {e}")
            pass

        try:
            # get bricks to send to getpage
            one_off_bricks = return_one_off_bricks(client)
            # randomize order so we don't do double scraping later
            random.shuffle(one_off_bricks)
            # send bricks to getpage
            project_id = "sneakyscraper"
            topic_id = "getpage"

            dispatch_for_master_temp = dispatch_bricks(one_off_bricks, project_id, topic_id, brick_origin='pagedispatch', brick_type='getfriends')
        except Exception as e:
            print(f"problem getting one_off_bricks: {e}")

        message = 'parents_one_off'
    elif brick_type == 'parents':
        print('Parents brick received. Run parents logic.')
        decoded_parents = decode_data_dict(brick_data)
        print(f'Parents received were: {decoded_parents}')
        message = 'parents'

    elif brick_type == 'last_parent':
        # Process the last_parent from a brick batch 
        # sent by getpage over pub/sub
        print(f'last_parent received was: {brick_data}')
        last_parent_status = process_last_parent(inbound_brick, client, spanner_db)
        print(f"last_parent_status: {last_parent_status}")
        message = "last_parent"

    elif brick_type == 'last_child':
        # print('Last parent brick received. Run last_parents logic.')
        decoded_last_child_brick = brick_data
        # print(f'last_parent_brick_received: {decoded_last_parent_brick}, now checking...')
        is_first_last_child = check_last_child(decoded_last_child_brick, client)
        if is_first_last_child is True:
            # Write to last_child_log first
            last_child_first = stream_last_child_log(inbound_brick, decoded_last_child_brick, client)
            # print(f"Just wrote last_child_first record to BigQuery: {last_child_first}")
            # Write to brick_log second
            # last_child_first_pubsub = stream_pubsub_record_to_bq(inbound_brick, 'sneakyscraper.scrape_whotwi.brick_log', client)
            try:
                spanner_response = stream_pubsub_record_to_spanner(inbound_brick, spanner_db)
                print(f"wrote first last_child pubsub message to spanner: {spanner_response}")
                # spanner_response = stream_pubsub_record_to_spanner(inbound_brick, instance_id, database_id)
            except Exception as e:
                # TODO: Might want to "return message=blahblah out of this function here 
                # if we can't write a record to spanner
                # because this pubsub message is a duplicate
                print(f"problem writing first last_child pubsub record to spanner: {e}")
                pass

            last_child_uuid = decoded_last_child_brick['last_child_uuid']
            child_batch_list = get_children_batch(last_child_uuid, spanner_db)

            child_batch_dict = bq_list_to_dict(child_batch_list)
            bearer_token = os.environ.get('twitter_api_bearer_staging')

            print('Attempting to get missing metadata for new accounts from Twitter API')
            # try:
            new_friends_tapi = get_metadata_tapi(list(child_batch_dict.keys()), MetadataRequests().all_reqs, bearer_token)
            child_batch_dict = merge_tapi_whotwi_dicts(new_friends_tapi, child_batch_dict)

            # CHECK AGAINST MASTER
            print(f"child_batch_dict keys for check_master: {child_batch_dict.keys()}")

            try:
                print('Attempting to use userid AND username...')
                accounts_for_master, already_friends_dict = get_new_accounts_temp(child_batch_dict, client, by='both')
                print(f"Succesfully used userids and usernames with new_accounts: {accounts_for_master.keys()}") #\
            except Exception as e:
                print(f"Problem with checking accounts using usernames and userids: {e}")
                pass

            # accounts_for_master, already_friends_dict = new_accounts(child_batch_dict, check_master_test)
            if len(accounts_for_master) > 0:
                print(f"accounts_for_master: {accounts_for_master}, already_friends_dict: {already_friends_dict}")
            else:
                print(f"already_friends_dict: {already_friends_dict}")

            # Write previously seen accounts to graph table
            # try:
            for_graph_dict = return_dict_for_graph(already_friends_dict)
            num_batches_g = len(for_graph_dict)
            if num_batches_g > 20:
                num_batches_g = 20

            print(f"for_graph_dict: {for_graph_dict}")

            # Production:
            table_id_graph = "sneakyscraper.scrape_whotwi.graph"
            stream_to_graph = batch_stream_bigquery(for_graph_dict, client, table_id_graph, num_batches=num_batches_g) 
            print(f"stream_to_graph: {stream_to_graph}")

            # Write new accounts to for_mater_temp table
            # try:
            for_master_temp_dict = accounts_for_master
            num_batches_mt = len(for_master_temp_dict)
            if num_batches_mt > 20:
                num_batches_mt = 20
            
            # stream to for_master_temp
            if len(for_master_temp_dict) > 0:
                stream_to_master_temp = stream_to_for_master_temp(for_master_temp_dict, spanner_db)

                print(f"stream_to_master_temp: {stream_to_master_temp}")

                # Production:
                # send message to classifybricks
                project_id = "sneakyscraper"
                topic_id = "classifybricks"
                classifybricks_brick = [{'last_child_uuid': last_child_uuid}]
                print(f"classifybricks_brick: {classifybricks_brick}")

                dispatch_for_master_temp = dispatch_bricks(classifybricks_brick, project_id, topic_id, brick_origin='pagedispatch', brick_type='for_master_temp_sort')

                # send message to page_dispatch
                # brick_message = {'username': 'username', 'num_followers': 10}
                # brick_type_gf = 'dispatch_getfollowers'
                # topic_id_pd = 'pagedispatch'
                # project_id = "sneakyscraper"

                # try:
                #     getfollowers_bricks = convert_dict_to_strs(for_master_temp_dict)
                #     dispatch_getfollowers_temp = dispatch_bricks(getfollowers_bricks, project_id, topic_id_pd, brick_origin='found_new_friends', brick_type=brick_type_gf)
                # except Exception as e:
                #     print(f"problem with getfollowers in early cascade")

                # # def dispatch_bricks(list_of_bricks, project_id, topic_id, brick_origin='children_test', brick_type='child'):
                print(f"dispatch_for_master_temp: {dispatch_for_master_temp}")

            # # print(f"child_batch from child: {child_batch}, type: {type(child_batch)}")
            # # print(f"friend_changes_list from child: {friend_changes_list}, type: {type(friend_changes_list)}")

            # send message to getpage
            message = f"first_last_child: {inbound_brick}, type: {type(inbound_brick)}, brick: {decoded_last_child_brick}, child_batch_dict: {child_batch_dict}"
        elif is_first_last_child is False:
            # print('last_child_seen_again... returning 200')
            # Write to last_child_log first
            last_child_second = stream_last_child_log(inbound_brick, decoded_last_child_brick, client)
            # print(f"Just wrote last_child_second record to BigQuery: {last_child_second}")
            # last_child_second_pubsub = stream_pubsub_record_to_bq(inbound_brick, 'sneakyscraper.scrape_whotwi.brick_log', client)
            try:
                instance_id = 'toto'
                database_id = 'scrape_whotwi'
                # spanner_response = stream_pubsub_record_to_spanner(inbound_brick, instance_id, database_id)
                spanner_response = stream_pubsub_record_to_spanner(inbound_brick, spanner_db)
            except Exception as e:
                print(f"problem writing second child pubsub record to spanner: {e}")

            # print(f"Just wrote last_child_second_pubsub record to BigQuery: {last_child_second_pubsub}")
            message = f"second_last_child: {decoded_last_child_brick}"
    elif brick_type == 'dispatch_getfollowers':
        try:
            dispatch_getfollowers_spanner = stream_pubsub_record_to_spanner(inbound_brick, spanner_db)
            print(f"succesfuly wrote dispatch_getfollowers_spanner to spanner: {dispatch_getfollowers_spanner}")
        except Exception as e:
            print(f"problem writing dispatch_getfollowers_spanner to spanner: {e}")
            pass
        project_id = "sneakyscraper"
        topic_id = "getpage"
        getfollowers_dict = brick_data
        print(f"getfollowers_dict: {getfollowers_dict}")
        getfollowers_bricks = return_inputs_get_followers_parallel(getfollowers_dict)
        print(f"getfollowers_bricks: {getfollowers_bricks}")

        dispatch_getfollowers_temp = dispatch_bricks(getfollowers_bricks, project_id, topic_id, brick_origin='pagedispatch/dispatch_getfollowers', brick_type='getfollowers')
        message = "dispatch_getfollowers brick"
    elif brick_type == 'dispatch_alert_followers':
        try:
            dispatch_getfollowers_alerts_spanner = stream_pubsub_record_to_spanner(inbound_brick, spanner_db)
            # print(f"succesfuly wrote dispatch_getfollowers_alerts_spanner to spanner: {dispatch_getfollowers_alerts_spanner}")
        except Exception as e:
            print(f"problem writing dispatch_getfollowers_alerts_spanner to spanner: {e}")
            pass

        project_id = "sneakyscraper"
        topic_id = "getpage"
        getfollowers_dict = brick_data
        # print(f"getfollowers_dict: {getfollowers_dict}")
        getfollowers_bricks = return_inputs_get_followers_parallel(getfollowers_dict)
        # print(f"getfollowers_bricks: {getfollowers_bricks}")

        dispatch_getfollowers_temp = dispatch_bricks(getfollowers_bricks, project_id, topic_id, brick_origin='pagedispatch/dispatch_getfollowers', brick_type='getfollowers')
        message = "dispatch_getfollowers brick from dispatch_alert_followers"

    else:
        # print('unknown_brick_type_received. Returning 200.')
        message = "unknown_brick_type_received. Returning 200."


    return message


def process_last_parent(inbound_brick, client, spanner_db):
    # Message received from getpage indicating that we've gotten all the metadata for all of the parents
        # Now 
    decoded_last_parent_brick =  inbound_brick['message']['data']

    print(f'last_parent_brick_received: {decoded_last_parent_brick}, now checking...')
    is_first_last_parent = check_last_parent(decoded_last_parent_brick, client)
    if is_first_last_parent is True:
        # Write to last_parent_log first
        last_parent_first = stream_last_parent_log(inbound_brick, decoded_last_parent_brick, client)
        print(f"Just wrote last_parent_first record to BigQuery: {last_parent_first}")
        # Write to brick_log second
        try:
            instance_id = 'toto'
            database_id = 'scrape_whotwi'
            # spanner_response = stream_pubsub_record_to_spanner(inbound_brick, instance_id, database_id)
            spanner_response = stream_pubsub_record_to_spanner(inbound_brick, spanner_db)
        except Exception as e:
            # TODO: Might want to "return message=blahblah out of this function here 
            # if we can't write a record to spanner
            print(f"problem writing first last parent pubsub record to spanner: {e}")
            pass

        # last_parent_first_pubsub = stream_pubsub_record_to_bq(inbound_brick, 'sneakyscraper.scrape_whotwi.brick_log', client)
        print(f"Just wrote last_parent_first_pubsub record to Spanner: {spanner_response}")
        # DO LAST PARENT ROUTINE HERE
        friend_changes = get_friend_changes(decoded_last_parent_brick, client, spanner_db)
        
        friend_changes_list = package_friend_changes(friend_changes, decoded_last_parent_brick)
        # write friend changes to temporary database
        project_id = "sneakyscraper"
        topic_id = "getpage"

        # Testing:
        # print(f" Would be dispatching friend_changes_list children bricks: {friend_changes_list}")

        # Production
        dispatch_children = dispatch_bricks(friend_changes_list, project_id, topic_id, brick_origin='children_test', brick_type='children')
        # def dispatch_bricks(list_of_bricks, project_id, topic_id, brick_origin='parents_test', brick_type='parents'):
        print(f"dispatch_children: {dispatch_children}, Sent friend_changes_list children bricks: {friend_changes_list}")

        # TODO: Update last_n_friends using last_parent_uuid and last_n_friends_temp
        # Write to last_n_friends from last_n_friends_temp for this last_parent_uuid

        # print(f"parent_batch from parent: {parent_batch}, type: {type(parent_batch)}")
        # print(f"friend_changes_list from parent: {friend_changes_list}, type: {type(friend_changes_list)}")

        # send message to getpage
        message = f"friend_changes_list from parent: {friend_changes_list}, brick: {decoded_last_parent_brick}"
    elif is_first_last_parent is False:
        # print('last_parent_seen_again... returning 200')
        # Write to last_parent_log first
        last_parent_second = stream_last_parent_log(inbound_brick, decoded_last_parent_brick, client)
        # print(f"Just wrote last_parent_second record to BigQuery: {last_parent_second}")
        try:
            instance_id = 'toto'
            database_id = 'scrape_whotwi'
            spanner_response = stream_pubsub_record_to_spanner(inbound_brick, spanner_db)
            # spanner_response = stream_pubsub_record_to_spanner(inbound_brick, instance_id, database_id)

            print("succesfully wrote second parent to spanner")
            # last_parent_second_pubsub = stream_pubsub_record_to_bq(inbound_brick, 'sneakyscraper.scrape_whotwi.brick_log', client)
        except Exception as e:
            print("problem streaming second_last_parent to spanner")
        # print(f"Just wrote last_parent_second_pubsub record to BigQuery: {last_parent_second_pubsub}")
        message = f"second_last_parent: {decoded_last_parent_brick}"

    return message
        

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
    errors = []
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

def return_dict_for_graph(already_friends_dict):
    dict_for_graph = {}
    for username, metadata in already_friends_dict.items():
        for_graph_dict = {}
        for_graph_dict['num_friends'] = metadata['num_friends']
        for_graph_dict['num_followers'] = metadata['num_followers']
        for_graph_dict['num_tweets'] = metadata['num_tweets']
        for_graph_dict['num_crush'] = metadata['num_crush']
        for_graph_dict['num_crushed_on'] = metadata['num_crushed_on']
        for_graph_dict['creation_date'] = metadata['creation_date']
        for_graph_dict['twitter_url'] = metadata['twitter_url']
        for_graph_dict['bio'] = metadata['bio']
        for_graph_dict['location'] = metadata['location']
        for_graph_dict['website'] = metadata['website']
        for_graph_dict['list_name'] = metadata['list_name']
        for_graph_dict['errors'] = metadata['errors']
        for_graph_dict['max_errors'] = metadata['max_errors']
        for_graph_dict['username'] = metadata['username']
        for_graph_dict['userid'] = metadata['userid']
        for_graph_dict['last_tweet'] = metadata['last_tweet']
        for_graph_dict['last_checked'] = metadata['last_checked']
        for_graph_dict['parent_account'] = metadata['parent_account']
        for_graph_dict['username_char_ind'] = metadata['username_char_ind']
        dict_for_graph[username] = for_graph_dict
    return dict_for_graph


def merge_tapi_whotwi_dicts(tapi_dict, whotwi_dict):
    '''Overwrite dictionary entries in a flat whotwi metadata dictionary with metadata
    from Twitter API.

    Inputs
    ------
    tapi_dict : dict
        Dictionary of metadata returned by get_metadata_tapi()
    whotwi_dict : dict
        Dictionary of metadata returned by get_fof and then flattened

    Returns
    -------
    whotwi_dict : dict
        Whotwi dict with values replaced by values in tapi_dict

    '''

    common_usernames = list(set(tapi_dict.keys()).intersection(set(whotwi_dict.keys())))

    for username in common_usernames:
        whotwi_temp = whotwi_dict[username].copy()
        tapi_temp = tapi_dict[username].copy()
        whotwi_temp['location'] = tapi_temp['location']
        whotwi_temp['creation_date'] = tapi_temp['creation_date']
        whotwi_dict[username] = whotwi_temp

    return whotwi_dict

def package_friend_changes(friend_changes_dict, decoded_last_parent_brick):
    last_parent_uuid = decoded_last_parent_brick['last_parent_uuid']
    all_pages_temp = []
    for username in friend_changes_dict.keys():
        # pages_temp = []
        num_friends = friend_changes_dict[username]['num_friends']
        userid = 0
        if 'userid' in friend_changes_dict[username]:
            userid = friend_changes_dict[username]['userid']
        num_pages = return_num_pages(num_friends)
        if num_pages > 101:
            num_pages = 101
        for page in range(1, num_pages):
            num_friends_per_page = friends_per_page(page, num_friends)


            temp_dict = {'username': username, 'userid': userid, 'page': page, 'num_pages': num_pages-1, 
            'num_friends_per_page': num_friends_per_page, 'last_parent_uuid': last_parent_uuid}
            # pages.append((username, page))
            # pages_temp.append(temp_dict)

            all_pages_temp.append(temp_dict)


    return all_pages_temp


def bq_list_to_dict(bq_list):
    '''Convert a BigQuery result list to a dictionary
    
    '''
    bq_dict = {}
    for sub_dict in bq_list:
        username = sub_dict['username']
        bq_dict[username] = sub_dict

    return bq_dict
        

def get_friend_changes(decoded_last_parent, client, spanner_db):
    '''
    
    Inputs
    ------
    decoded_last_parent : dict
        Dictionary of metadata associated with a last_parent
        brick message
    client : bigquery.Client()
        BigQuery client connector
    spanner_db : spanner.database
        Spanner database connector

    Returns
    -------
    changes_to_ship : dict
        Dictionary of metadata where keys are usernames and num_friends field in
        the values dictionary is the number of new friends added by that username
        key.

    
    '''
    # Get UUID of the batch that is finishing
    last_parent_uuid = decoded_last_parent['last_parent_uuid']

    # Get parent metadata scraped in last batch from SPANNER
    last_n_friends_new_ = get_parent_batch(last_parent_uuid, spanner_db)
    last_n_friends_new_ = clean_for_last_n_friends(last_n_friends_new_)

    last_n_friends_new = bq_list_to_dict(last_n_friends_new_)
    # Get last_n_friends from main table
    last_n_friends_recent = get_last_n_friends_bq_dict(client)
    # last_n_friends_recent = bq_list_to_dict(last_n_friends_recent_)

    # Compare new results with recent results
    changes_to_ship = detect_changes_dict(last_n_friends_new, last_n_friends_recent)

    try:
        lnf_copy_table_id = 'sneakyscraper.scrape_whotwi.last_n_friends_copy'
        write_last_n_friends_new_bq = batch_stream_bigquery(last_n_friends_new, client, lnf_copy_table_id)
        print(f"success with write_last_n_friends_new_bq: {write_last_n_friends_new_bq}")
    except Exception as e:
        print(f"write_last_n_friends_new_bq failed: {e}")
        pass

    return changes_to_ship

def clean_for_last_n_friends(list_of_dicts):
    new_list_of_dicts = []
    
    for metadata_temp in list_of_dicts:
        metadata = metadata_temp.copy()
        metadata.pop('brick_batch_uuid', None)
        metadata.pop('username_char_ind', None)
        new_list_of_dicts.append(metadata)
        
    return new_list_of_dicts

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
        userid = 0
        if 'userid' in last_n_friends_dict[username]:
            userid = last_n_friends_dict[username]['userid']
        if username in last_n_friends_bq_dict.keys():
            num_friends_new = last_n_friends_dict[username]['num_friends']
            num_friends_old = last_n_friends_bq_dict[username]['num_friends']
            diff_num_friends = num_friends_new-num_friends_old
            if diff_num_friends > 0 and num_friends_old > 0:
                changes_dict[username] = {'num_friends': diff_num_friends, 'userid': userid}
            elif diff_num_friends < 0 and num_friends_old > 0:
                if num_friends_new <= 50:
                    lookback = num_friends_new
                elif num_friends_new > 50:
                    lookback = 50
                changes_dict[username] = {'num_friends': lookback, 'userid': userid}
        elif username not in last_n_friends_bq_dict.keys():
            changes_dict[username] = {'num_friends': 2, 'userid': userid}
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
    SELECT username, num_friends, last_checked, userid
    FROM sneakyscraper.scrape_whotwi.last_n_friends_copy
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


def get_parent_batch(last_parent_uuid, database):

    results_list = []
    
    # Execute SQL query
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            "SELECT * "
            "FROM last_n_friends_temp "
            "WHERE brick_batch_uuid = @last_parent_uuid;",
            params={"last_parent_uuid": last_parent_uuid},
            param_types={"last_parent_uuid": spanner.param_types.STRING},
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

def get_children_batch(last_child_uuid, database):
    results_list = []
    
    # Execute SQL query
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            "SELECT * EXCEPT(parent_userid) "
            "FROM new_children_temp "
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
        print(f"Error with check_last_parent_uuid: {e}")
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

    
    


def process_grandparents(inbound_brick, database):
    '''Processes grandparent Twitter accounts (curated lists managed by mike)

    Inputs
    ------
    inbound_brick : dict
        Decoded inbound brick message containing 

    Returns
    -------
    status : str
        Status of processing grandparents
    
    '''

    # List of grandparent accounts from brick message
    decoded_grandparents = inbound_brick['message']['data']
    # Metadata to request from Twitter API
    metadata_requests = MetadataRequests().all_reqs
    # Twitter API Token
    bearer_token = os.environ.get('twitter_api_bearer_staging')

    try:
        grandparent_record = stream_pubsub_record_to_spanner(inbound_brick, database)
        print(f"succesfully wrote grandparent_record to spanner: {grandparent_record}")
    except Exception as e:
        print(f"problem writing grandparent_record to spanner: {e}")
        pass


    # Get the number of friends for each grandparent account so we can create the individual
    # brick messages we need to get the metadata for each parent account (friends followed
    # by each grandparent are called parents)
    twitter_metadata = get_metadata_tapi(decoded_grandparents, metadata_requests, bearer_token)
    print(f"Twitter metadata of grandparent accounts has length: {len(twitter_metadata)}")

    # Generate brick messages for parent accounts
    username_page_dicts = gen_dicts_getpage(twitter_metadata)
    # Print out the tuples
    print(f"username page dicts are: {username_page_dicts}")

    # Send the brick messages to getpage
    try:
        project_id = "sneakyscraper"
        topic_id = "getpage"
        brick_type = "parents"
        # Try sending bricks in sub batches in case any messages are dropped
        # This can be fixed by using getmetadata in the future
        max_els = 40
        brick_batches = list_to_lol(username_page_dicts, max_els)
        print(f"number of brick_batches: {len(brick_batches)}")
        for brick_batch in brick_batches:
            # Testing
            # print(f"Would be sending brick_batch: {brick_batch}")

            # Production
            # Parents sent to getpage have a brick_type of 'parents'
            dispatch_test = dispatch_bricks(brick_batch, project_id, topic_id, brick_origin='parents_test', brick_type=brick_type)
            print(f"Just sent brick_batch: {brick_batch}, {dispatch_test}")
        status = "process_grandpares(): Success"

    except Exception as e:
        status = f"Problem sending brick messages from process_grandparents(): {e}"
        pass

    return status


def gen_dicts_getpage(usernames_metadata):
    '''Returns a list of dicts for use in brick messages
    sent to getpage.

    Inputs
    ------
    usernames_metadata : dict
        Parent account usernames with friend limit as num_friends

    Returns
    -------
    all_pages : list
        List of dictionaries where dict['username] is username and dict['page'] is 
        a whotwi page number, dict['num_pages'] is the expected number of pages
        for scraping, dict['num_friends_per_page'] is the expected number of
        friends to scrape from that particular page
    
    '''
    # Gather metadata to create page counts, etc
    # usernames_metadata = return_metadata_fof_parallel(usernames, friend_limit)
    all_pages = []
    for username in usernames_metadata.keys():
        num_friends = usernames_metadata[username]['num_friends']
        userid = 0
        if 'userid' in usernames_metadata[username]:
            userid = usernames_metadata[username]['userid']
            print(f"found userid of grandparent: {username} --> {userid}")
        num_pages = return_num_pages(num_friends)
        if num_pages > 101:
            num_pages = 101
        for page in range(1, num_pages):
            num_friends_per_page = friends_per_page(page, num_friends)

            temp_dict = {'username': username, 'userid': userid, 
            'page': page, 'num_pages': num_pages-1, 
            'num_friends_per_page': num_friends_per_page}

            all_pages.append(temp_dict)


    return all_pages

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

def return_inputs_get_followers_parallel(metadata_dict, max_followers=500):
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

    for key in metadata_dict.keys():
        if type(metadata_dict[key]) is datetime.datetime:
            metadata_dict[key] = metadata_dict[key].isoformat()

    all_pages_temp = []
    username = metadata_dict['username']
        # pages_temp = []
    num_followers = metadata_dict['num_followers']
    num_pages = return_num_pages(num_followers)
    if num_pages > 101:
        num_pages = 101
    total_followers = 0
    
    for page in range(num_pages-1, 0, -1):
        num_followers_per_page = friends_per_page(page, num_followers)
        total_followers = total_followers + num_followers_per_page
        
        if total_followers < max_followers:

            temp_dict = {'username': username, 'page': page, 'num_pages': num_pages-1, 'num_followers_per_page': num_followers_per_page, 'metadata_dict': metadata_dict}
        # pages.append((username, page))
        # pages_temp.append(temp_dict)

            all_pages_temp.append(temp_dict)


    return all_pages_temp

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
        char_ind = flattened_dict[username]['username_char_ind']
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

def check_pubsub_message_id(pubsub_message_id, database):
    
    
    pubsub_message_id_int = int(pubsub_message_id)
    pubsub_message_id_rev = str(revbits(pubsub_message_id_int))
    print(f"pubsub_message_id_int: {pubsub_message_id_int}")
    print(f"Inside check_pubsub_message_id, type(pubsub_message_id_rev): {type(pubsub_message_id_rev)}")
    print(f"type(pubsub_message_id): {type(pubsub_message_id)}")
    print(f"type(pubsub_message_id_int): {type(pubsub_message_id_int)}")
    # spanner_client = spanner.Client()
    # instance = spanner_client.instance(instance_id)
    # database = instance.database(database_id)
    results_list = []
    
    with database.snapshot() as snapshot:
        print("inside database.snapshot() in check_pubsub_message_id")
        results = snapshot.execute_sql(
            "SELECT COUNT(*) as num_messages "
            "FROM brick_log "
            "WHERE pubsub_message_id_reverse = @pubsub_message_id_rev;",
            params={"pubsub_message_id_rev": pubsub_message_id_rev},
            param_types={"pubsub_message_id_rev": spanner.param_types.STRING},
        )
        print("writing rows to spanner in check_pubsub_message_id")
        for row in results:
            results_list.append({"num_messages": row[0]})
            
    return results_list

# def check_master(flattened_dict, client):
#     '''Check master for usernames.

#     Inputs
#     ------
#     flattened_dict : dict
#         Dictionary of usernames we are checking
#     client : bigquery.Client()
#         BigQuery Client object

#     Returns
#     -------
#     records : list
#         List of records (each record is a dictionary) from master
    
#     '''
#     if len(flattened_dict) > 0:
#         usernames = usernames_str_check_master(flattened_dict)
#         numbers = char_inds_check_master(flattened_dict)
#         # master --> master_testing
#         sql = f"""WITH PARTITIONED_USERNAMES AS(
#         SELECT username
#         FROM sneakyscraper.scrape_whotwi.master_clean
#         WHERE 
#         username_char_ind in {numbers}),
#         RANKED_MESSAGES AS( 
#         SELECT * 
#         FROM PARTITIONED_USERNAMES
#         WHERE
#         username IN {usernames})
#         SELECT * FROM RANKED_MESSAGES;"""
#         # print(sql)
#         try:
#             query_job = client.query(sql)
#             records = [dict(row) for row in query_job]
#         except Exception as e:
#             records = []
#             print(f"Error with check_master: {e}")
#             pass
#     else:
#         records = []
#     return records

def check_master(flattened_dict, client, by='userid'):
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
    
    if by == 'userid':
        if len(flattened_dict) > 0:
            userids_str = userids_str_check_master(flattened_dict)
            # master --> master_testing
            sql = f"""
            SELECT username, userid
            FROM
            sneakyscraper.scrape_whotwi.master_clean
            WHERE
            userid in {userids_str}"""
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
    elif by == 'username':
        if len(flattened_dict) > 0:
            usernames = usernames_str_check_master(flattened_dict)
            numbers = char_inds_check_master(flattened_dict)
            # master --> master_testing
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


# def new_accounts(new_friends_flat, records_from_master):
#     '''Function to return new accounts that weren't in master.

#     Inputs
#     ------
#     new_friends_flat : dict
#         Dictionary of user metadata from scraping whotwi for children's new friends
#     records_from_master : list
#         Records returned from BigQuery (list of dicts)

#     Returns
#     -------
#     new_friends_dict : dict
#         Dictionary of metadata for accounts not in master
    
#     '''

#     just_checked_list = list(new_friends_flat.keys())
#     old_friends_list = [x['username'] for x in records_from_master]
#     new_friends_list = list(set(just_checked_list) - set(old_friends_list))
#     already_friends_list = list(set(old_friends_list) - set(new_friends_list))
#     # old_friends_dict = bqlod_to_dict(records_from_master)

#     if len(new_friends_list) > 0:
#         new_friends_dict = {username: new_friends_flat[username] for username in new_friends_list}
#     else:
#         new_friends_dict = {}
#     try:
#         if len(already_friends_list) > 0:
#             already_friends_dict = {username: new_friends_flat[username] for username in already_friends_list}
#         else:
#             already_friends_dict = {}
#     except Exception as e:
#         print(f'Failed to create already friends {e}')
#         already_friends_dict = {}
    
#     return new_friends_dict, already_friends_dict
def new_accounts(new_friends_flat, records_from_master, by='userid'):
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

    if by == 'userid':
        just_checked_list = [new_friends_flat[key]['userid'] for key in new_friends_flat.keys()]
        old_friends_list = list(set([x['userid'] for x in records_from_master]))    
        new_friends_list = list(set(just_checked_list) - set(old_friends_list))
        new_friends_list_usernames = [x['username'] for x in list(new_friends_flat.values()) if x['userid'] in new_friends_list]
        already_friends_list = list(set(old_friends_list) - set(new_friends_list))
        already_friends_list_usernames = [x['username'] for x in list(new_friends_flat.values()) if x['userid'] in already_friends_list]
        print(f"old_friends_list: {old_friends_list}")
        print(f"new_friends_list: {new_friends_list}")
        print(f"new_friends_list_usernames: {new_friends_list_usernames}")
        print(f"already_friends_list: {already_friends_list}")
        print(f"already_friends_list_usernames: {already_friends_list_usernames}")

        if len(new_friends_list_usernames) > 0:
            new_friends_dict = {username: new_friends_flat[username] for username in new_friends_list_usernames}
        else:
            new_friends_dict = {}

        try:
            if len(already_friends_list_usernames) > 0:
                already_friends_dict = {username: new_friends_flat[username] for username in already_friends_list_usernames}
            else:
                already_friends_dict = {}
        except Exception as e:
            print(f'Failed to create already friends {e}')
            already_friends_dict = {}
            
    elif by == 'username':
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

def userids_str_check_master(flattened_dict):
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
    userids_str = ''
    for i, (username, metadata) in enumerate(flattened_dict.items()):
        if i < (len(flattened_dict)-1):
            userids_str += f"{metadata['userid']}, "
        else:
            userids_str += f"{metadata['userid']}"
    userids_str = '(' + userids_str + ')'
    return userids_str

def get_new_accounts(child_batch_dict, client, by='userid'):
    if by == 'userid':
        records_from_master = check_master(child_batch_dict, client, by='userid')
        new_friends_dict, already_friends_dict = new_accounts(child_batch_dict, records_from_master, by='userid')
    elif by == 'username':
        records_from_master = check_master(child_batch_dict, client, by='username')
        new_friends_dict, already_friends_dict = new_accounts(child_batch_dict, records_from_master, by='username')
    
    return new_friends_dict, already_friends_dict