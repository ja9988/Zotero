from external_funcs import MetadataRequests, get_metadata_bs4_driver
import base64
import json
# from twitter_api_tools import get_metadata_tapi
from send_brick import dispatch_bricks
import random
from time import sleep
import datetime


# def check_twitter_metadata(twitter_metadata, list_of_parents):
#     try:
#         for username in twitter_metadata.keys():
#             if twitter_metadata[username]['num_friends'] == 0:
#                 return list_of_parents
#         return []
#     except Exception as e:
#         print(f"Exception in check_twitter_metadata: {e}")
#         return list_of_parents


def check_pubsub_message_id(pubsub_message_id, client):
    sql = f"""SELECT COUNT(*) as num_messages
    FROM sneakyscraper.scrape_whotwi.brick_log
    WHERE 
    pubsub_message_id = '{pubsub_message_id}';"""
    try:
        query_job = client.query(sql)
        records = [dict(row) for row in query_job]
    except Exception as e:
        records = []
        print(f"Error with check_pubsub_message_id: {e}")
        pass

    return records

def check_brick(brick_json, client):
    pubsub_message_id = brick_json['message']['message_id']
    pubsub_message_id_count = check_pubsub_message_id(pubsub_message_id, client)
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
    brick_batch_total = int(brick_json['message']['attributes']['brick_batch_total'])
    brick_json['message']['attributes']['brick_batch_total'] = brick_batch_total
    brick_number = int(brick_json['message']['attributes']['brick_number'])
    brick_json['message']['attributes']['brick_number'] = brick_number
    # print(f"Type of brick_number: {type(brick_json['message']['attributes']['brick_number'])}")
    return brick_json

def check_brick_batch(brick_json, client):
    brick_batch_uuid = brick_json['message']['attributes']['brick_batch_uuid']
    brick_batch_total = brick_json['message']['attributes']['brick_batch_total']

    brick_batch_count = check_brick_batch_count(brick_batch_uuid, client)
    batch_count = brick_batch_count[0]['num_messages']

    if batch_count == brick_batch_total:
        is_last_brick_message = True
    else:
        is_last_brick_message = False

    return is_last_brick_message

def check_brick_batch_count(brick_batch_uuid, client):
    sql = f"""SELECT COUNT(*) as num_messages
    FROM sneakyscraper.scrape_whotwi.brick_log
    WHERE 
    brick_batch_uuid = '{brick_batch_uuid}';"""
    try:
        query_job = client.query(sql)
        records = [dict(row) for row in query_job]
    except Exception as e:
        records = []
        print(f"Error with check_brick_batch_count: {e}")
        pass

    return records

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

def parents_to_list(metadata_result, brick_batch_uuid):
    metadata_result_list = []
    for username, metadata_old in metadata_result.items():
        metadata_temp = {}
        for key in metadata_old.keys():
            if type(metadata_old[key]) is datetime.datetime:
                metadata_temp[key] = metadata_old[key].isoformat()
            else:
                metadata_temp[key] = metadata_old[key]

        metadata_temp['brick_batch_uuid'] = brick_batch_uuid
        metadata_result_list.append(metadata_temp)

    return metadata_result_list

def new_children_to_list(metadata_result, last_child_uuid):
    metadata_result_list = []
    for username, metadata_old in metadata_result.items():
        metadata_temp = {}
        for key in metadata_old.keys():
            if type(metadata_old[key]) is datetime.datetime:
                metadata_temp[key] = metadata_old[key].isoformat()
            else:
                metadata_temp[key] = metadata_old[key]

        metadata_temp['brick_batch_uuid'] = last_child_uuid
        metadata_result_list.append(metadata_temp)

    return metadata_result_list

def read_brick(inbound_brick, client):
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
    if brick_type == 'parent':
        # First we extract the grandparent username, friends page and how many parents
        # metadata we expect to scrape from that friends page
        parent_metadata = decode_data_dict(brick_data)
        username = parent_metadata['username']

        # Now we use the parameters to get the metadata of parents from this grandparent page
        metadata_result = get_metadata_bs4_driver(username, timeout_lim=20,metadata_requests=MetadataRequests().all_reqs)
        # We convert the parents metadata from this page to a list and add the 
        # brick_batch_uuid to the dictionary for each parent metadata dictionary.
        table_id_brick_log = 'sneakyscraper.scrape_whotwi.brick_log'
        test_stream_brick = stream_pubsub_record_to_bq(inbound_brick, table_id_brick_log, client)
        
    
        message = f"brick: {brick_number}, {brick_batch_total}, {brick_batch_uuid} received a parent: {metadata_result}"
    elif brick_type == 'child':
        message = "child"
    else:
        # print('Unknown brick type received. Returning 200.')
        message = 'unknown_brick_type'


    return message

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