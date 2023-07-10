import datetime as datetime
import requests
from bs4 import BeautifulSoup
from dataclasses import dataclass, field
from typing import Dict, Tuple
import time
from dateutil.relativedelta import relativedelta
import re
from joblib import Parallel, delayed
import random
import wordninja
import string
import json
from google.cloud import aiplatform
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value
from typing import Dict
import os

@dataclass(init=True, frozen=True)
class MetadataRequests:
    '''Simple dataclass to hold default metadata_requests dictionaries etc

    Attributes
    ----------
    default : dict
        Default dictionary for metadata_requests. Retrieves num_friends and num_followers.
    all_reqs: dict
        Retrieves all possible metadata_requests types
    
    '''

    default : Dict = field(default_factory=lambda: {'username': True, 'userid': True, 'list_name': True, 
                             'num_friends': True, 'num_followers': True, 'num_tweets': False, 
                             'num_crush': False, 'num_crushed_on': False, 'creation_date': False, 
                             'twitter_url': False, 'bio': False, 'location': False, 'website': False, 
                             'last_tweet': True, 'last_checked': True, 'parent_account': True})
    all_reqs : Dict = field(default_factory=lambda: {'username': True, 'userid': True, 'list_name': True, 
                             'num_friends': True, 'num_followers': True, 'num_tweets': True, 
                             'num_crush': True, 'num_crushed_on': True, 'creation_date': True, 
                             'twitter_url': True, 'bio': True, 'location': True, 'website': True, 
                             'last_tweet': True, 'last_checked': True, 'parent_account': True})
    num_friends : Dict = field(default_factory=lambda: {'username': True, 'userid': True, 'list_name': False, 
                             'num_friends': True, 'num_followers': False, 'num_tweets': False, 
                             'num_crush': False, 'num_crushed_on': False, 'creation_date': False, 
                             'twitter_url': False, 'bio': False, 'location': False, 'website': True, 
                             'last_tweet': True, 'last_checked': True, 'parent_account': True})
    num_followers : Dict = field(default_factory=lambda: {'username': True, 'userid': True, 'list_name': False,
                             'num_friends': False, 'num_followers': True, 'num_tweets': False, 
                             'num_crush': False, 'num_crushed_on': False, 'creation_date': False, 
                             'twitter_url': False, 'bio': False, 'location': False, 'website': False, 
                             'last_tweet': True, 'last_checked': True, 'parent_account': True})
    friends_of_friends : Dict = field(default_factory=lambda: {'username': True, 'userid': True, 'list_name': True, 
                             'num_friends': True, 'num_followers': True, 'num_tweets': True, 
                             'num_crush': False, 'num_crushed_on': False, 'creation_date': False, 
                             'twitter_url': True, 'bio': True, 'location': False, 'website': True, 
                             'last_tweet': True, 'last_checked': True, 'parent_account': True})

@dataclass(init=True, frozen=True)
class RequestHeaders:
    '''Simple dataclass to hold default metadata_requests dictionaries

    Attributes
    ----------
    default : dict
        Default dictionary for metadata_requests. Retrieves num_friends and num_followers.
    all_reqs: dict
        Retrieves all possible metadata_requests types
    
    '''

    default : Dict = field(default_factory=lambda: {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36',
    })

@dataclass(init=True, frozen=False)
class MetadataDefaults():
    '''Simple dataclass to hold default metadata values and entries. Default Metadata dictionaries are 
    dynamically updated when new fields are added.

    Attributes
    ----------
    default : dict
        Default dictionary for metadata_requests. Retrieves num_friends and num_followers.
    all_reqs: dict
        Retrieves all possible metadata_requests types
    
    '''
    
    # Default metadata dictionaries
    default: Dict = field(init=False)
    default_fof : Dict = field(init=False)
    get_metadata_fail: Dict = field(init=False)
    get_fof_fail : Dict = field(init=False)

    # Default metadata dictionary entries (adding variables here will change default dictionaries)
    username : str = ''
    userid : int = 0
    list_name : str = ''
    num_friends : int = 0
    num_followers : int = 0
    num_tweets : int = 0
    num_crush : int = 0
    num_crushed_on : int = 0
    creation_date : datetime.datetime = datetime.datetime(2000, 1, 1)
    twitter_url : str = ''
    bio : str = ''
    location : str = ''
    website : str = ''
    last_tweet : datetime.datetime = datetime.datetime(2000, 1, 1)
    last_checked: datetime.datetime = datetime.datetime.now()
    parent_account : str = ''
    # Private variables excluded from count
    # Default number of errors to report back
    __errors : int = 0
    # Max number of errors that can be returned from a fof crawl
    __max_errors_fof : int = 12
    __max_errors_metadata : int = field(init=False)
   
    def __post_init__(self):
        max_errors = self.__count_metadata_attrs()
        self.__max_errors_metadata = max_errors
        self.default = self.__return_def_dict(self.__errors, max_errors)
        self.default_fof = self.__return_def_dict(self.__errors, self.__max_errors_fof)
        self.get_metadata_fail = self.__return_def_dict(max_errors, max_errors)
        self.get_fof_fail = self.__return_def_dict(self.__max_errors_fof, self.__max_errors_fof)
    def __count_metadata_attrs(self):
        # Function to count number of metadata attributes
        # this is used to fill max_errors
        count = 0
        for key, value in MetadataDefaults.__dict__.items():
            if type(value) is not dict and '__' not in key:
                count +=1
        return count

    def __return_def_dict(self, terrors, max_terrors):
        default = {}
        for key, value in MetadataDefaults.__dict__.items():
            if type(value) is not dict and '__' not in key:
                default[key] = value
        default['errors'] = terrors
        default['max_errors'] = max_terrors
        return default

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

def letter_to_num(input_str):
    numbers = [ord(character.lower()) - 96 for character in input_str]
    numbers = [x if 1 <= x <= 26 else 0 for x in numbers]
    if len(numbers) == 0:
        numbers = [0]
    return numbers

def add_char_ind(flattened_dict):
    '''Add character index for partitioned BigQuery table.

    Inputs
    ------
    flattened_dict : dict
        Dictionary of metadata where username is key, value is dict of metadata

    Returns
    -------
    flattened_dict: dict
        Dictionary of metadata where username is key, value is dict of metadata with
        username_char_ind added.

    '''

    for key in flattened_dict.keys():
        char_ind = letter_to_num(key)[0]
        flattened_dict[key]['username_char_ind'] = char_ind
    return flattened_dict

def get_metadata_bs4_driver(username, timeout_lim=20, metadata_requests=None):
    '''Function to quicky return metadata of username.

    Inputs
    ------
    username : str
        String of twitter username to find metadata (excluding @ symbol)
    timeout_lim : int
        Number of seconds to wait for whotwi website to load with bs4
    metadata_requests : dict
        Dictionary of metadata requested for each account. See inline for options.

    Returns
    -------
    return_dict : dict
        Dictionary of dictionaries containing requested metadata for each account.

    '''

    # Constrict what we are retrieving
    if metadata_requests is None:
        metadata_requests = MetadataRequests().all_reqs

    # Get a random proxy - put this here so each username has a different proxy to simulate
    # real "browsing" lmao
    # proxy  = gen_random_proxy()

    # Headers for bs4
    headers = RequestHeaders().default

    # Counting errors
    max_errors = sum(list(metadata_requests.values()))
    if metadata_requests['twitter_url'] is True:
        max_errors = max_errors - 1

    # Compose full URL for whotwi website
    full_url = f"https://en.whotwi.com/{username}/friends_except_followers"

    # Go to whotwi friends page url
    for attempt in range(10):   
        try:
            whotwi_request = requests.get(full_url, headers=headers, timeout=timeout_lim).text
            soup_whotwi_1 = BeautifulSoup(whotwi_request, 'html.parser')
            metadata = MetadataDefaults().default
            metadata = _username_get_metadata(username, metadata, metadata_requests)
            metadata = _userid_get_metadata(soup_whotwi_1, metadata, metadata_requests)
            metadata = _list_name_get_metadata(soup_whotwi_1, metadata, metadata_requests)
            metadata = _num_friends_get_metadata(soup_whotwi_1, metadata, metadata_requests)
            metadata = _num_followers_get_metadata(soup_whotwi_1, metadata, metadata_requests)
            metadata = _num_tweets_get_metadata(soup_whotwi_1, metadata, metadata_requests)
            metadata = _num_crush_get_metadata(soup_whotwi_1, metadata, metadata_requests)
            metadata = _num_crushed_on_get_metadata(soup_whotwi_1, metadata, metadata_requests)
            metadata = _creation_date_get_metadata(soup_whotwi_1, metadata, metadata_requests)
            metadata = _twitter_url(username, metadata, metadata_requests)
            metadata = _bio_get_metadata(soup_whotwi_1, metadata, metadata_requests)
            metadata = _location_get_metadata(soup_whotwi_1, metadata, metadata_requests)
            metadata = _website_get_metadata(soup_whotwi_1, metadata, metadata_requests)
            metadata = _last_tweet_get_metadata(username, metadata, metadata_requests)
            metadata = _last_checked_get_metadata(metadata, metadata_requests)
        except Exception as e:
            print(f'TIMEOUT ERROR: THROTTLED? RETRYING... ATTEMPT NUMBER {attempt} for {full_url} with Exception: {e} ')
            sleep_time = random.randint(2,7)
            time.sleep(sleep_time)
        else:
            break
    else:
        # print('-----------------------Get Metadata Fail-----------------------')
        metadata = MetadataDefaults().get_metadata_fail

    return metadata

def get_metadata_bs4(username, timeout_lim=20, metadata_requests=None):
    '''
    
    Inputs
    ------
    username : str or list or dict
        Twitter username(s) to return metadata for (excluding @ symbol)
    timeout_lim : int
        Number of seconds to wait for whotwi website to load with bs4
    metadata_requests : dict
        Dictionary of metadata requested for each account. See get_metadata_bs4_driver for options.
    
    '''
    if metadata_requests is None:
        metadata_requests = MetadataRequests().all_reqs

    # Detect type of username and interact with get_metadata_bs4_driver appropriately
    friends_metadata = {}
    if type(username) is str:
        # Call get_metadata_bs4_driver
        metadata = get_metadata_bs4_driver(username, timeout_lim=timeout_lim, metadata_requests=metadata_requests)
        friends_metadata[username] = metadata
    if type(username) is list:
        for single_username in username:
            # Call get_metadata_bs4_driver
            metadata = get_metadata_bs4_driver(single_username, timeout_lim=timeout_lim, metadata_requests=metadata_requests)
            friends_metadata[single_username] = metadata
    if type(username) is dict:
        for single_username in username.keys():
            # Call get_metadata_bs4_driver
            metadata = get_metadata_bs4_driver(single_username, timeout_lim=timeout_lim, metadata_requests=metadata_requests)
            friends_metadata[single_username] = metadata

    return friends_metadata

def get_metadata_bs4_pl(usernames, timeout_lim=20, metadata_requests=None, num_jobs=2):
    '''Function to return metadata from many accounts using parallel get_metadata_bs4.
    
    Inputs
    ------
    usernames : dict or list
        Dictionary with twitter usernames as keys, anything as value
        Alternatively, a list of twitter usernames
    timeout_lim : int
        Number of seconds to wait for whotwi website to load with bs4
    metadata_requests : dict
        Dictionary of metadata requested for each account. See get_metadata_bs4_driver for options.
    num_jobs : int
        Number of parallel get_metadata_bs4 jobs

    Returns
    -------
    friends_metadata : dict
        Dictionary with username key, value is dict of metadata

    '''

    # Split up dictionary or list of usernames into num_jobs chunks
    chunked_usernames = return_chunks(usernames, num_jobs)

    # Add input arguments for get_metadata_bs4
    input_pl = [(username, timeout_lim, metadata_requests) for username in chunked_usernames]

    # Run get_metadata_bs4 in parallel
    friends_metadata_parallel = Parallel(n_jobs=num_jobs, verbose=10)(delayed(get_metadata_bs4)(arg1, arg2, arg3) for arg1, arg2, arg3 in input_pl)

    # Throw everything into a single dictionary
    friends_metadata = {}
    for d in friends_metadata_parallel:
        friends_metadata.update(d)

    return friends_metadata

def get_friends_of_friends_bs4(username, friend_limit=5000, timeout_lim=20, metadata_requests=None):
    '''Function to get metadata of friends when getting friends of an account.

    Inputs
    ------
    username : str or list or dict
        Twitter usernames to return metadata for (excluding @ symbol) friends of.
        If list, will call get_metadata_bs4 on each username.
        If dict, keys should be usernames, value should be metadata dictionary with num_friends key/value pair
    timeout_lim : int
        Number of seconds to wait for whotwi webpages to load with requets
    metadata_requests : dict
        Dictionary of metadata requested for each friend of each username. See get_metadata_bs4_driver for options.
    
    Returns
    -------
    friends_metadata : dict
        Dictionary with username(s) as keys with values that are dicts with usernames of
        friends of username(s) as keys, values that are dict of metadata
    
    
    '''

    if metadata_requests is None:
        metadata_requests = MetadataRequests().friends_of_friends


    friends_metadata = {}

    if type(username) is str:
        metadata = get_metadata_bs4(username, timeout_lim=timeout_lim, metadata_requests=MetadataRequests().num_friends)
        friends_metadata_temp = get_friends_of_friends_bs4_driver(username, friend_limit=friend_limit, metadata=metadata, timeout_lim=timeout_lim, metadata_requests=metadata_requests)
        friends_metadata[username] = friends_metadata_temp

    if type(username) is list:
        for single_username in username:
            metadata = get_metadata_bs4(single_username, timeout_lim=timeout_lim, metadata_requests=MetadataRequests().num_friends)
            friends_metadata_temp = get_friends_of_friends_bs4_driver(single_username, friend_limit=friend_limit, metadata=metadata, timeout_lim=timeout_lim, metadata_requests=metadata_requests)
            friends_metadata[single_username] = friends_metadata_temp

    if type(username) is dict:
        for single_username in username.keys():
            metadata = {single_username : username[single_username]}
            if 'num_friends' not in metadata[single_username]:
                metadata = get_metadata_bs4(single_username, timeout_lim=timeout_lim, metadata_requests=MetadataRequests().num_friends)
            friends_metadata_temp = get_friends_of_friends_bs4_driver(single_username, friend_limit=friend_limit, metadata=metadata, timeout_lim=timeout_lim, metadata_requests=metadata_requests)
            friends_metadata[single_username] = friends_metadata_temp

    return friends_metadata

def get_friends_of_friends_bs4_pl(usernames, friend_limit=5000, timeout_lim=20, metadata_requests=None, num_jobs=2):

    # Split up dictionary or list of usernames into num_jobs chunks
    chunked_usernames = return_chunks(usernames, num_jobs)

    if metadata_requests is None:
        metadata_requests = MetadataRequests().friends_of_friends

    # Add input arguments for get_friends_bs4
    input_pl = [(username, friend_limit, timeout_lim, metadata_requests) for username in chunked_usernames]

    # Run get_friends_bs4 in parallel
    friends_metadata_parallel = Parallel(n_jobs=num_jobs, verbose=10)(delayed(get_friends_of_friends_bs4)(arg1, arg2, arg3, arg4) for arg1, arg2, arg3, arg4 in input_pl)

    # Throw everything into a single dictionary
    friends_metadata = {}
    for d in friends_metadata_parallel:
        friends_metadata.update(d)

    return friends_metadata

def get_friends_of_friends_bs4_driver(username, friend_limit=5000, metadata=None, timeout_lim=20, metadata_requests=None):
    '''Function to get metadata of friends when getting friends of an account.

    Inputs
    ------
    username : str
        Twitter username to return metadata of accounts for (excluding @ symbol)
    metadata : dict
        Metadata dictionary associated with username. Must contain num_friends entry.
    timeout_lim : int
        Number of seconds to wait for whotwi website to load with bs4
    metadata_requests : dict
        Dictionary of metadata requested for each account. See MetadataRequests() for options.
    
    Returns
    -------
    friends_metadata : dict
        Dictionary with usernames that are friends of username as keys, values are dict of metadata
    
    
    '''

    # Get a random proxy - put this here so each username has a different proxy to simulate
    # real "browsing" lmao
    # proxy  = gen_random_proxy()
    session = requests.Session()
    # session.proxies.update(proxy)
    session.headers.update(RequestHeaders().default)

    if metadata_requests is None:
        metadata_requests = MetadataRequests().friends_of_friends
    
    if metadata is None:
        metadata = get_metadata_bs4(username, timeout_lim, metadata_requests=MetadataRequests().num_friends)
    num_friends = metadata[username]['num_friends']
    if num_friends == 0:
        friends_of_friends_pages = [{}]
    else:
        if num_friends > friend_limit:
            num_friends = friend_limit
        friends_of_friends_pages = []
        num_pages = num_friends//50 + 2
        # whotwi only has the last 5000 friends
        if num_pages > 101:
            num_pages = 101
        for page in range(1, num_pages):
            friends_metadata_temp = get_friends_of_friends_bs4_page(username, page, timeout_lim, metadata_requests, session=session)
            friends_of_friends_pages.append(friends_metadata_temp)
    
    session.close()
    # Merge list of dictionaries created from each page
    friends_metadata_temp = {}
    for d in friends_of_friends_pages:
        if bool(d):
            friends_metadata_temp.update(d)

    # Filter down to num_friends
    if num_friends < len(friends_metadata_temp):
        friends_metadata = {}
        list_metadata_temp = list(friends_metadata_temp.items())[0:num_friends]
        for entry in list_metadata_temp:
            friends_metadata[entry[0]] = entry[1]
    else:
        friends_metadata = friends_metadata_temp

    return friends_metadata

def get_friends_of_friends_bs4_page(username, page, timeout_lim=20, metadata_requests=None, session=None):
    '''Function to get metadata from friends list view page.
    
    Inputs
    ------
    username : str
        Twitter username of account to get friends with metadata from
    page : int
        Page of whotwi friends results to scrape
    timeout_lim : int
        Number of seconds to wait for whotwi website to load with bs4
    metadata_requests : dict
        Dictionary of metadata requested for each account. See get_metadata_bs4_driver for options.
    

    Returns
    -------
    friends_metadata : dict
        Dictionary with friends usernames as keys, values are dict of collected metadata from page page.
    '''
    if metadata_requests is None:
        metadata_requests = MetadataRequests().friends_of_friends

    # Build URL to get 50 most recent friends
    whotwi_link = f"https://en.whotwi.com/{username}/friends/user_list?page={page}+&view_type=list"
    
    # Get first page of friends
    try:
        # Submit GET request
        whotwi_get = session.get(whotwi_link, timeout=timeout_lim)
        # whotwi_get = requests.get(whotwi_link, headers=RequestHeaders().default, timeout=10).text
        # Parse with bs4
        whotwi_soup = BeautifulSoup(whotwi_get.text, 'html.parser')
        print(f"succesfully got bs4 page {page} for {username}")
    except Exception as e:
        print(f"Failed to get bs4 page {page} for {username} with Exception {e}")
        # print('with exception', e)
        pass
    friends_metadata = {}
    
    try:
        friends_soup = whotwi_soup.find_all('li', itemtype="http://schema.org/Person")
        for friend_soup in friends_soup:
            # create dictionary for metadata from default type for fof
            metadata = MetadataDefaults().default_fof
            child_username, metadata = _username_friends_of_friends(friend_soup, metadata, metadata_requests)
            metadata = _userid_friends_of_friends(friend_soup, metadata, metadata_requests)
            metadata = _list_name_friends_of_friends(friend_soup, metadata, metadata_requests)
            metadata = _counts_friends_of_friends(friend_soup, metadata, metadata_requests)
            metadata = _twitter_url(child_username, metadata, metadata_requests)
            metadata = _bio_friends_of_friends(friend_soup, metadata, metadata_requests)
            metadata = _website_friends_of_friends(friend_soup, metadata, metadata_requests)
            metadata = _last_tweet_friends_of_friends(friend_soup, metadata, metadata_requests)
            metadata = _last_checked_friends_of_friends(metadata, metadata_requests)
            metadata = _parent_account_friends_of_friends(username, metadata, metadata_requests)
            friends_metadata[child_username] = metadata
    except:
        pass

    return friends_metadata

def get_fof_bs4_page(username, page, timeout_lim=20, metadata_requests=None, session=None):
    '''Function to get metadata from friends list view page.
    
    Inputs
    ------
    username : str
        Twitter username of account to get friends with metadata from
    page : int
        Page of whotwi friends results to scrape
    timeout_lim : int
        Number of seconds to wait for whotwi website to load with bs4
    metadata_requests : dict
        Dictionary of metadata requested for each account. See get_metadata_bs4_driver for options.
    

    Returns
    -------
    friends_metadata : dict
        Dictionary with friends usernames as keys, values are dict of collected metadata from page page.
    '''
    if metadata_requests is None:
        metadata_requests = MetadataRequests().friends_of_friends

    # Build URL to get 50 most recent friends
    whotwi_link = f"https://en.whotwi.com/{username}/friends/user_list?page={page}+&view_type=list"
    # Get page of friends
    for attempt in range(6):
        try:
            # Submit GET request
            whotwi_get = session.get(whotwi_link, timeout=timeout_lim)
            # Parse with bs4
            whotwi_soup = BeautifulSoup(whotwi_get.text, 'html.parser')
        except Exception as e:
            if attempt > 3:
                print(f'Failed to load page {page} for {username}. Will retry... attempt {attempt} logged, {e}')
            # print(f'Will retry... attempt {attempt} logged')
            sleep_time = random.randint(2,7)
            time.sleep(sleep_time)
            if timeout_lim <= 30:
                timeout_lim=timeout_lim + 2
        else:
            break
    else:
        print(f'Total failure getting page {page} for {username}')
    friends_metadata = {}
    
    
    try:
        friends_soup = whotwi_soup.find_all('li', itemtype="http://schema.org/Person")
        for friend_soup in friends_soup:
            # create dictionary for metadata from default type for fof
            metadata = MetadataDefaults().default_fof
            child_username, metadata = _username_friends_of_friends(friend_soup, metadata, metadata_requests)
            metadata = _userid_friends_of_friends(friend_soup, metadata, metadata_requests)
            metadata = _list_name_friends_of_friends(friend_soup, metadata, metadata_requests)
            metadata = _counts_friends_of_friends(friend_soup, metadata, metadata_requests)
            metadata = _twitter_url(child_username, metadata, metadata_requests)
            metadata = _bio_friends_of_friends(friend_soup, metadata, metadata_requests)
            metadata = _website_friends_of_friends(friend_soup, metadata, metadata_requests)
            metadata = _last_tweet_friends_of_friends(friend_soup, metadata, metadata_requests)
            metadata = _last_checked_friends_of_friends(metadata, metadata_requests)
            metadata = _parent_account_friends_of_friends(username, metadata, metadata_requests)
            friends_metadata[child_username] = metadata
    except Exception as e:
        print(f"Missing page {page} for {username}! {e}")
        pass

    return {'parent': username, 'page': page, 'metadata': friends_metadata}

def get_fof_parallel(username, metadata_requests=None, timeout_lim=20, friend_limit=5000, num_jobs=2):
    '''An improved method for getting friends of friends for multiple accounts. Avoids
    joblib.Parallel threads waiting for accounts with many friends to finish. Instead
    we simply figure out all of the pages that need to be scraped ahead of time, scrape
    them all and then aggregate them together at the end to produce an output with the
    same format as get_friends_of_friends_bs4. This method is much faster as a result
    of using threads more efficiently.

    Inputs
    ------
    username : str or list or dict
        String, list or dictionary of parent accounts. If dictionary please ensure 
        each username has a num_friends entry or else it will be slow.
    metadata_requests : dict
        Dictionary of metadata requested for each account. See get_metadata_bs4_driver for options.
    timeout_lim : int
        Number of seconds to wait for whotwi website to load with bs4
    friend_limit : int
        Maximum number of friends to return. Applies to all accounts. For control of
        individual accounts, use the num_friends attribute in a dictionary.
    num_jobs : int
        Number of threads to use. Defaults to 2, but higher numbers should be used to
        take advantage of threads.

    Returns
    -------
    fof_parralel_collected : dict
        Dictionary of metadata organized with parent accounts as keys as in
        get_friends_of_friends_bs4

    '''
    # Get a random proxy - put this here so each username has a different proxy to simulate
    # real "browsing" lmao
    # proxy  = gen_random_proxy()
    session = requests.Session()
    # session.proxies.update(proxy)
    session.headers.update(RequestHeaders().default)

    if metadata_requests is None:
        metadata_requests = MetadataRequests().friends_of_friends
    
    fof_metadata  = return_metadata_fof_parallel(username, friend_limit)
    username_page_tuples = return_inputs_fof_parallel(fof_metadata)
    input_pl = [(entry[0], entry[1], timeout_lim, metadata_requests, session) for entry in username_page_tuples]

    print(f"Running get_fof_parallel with {len(input_pl)} pages to scrape")

    # Run get_fof_bs4_page in parallel
    fof_parallel = Parallel(n_jobs=num_jobs, verbose=10)(delayed(get_fof_bs4_page)(arg1, arg2, arg3, arg4, arg5) for arg1, arg2, arg3, arg4, arg5 in input_pl)
    # Organize results according to parent and by page number
    fof_parralel_collected = sort_and_aggregate_dict_fof_parallel(fof_parallel)

    for parent_username in fof_metadata.keys():
        num_friends_requested = fof_metadata[parent_username]['num_friends']
        num_friends_returned = len(fof_parralel_collected[parent_username].keys())

        if num_friends_requested < num_friends_returned:
            parent_friends_temp = {}
            list_parent_metadata_temp = list(fof_parralel_collected[parent_username].items())[0:num_friends_requested]
            for entry in list_parent_metadata_temp:
                parent_friends_temp[entry[0]] = entry[1]

            fof_parralel_collected[parent_username] = parent_friends_temp

    return fof_parralel_collected

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

def sort_and_aggregate_dict_fof_parallel(list_of_dicts):
    '''Function to aggregate results from parallel calls to get_fof_bs4_page

    Inputs
    ------
    list_of_dicts : list
        List of dicts each having a parent, page and metadata key

    Returns
    -------
    new_dict : dict
        Dictionary of metadata organized like get_friends_of_friends_bs4
    
    '''
    unique_parents = list(set([v['parent'] for v in list_of_dicts] ))
    new_dict = {}
    for parent_username in unique_parents:
        sub_list_of_dicts = [x for x in list_of_dicts if x['parent'] == parent_username]
        sorted_sub_list_of_dicts = sorted(sub_list_of_dicts, key=lambda t: t['page']) 
        new_dict_temp = {}
        for sorted_page in sorted_sub_list_of_dicts:
            new_dict_temp.update(sorted_page['metadata'])
        new_dict[parent_username] = new_dict_temp
        
    return new_dict


def return_metadata_fof_parallel(usernames, friend_limit):
    '''Return a valid parent metadata dictionary for use with get_fof_parallel
    
    Inputs
    ------
    usernames : str or list or dict
        String, list or dictionary of parent accounts. If dictionary please ensure 
        it has a num_friends entry
    friend_limit : int
        Maximum number of friends to return by get_fof_parallel

    Returns
    -------
    usernames_metadata : dict
        Valid parent metadata dictionary for use with get_fof_parallel
    
    '''
    # Gather metadata to create page counts, etc
    if type(usernames) is list:
        if len(usernames) > 1:
            num_jobs = len(usernames)
            if num_jobs > 20:
                num_jobs = 20
            usernames_metadata = get_metadata_bs4_pl(usernames, num_jobs=len(usernames), metadata_requests=MetadataRequests().all_reqs)
        else:
            usernames_metadata = get_metadata_bs4(usernames, metadata_requests=MetadataRequests().all_reqs)
        usernames_metadata = check_metadata_dict_fof_parallel(usernames_metadata, friend_limit)
    elif type(usernames) is dict:
        usernames_metadata = usernames
        usernames_metadata = check_metadata_dict_fof_parallel(usernames_metadata, friend_limit)
    elif type(usernames) is str:
        usernames_metadata = get_metadata_bs4(usernames, metadata_requests=MetadataRequests().all_reqs)
        usernames_metadata = check_metadata_dict_fof_parallel(usernames_metadata, friend_limit)

    return usernames_metadata
    
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

def check_metadata_dict_fof_parallel(usernames_metadata, friend_limit):
    '''Verify and fix the metadata dictionary used to run get_fof_parallel

    Inputs
    ------
    usernames_metadata : dict
        Dictionary of parent usernames with valid num_friends entries
    friend_limit : int
        Maximum number of friends to return for a given username

    Returns
    -------
    clean_dict : dict
        Cleaned up version of usernames_metadata
    
    
    '''
    for username in usernames_metadata.keys():
        if 'num_friends' in usernames_metadata[username].keys():
            if usernames_metadata[username]['num_friends'] > friend_limit:
                usernames_metadata[username]['num_friends'] = friend_limit
        else:
            username_metadata = get_metadata_bs4(username, metadata_requests=MetadataRequests().all_reqs)
            if username_metadata['num_friends'] > friend_limit:
                username_metadata['num_friends'] = friend_limit
            usernames_metadata[username] = username_metadata

    # Get rid of usernames with zero num_friends
    clean_dict = {}
    for username in usernames_metadata.keys():
        if usernames_metadata[username]['num_friends'] > 0:
            clean_dict[username] = usernames_metadata[username]


    return clean_dict


# -------------------------------------- BS4 Scraping ------------------------------------------ #
def _username_get_metadata(username, metadata, metadata_requests):
    if metadata_requests['username'] is True:
        try:
            metadata['username'] = username
        except:
            metadata['errors'] += 1
            pass
    return(metadata)

def _userid_get_metadata(soup_whotwi_1, metadata, metadata_requests):
    if metadata_requests['userid'] is True:
        try:
            userid_tag = str(soup_whotwi_1.find(lambda tag: tag.name == 'div' and tag.get('class') == ['friendship_buttons']).find('button'))
            metadata['userid'] = int(re.findall(r'data-user-id="(.*?)"', userid_tag)[0])
        except:
            metadata['errors'] += 1
            pass
    return(metadata)

def _list_name_get_metadata(soup_whotwi_1, metadata, metadata_requests):
     # Get the friend count element
    if metadata_requests['list_name'] is True:
        whotwi_name_element = soup_whotwi_1.find('div', {'id': 'user_column_summary_name'})            
        # Return integer number of friends
        try:
            metadata['list_name'] = whotwi_name_element.contents[0].strip()
        except:
            metadata['errors'] += 1
            pass
    return(metadata)

def _num_friends_get_metadata(soup_whotwi_1, metadata, metadata_requests):
    # Get the friend count element
    if metadata_requests['num_friends'] is True:
        whotwi_friend_count_element = soup_whotwi_1.find('div', {'class': 'user_column_menu_li_right js_friends'})
        # Return integer number of friends
        try:
            metadata['num_friends'] = int(whotwi_friend_count_element.contents[0])
        except:
            metadata['errors'] += 1
            pass
    return(metadata)

def _num_followers_get_metadata(soup_whotwi_1, metadata, metadata_requests):
    # Get the followers count element
    if metadata_requests['num_followers'] is True:
        whotwi_followers_count_element = soup_whotwi_1.find('div', {'class': 'user_column_menu_li_right js_followers'})
        # Return integer number of followers
        try:
            metadata['num_followers'] = int(whotwi_followers_count_element.contents[0])
        except:
            metadata['errors'] += 1
            pass
    return(metadata)

def _num_tweets_get_metadata(soup_whotwi_1, metadata, metadata_requests):
    # Get the number of tweets element
    if metadata_requests['num_tweets'] is True:
        whotwi_num_tweets_element = soup_whotwi_1.find('div', {'class': 'user_column_menu_li_right js_tweets'})
        # Return integer number of tweets
        try:
            metadata['num_tweets'] = int(whotwi_num_tweets_element.contents[0])
        except:
            metadata['errors'] += 1
            pass
    return(metadata)

def _num_crush_get_metadata(soup_whotwi_1, metadata, metadata_requests):
    # Get crush element
    if metadata_requests['num_crush'] is True:
        whotwi_crush_element = soup_whotwi_1.find('div', {
            'class': 'user_column_menu_li_right js_friends_except_followers'})
        # Return integer number of crushes
        try:
            metadata['num_crush'] = int(whotwi_crush_element.contents[0])
        except:
            metadata['errors'] += 1
            pass
    return(metadata)

def _num_crushed_on_get_metadata(soup_whotwi_1, metadata, metadata_requests):
    # Get crushed on element
    if metadata_requests['num_crushed_on'] is True:
        whotwi_crushed_on_element = soup_whotwi_1.find('div', {
            'class': 'user_column_menu_li_right js_followers_except_friends'})
        # Return integer number of crushed on
        try:
            metadata['num_crushed_on'] = int(whotwi_crushed_on_element.contents[0])
        except:
            metadata['errors'] += 1
            pass
    return(metadata)

def _creation_date_get_metadata(soup_whotwi_1, metadata, metadata_requests):
    if metadata_requests['creation_date'] is True:
        # Get the account creation date element
        whotwi_creation_element = soup_whotwi_1.find('li', {'data-toggle': 'tooltip'})
        # Get date of account creation
        try:
            match = re.search(r'\d{4}/\d{1,2}/\d{1,2}', whotwi_creation_element.text)
            year_month_day = match.group(0).split('/')
            year = int(year_month_day[0])
            month = int(year_month_day[1])
            day = int(year_month_day[2])
            metadata['creation_date'] = datetime.datetime(year, month, day)
        except:
            metadata['errors'] += 1
            pass
    return(metadata)

def _bio_get_metadata(soup_whotwi_1, metadata, metadata_requests):
    if metadata_requests['bio'] is True:
        # Get the bio element
        whotwi_bio_element = soup_whotwi_1.find('div', {'id': 'user_column_description'})
        # Return bio
        try:
            metadata['bio'] = whotwi_bio_element.contents[0].strip()
        except:
            metadata['errors'] += 1
            pass
    return(metadata)

def _location_get_metadata(soup_whotwi_1, metadata, metadata_requests):
    if metadata_requests['location'] is True:
        # <i class="glyphicon glyphicon-map-marker"></i>
        # Get the account location element
        whotwi_location_element = soup_whotwi_1.find('ul', {'class': 'user_column_info_list'})
        if whotwi_location_element is not None:
            # Get location of account
            location_icon = whotwi_location_element.find('i', {'class': 'glyphicon glyphicon-map-marker'})
            if location_icon is not None:
                try:
                    metadata['location'] = whotwi_location_element.find('li').text.strip()
                except:
                    metadata['errors'] += 1
                    pass
    return(metadata)

def _website_get_metadata(soup_whotwi_1, metadata, metadata_requests):
    if metadata_requests['website'] is True:
        # Get the account website element
        whotwi_website_element = soup_whotwi_1.find('ul', {'class': 'user_column_info_list'})
        # Get website URL of account
        try:
            metadata['website'] = whotwi_website_element.find('a').text.strip()
            # print(website)
        except:
            metadata['errors'] += 1
            pass
    return(metadata)

def _last_tweet_get_metadata(username, metadata, metadata_requests):
    last_tweet_url = f"https://en.whotwi.com/{username}/tweets?&page=1"
    if metadata_requests['last_tweet'] is True:
        # Don't bother looking for last tweet if they haven't tweeted
        if metadata_requests['num_tweets'] is True and metadata['num_tweets'] == 0:
            # print(f"{username} didn't check last_tweet")
            pass
        else:
            last_tweet_request = requests.get(last_tweet_url, headers=RequestHeaders().default, timeout=30).text
            last_tweet_soup = BeautifulSoup(last_tweet_request, 'html.parser')
            try:
                tweet_list_element = last_tweet_soup.find(lambda tag: tag.name == 'ul' and tag.get('class') == ['tweet_list'])
                date_last_tweet_tag_as_str = str(tweet_list_element.find(lambda tag: tag.name == 'li' and tag.get('class') == ['date']))
                date_str_from_url = re.findall(r'archive/(.*?)">', date_last_tweet_tag_as_str)[0]
                metadata['last_tweet'] = datetime.datetime.strptime(date_str_from_url, '%Y/%m/%d')
            except Exception as e:
                # print(f"{username} exited when checking last_tweet")
                # print(e)
                metadata['errors'] += 1
                pass
    return(metadata)

def _last_checked_get_metadata(metadata, metadata_requests):
    if metadata_requests['last_checked'] is True:
        try:
            metadata['last_checked'] = datetime.datetime.now()
        except:
            metadata['errors'] += 1
            pass
    return(metadata)

def _twitter_url(username, metadata, metadata_requests):
    if metadata_requests['twitter_url'] is True:
        try:
            metadata['twitter_url'] = f"https://www.twitter.com/{username}"
        except:
            metadata['errors'] += 1
            pass
    return(metadata)

def _username_friends_of_friends(friend_soup, metadata, metadata_requests):
    username = 'NOT FOUND'
    try:
        username = friend_soup.find('span',{'class': 'user_list_screen_name'}).text.strip('@')
        if metadata_requests['username'] is True:
            metadata['username'] = username
    except:
        metadata['errors'] += 1
    return(username, metadata)

def _userid_friends_of_friends(friend_soup, metadata, metadata_requests):
    if metadata_requests['userid'] is True:
        try:
            userid_tag = str(friend_soup.find(lambda tag: tag.name == 'div' and tag.get('class') == ['friendship_buttons']).find('button'))
            metadata['userid'] = int(re.findall(r'data-user-id="(.*?)"', userid_tag)[0])
        except:
            metadata['errors'] += 1
            pass
    return(metadata)

def _list_name_friends_of_friends(friend_soup, metadata, metadata_requests):
    if metadata_requests['list_name'] is True:
        try:
            metadata['list_name'] = friend_soup.find('span',{'class': 'user_list_name'}).text.strip()
        except:
            metadata['errors'] += 1
            pass
    return(metadata)

def _counts_friends_of_friends(friend_soup, metadata, metadata_requests):
    if metadata_requests['num_friends'] is True or metadata_requests['num_followers'] is True or metadata_requests['num_tweets'] is True: 
        try:
            counts = friend_soup.find_all(lambda tag: tag.name == 'div' and tag.get('class') == ['user_list_description_pc'])[0].find_all('b')
            if metadata_requests['num_friends'] is True:
                metadata['num_friends'] = int(counts[1].text)
            if metadata_requests['num_followers'] is True:
                metadata['num_followers'] = int(counts[2].text)
            if metadata_requests['num_tweets'] is True:
                metadata['num_tweets'] = int(counts[0].text)
        except:
            metadata['errors'] += 3
            pass
    return(metadata)

def _bio_friends_of_friends(friend_soup, metadata, metadata_requests):
    if metadata_requests['bio'] is True:
        try:
            metadata['bio'] = friend_soup.find('span',{'itemprop': 'description'}).text.replace('\n', ' ').strip()
        except:
            metadata['errors'] += 1
            pass
    return(metadata)

def _website_friends_of_friends(friend_soup, metadata, metadata_requests):
    if metadata_requests['website'] is True:
        try:
            metadata['website'] = friend_soup.find('div',{'class': 'user_list_url'}).find('a').text.strip()
        except:
            metadata['errors'] += 1
            pass
    return(metadata)

def _last_tweet_friends_of_friends(friend_soup, metadata, metadata_requests):
    if metadata_requests['last_tweet'] is True:
        time_now = datetime.datetime.now()
        try:
            tweet_time_str = friend_soup.find_all(lambda tag: tag.name == 'div' and tag.get('class') == ['user_list_description_pc'])[0].find('span').text.strip().lower()
            tweet_time_number = int(re.findall(r'\d+', tweet_time_str)[0])
            if 'hour' in tweet_time_str:
                metadata['last_tweet'] = time_now - relativedelta(hours=tweet_time_number)
            if 'day' in tweet_time_str:
                metadata['last_tweet'] = time_now - relativedelta(days=tweet_time_number)
            if 'week' in tweet_time_str:
                metadata['last_tweet'] = time_now - relativedelta(weeks=tweet_time_number)
            if 'month' in tweet_time_str:
                metadata['last_tweet'] = time_now - relativedelta(months=tweet_time_number)
            if 'year' in tweet_time_str:
                metadata['last_tweet'] = time_now - relativedelta(years=tweet_time_number)
        except:
            metadata['errors'] += 1
            pass
    return(metadata)

def _last_checked_friends_of_friends(metadata, metadata_requests):
    if metadata_requests['last_checked'] is True:
        try:
            metadata['last_checked'] = datetime.datetime.now()
        except:
            metadata['errors'] += 1
            pass
    return(metadata)

def _parent_account_friends_of_friends(parent_username, metadata, metadata_requests):
    if metadata_requests['parent_account'] is True:
        try:
            metadata['parent_account'] = parent_username
        except:
            metadata['errors'] += 1
            pass
    return(metadata)


# ------------------------------------ End BS4 Scraping ---------------------------------------- #
# ---------------------------------------- Big Query ------------------------------------------- #
def get_parent_scores_bq(client):
    '''Returns scores for lookup and alerts.

    Inputs
    ------
    client: biquery.Client()
        BigQuery Client object

    Returns
    -------
    parent_scores : dict
        Dictionary of parent score emojis. 
    
    '''
    print("Entering get_parent_scores_bq")
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=2)
    tomorrow = today + datetime.timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    tomorrow_str = tomorrow.strftime("%Y-%m-%d")
    sql = f"""
    WITH recent_parents_partition AS(
    SELECT username, parent_account, last_checked
    FROM sneakyscraper.scrape_whotwi.last_n_friends
    WHERE 
    last_checked BETWEEN TIMESTAMP('{yesterday_str}')
    AND TIMESTAMP('{tomorrow_str}') AND parent_account = 'onlymans7'),
    recent_parents_partition_2 AS(
    SELECT username, parent_account, last_checked
    FROM sneakyscraper.scrape_whotwi.last_n_friends
    WHERE 
    last_checked BETWEEN TIMESTAMP('{yesterday_str}')
    AND TIMESTAMP('{tomorrow_str}') AND parent_account = 'extrawomangirl'),
    recent_parents AS(
    SELECT username, parent_account, 1 as score FROM recent_parents_partition 
    WHERE TRUE 
    QUALIFY 1 = ROW_NUMBER() OVER(PARTITION BY username ORDER BY last_checked DESC)),
    recent_parents_2 AS(
    SELECT username, parent_account, 3 as score FROM recent_parents_partition_2
    WHERE TRUE 
    QUALIFY 1 = ROW_NUMBER() OVER(PARTITION BY username ORDER BY last_checked DESC)),
    onlymans7_scores AS(
    SELECT username, score FROM recent_parents),
    extrawomangirl_scores AS(
    SELECT username, score FROM recent_parents_2),
    recent_wizard_scores AS(
    SELECT username, score FROM sneakyscraper.scrape_whotwi.mikes_classifications 
    WHERE TRUE 
    QUALIFY 1 = ROW_NUMBER() OVER(PARTITION BY username ORDER BY date DESC)),
    onlymans7_scores_unique AS(
    SELECT username, score
    FROM onlymans7_scores A
    WHERE NOT EXISTS
    (SELECT username, score FROM recent_wizard_scores B WHERE B.username = A.username)),
    extrawomangirl_scores_unique AS(
    SELECT username, score
    FROM extrawomangirl_scores A
    WHERE NOT EXISTS
    (SELECT username, score FROM recent_wizard_scores B WHERE B.username = A.username)),
    all_scores AS(
    SELECT * FROM onlymans7_scores_unique
    UNION ALL
    SELECT * FROM extrawomangirl_scores_unique
    UNION ALL
    SELECT * FROM recent_wizard_scores)
    SELECT DISTINCT * FROM all_scores
    """

    query_job = client.query(sql)
    
    records = [dict(row) for row in query_job]
    
    parent_scores = {}
    for score_rec in records:
        score_temp = score_rec['score']
        if score_temp == 1:
            score_emoji = '🟢'
        elif score_temp == 2:
            score_emoji = '🟡'
        elif score_temp == 3:
            score_emoji = '🔴'
        else:
            score_emoji = '🔴'
            
        parent_scores[score_rec['username']] = score_emoji
    
    print("Exiting get_parent_scores_bq")
    
    return parent_scores

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

def flatten_dict(get_fof_dict):
    flattened_dict = {}
    for key in get_fof_dict.keys():
        flattened_dict.update(get_fof_dict[key])
    return flattened_dict


def compare_tapi_whotwi_bq(last_n_friends_dict_twitter, last_n_friends_dict_whotwi, last_n_friends_dict_bq):
    '''Returns a list or dictionary of num_friends from Whotwi or Twitter API
    to be written to BigQuery.
    
    Inputs
    ------
    last_n_friends_dict_twitter : dict
        Dictionary of metadata pulled via Twitter API. Flattened, i.e. each
        key is a username and each entry is a dictionary of metadata
    last_n_friends_dict_whotwi : dict
        Dictionary of metadata scraped from whotwi. Flattened, i.e. each
        key is a username and each entry is a dictionary of metadata
    last_n_friends_dict_bq : dict
        Dictionary of metadata from bigquery. Flattened, i.e. each
        key is a username and each entry is a dictionary of metadata
        
    Returns
    -------
    comparision_dict : dict
        Flattened metadata dictionary containing num_friends from whotwi and Twitter
        API as well as username and check_time.
    
    '''
    
    all_usernames = list(set(list(last_n_friends_dict_twitter.keys()) + list(last_n_friends_dict_whotwi.keys()) + list(last_n_friends_dict_bq.keys())))
    check_time = datetime.datetime.now()
    print(f"Calling compare_tapi_whotwi_bq with {len(all_usernames)} unique usernames")
    comparison_dict = {}
    for username in all_usernames:
        if username not in last_n_friends_dict_whotwi.keys():
            num_friends_whotwi = -1
        elif username in last_n_friends_dict_whotwi.keys():
            num_friends_whotwi = last_n_friends_dict_whotwi[username]['num_friends']
            
        if username not in last_n_friends_dict_twitter.keys():
            num_friends_twitter = -1
        elif username in last_n_friends_dict_twitter.keys():
            num_friends_twitter = last_n_friends_dict_twitter[username]['num_friends']
            
        if username not in last_n_friends_dict_bq.keys():
            num_friends_bq = -1
        elif username in last_n_friends_dict_bq.keys():
            num_friends_bq = last_n_friends_dict_bq[username]['num_friends']
        
        
        
        if len(set([num_friends_twitter, num_friends_whotwi, num_friends_bq])) != 1:
            comparison_dict_temp = {}
            comparison_dict_temp['username'] = username
            comparison_dict_temp['num_friends_twitter'] = num_friends_twitter
            comparison_dict_temp['num_friends_whotwi'] = num_friends_whotwi
            comparison_dict_temp['num_friends_bq'] = num_friends_bq
            comparison_dict_temp['check_time'] = check_time
            comparison_dict[username] = comparison_dict_temp
            
    return comparison_dict


def compare_tapi_whotwi(last_n_friends_dict_twitter, last_n_friends_dict_whotwi):
    '''Returns a list or dictionary of num_friends from Whotwi or Twitter API
    to be written to BigQuery.
    
    Inputs
    ------
    last_n_friends_dict_twitter : dict
        Dictionary of metadata pulled via Twitter API. Flattened, i.e. each
        key is a username and each entry is a dictionary of metadata
    last_n_friends_dict_whotwi : dict
        Dictionary of metadata scraped from whotwi. Flattened, i.e. each
        key is a username and each entry is a dictionary of metadata
        
    Returns
    -------
    comparision_dict : dict
        Flattened metadata dictionary containing num_friends from whotwi and Twitter
        API as well as username and check_time.
    
    '''
    
    all_usernames = list(set(list(last_n_friends_dict_twitter.keys()) + list(last_n_friends_dict_whotwi.keys())))
    check_time = datetime.datetime.now()
    print(f"Calling detect_changes_tapi_v_whotwi with {len(all_usernames)} unique usernames")
    comparison_dict = {}
    for username in all_usernames:
        if username in last_n_friends_dict_whotwi.keys() and username in last_n_friends_dict_twitter.keys():
            num_friends_twitter = last_n_friends_dict_twitter[username]['num_friends']
            num_friends_whotwi = last_n_friends_dict_whotwi[username]['num_friends']
        elif username in last_n_friends_dict_whotwi.keys() and username not in last_n_friends_dict_twitter.keys():
            num_friends_twitter = -1
            num_friends_whotwi = last_n_friends_dict_whotwi[username]['num_friends']
        elif username not in last_n_friends_dict_whotwi.keys() and username in last_n_friends_dict_twitter.keys():
            num_friends_twitter = last_n_friends_dict_twitter[username]['num_friends']
            num_friends_whotwi = -1

        if num_friends_twitter != num_friends_whotwi:
            comparison_dict_temp = {}
            comparison_dict_temp['username'] = username
            comparison_dict_temp['num_friends_twitter'] = num_friends_twitter
            comparison_dict_temp['num_friends_whotwi'] = num_friends_whotwi
            comparison_dict_temp['check_time'] = check_time
            comparison_dict[username] = comparison_dict_temp
    return comparison_dict

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
    print(f"Calling detect_changes_dict with lastnfriends {len(last_n_friends_dict)} and lastnfriendsbq {len(last_n_friends_bq_dict)}")
    changes_dict = {}
    for username in last_n_friends_dict.keys():
        if username in last_n_friends_bq_dict.keys():
            num_friends_new = last_n_friends_dict[username]['num_friends']
            num_friends_old = last_n_friends_bq_dict[username]['num_friends']
            diff_num_friends = num_friends_new-num_friends_old
            if diff_num_friends > 0 and num_friends_old > 0:
                if diff_num_friends > 100:
                    diff_num_friends = 100
                changes_dict[username] = {'num_friends': diff_num_friends}
        elif username not in last_n_friends_bq_dict.keys():
            changes_dict[username] = {'num_friends': 2}
    return changes_dict

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


def convert_dict_to_vertex(fof_metadata, parent_metadata, keys_to_keep=None):
    '''
    
    Input
    -----
    fof_metadata : dict
        Friends of parents dictionary. Friends of friends (not flattened)
        metadata dictionary containing usernames of parents as keys and
        dictionaries of their children accounts metadata. The format should
        match the output format of get_fof_parallel().
    parent_metadata : dict
        Dictionary of user metadata where keys are usernames and values are dictionaries of metadata

    Returns
    -------
    dict_for_vertex : dict
        Dictionary of metadata to send to Vertex API prediction endpoint.

    '''

    if keys_to_keep is None:
        keys_to_keep = ['username', 'list_name', 'bio', 'website', 'username_list_name_bio_website', 'fof_text']

    parent_metadata_ulbw = add_ulbw_str(parent_metadata)
    parent_w_fof = merge_metadata_fof_total(fof_metadata, parent_metadata_ulbw)

    vertex_metadata = []
    for username in parent_w_fof:
        parent_metadata_single = parent_w_fof[username]
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

#  fof_total = return_fof_ml_str(fof_dict)
#     merged_dicts = {}
#     for key in fof_total.keys():
#         metadata_dict_temp = metadata_dict[key].copy()
#         fof_total_temp = fof_total[key]
#         metadata_dict_temp.update(fof_total_temp)
#         merged_dicts[key] = metadata_dict_temp

    if keys_to_add is None:
        keys_to_add = ['username', 'list_name', 'bio', 'website']
    if separator is None:
        separator = ' '
    if new_key is None:
        new_key = 'username_list_name_bio_website'

    dict_with_ulbw = {}
    for username in parent_metadata.keys():
        user_metadata_temp = parent_metadata[username].copy()
        ulbw = ''
        for key in keys_to_add:
            ulbw += user_metadata_temp[key] + separator
        ulbw = ulbw[:-1]
        user_metadata_temp[new_key] = ulbw
        dict_with_ulbw[username] = user_metadata_temp

    return dict_with_ulbw



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

    print(f"Stream to BQ flattened_fof_dict: {flattened_fof_dict}")
    list_of_dicts = convert_dict_to_strs(flattened_fof_dict)
    print(f'Stream to BQ length of list_of_dicts: {len(list_of_dicts)}')
    print(f"Stream to BQ list_of_dicts.keys(): {list_of_dicts}")
    if num_batches > len(list_of_dicts):
        num_batches = len(list_of_dicts)
    if num_batches == 0:
        num_batches = 1
    print(f"Stream to BQ num_batches: {num_batches}")
    chunked_list_of_dicts = return_chunks(list_of_dicts, num_batches)
    print(f"Stream to BQ chunked_list_of_dicts: {chunked_list_of_dicts}")
    print(f"Stream to BQ len(chunked_list_of_dicts): {len(chunked_list_of_dicts)}")
    for sublist in chunked_list_of_dicts:
        errors = client.insert_rows_json(table_id, sublist)  # Make an API request.
        if errors == []:
            pass
        else:
            print(f"Encountered errors while inserting rows: {errors} to {table_id}")
    return errors



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
        char_ind = letter_to_num(username)[0]
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

def bqlod_to_dict(list_of_dicts, key='username'):
    '''Return a dictionary from a list of dicts
    
    Inputs
    ------
    list_of_dicts : list
        List of dictionaries
    key: str
        Dictionary key to use as mapping for dict_from_lists
        
    Returns
    -------
    dict_from_lists : dict
        Dictionary made from list_of_dicts with keys equal
        to key from each subdict
        
    '''
    
    dict_from_lists = {}
    for subdict in list_of_dicts:
        dict_from_lists[subdict[key]] = subdict
        
    return dict_from_lists

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

# ---------------------------------------- NLP UTILS ------------------------------------------- #

def common_member(a, b):
    a_set = set(a)
    b_set = set(b)
    if len(a_set.intersection(b_set)) > 0:
        return(True) 
    return(False)

def check_accounts_pl(get_metadata_dict, names_db, num_jobs=2):
    # Split up dictionary of username_metadata into num_jobs chunks
    try:
        chunked_accounts = return_chunks(get_metadata_dict, num_jobs)
    except Exception as e:
        print(f'Problem with chunking accounts {e}')

    # Add input arguments for get_metadata_bs4
    input_pl = [(username, names_db) for username in chunked_accounts]

    # Run get_metadata_bs4 in parallel
    classified_accounts_parallel = Parallel(n_jobs=num_jobs, verbose=10)(delayed(check_accounts)(arg1, arg2) for arg1, arg2 in input_pl)

    # Throw everything into a single dictionary
    classified_accounts = {}
    classified_accounts['nfts'] = {}
    classified_accounts['projects'] = {}
    classified_accounts['people'] = {}
    classified_accounts['crypto_people'] = {}
    classified_accounts['undetermined'] = {}
    classified_accounts['get_metadata_dict'] = {}
    for d in classified_accounts_parallel:
        classified_accounts['nfts'].update(d['nfts'])
        classified_accounts['projects'].update(d['projects'])
        classified_accounts['people'].update(d['people'])
        classified_accounts['crypto_people'].update(d['crypto_people'])
        classified_accounts['undetermined'].update(d['undetermined'])
        classified_accounts['get_metadata_dict'].update(d['get_metadata_dict'])


    return classified_accounts


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

#-------------------- Scrape Twitter ------------
def return_metadata_twitter(list_of_parents):
    headers = {'User-Agent': 
           'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:70.0) Gecko/20100101 Firefox/70.0 PyWebCopyBot/6.3.0',
           'Accept':'*/*',
           'Accept-Language': 'en-US,en;q=0.9'}
    metadata_dict = {}
    for username in list_of_parents:
        twitter_url = f'https://twitter.com/{username}/'
        twitter_request_response = requests.get(twitter_url, headers=headers)
        twitter_source_str = twitter_request_response.text
        twitter_source_str = twitter_source_str.replace('&quot;', """\"""")
        try:
            followers_count = re.findall(r'(?<="followers_count":)(.*)(?=,"friends_count")', twitter_source_str)
            num_followers = int(followers_count[0])
        except Exception as e:
            num_followers = 0
            print(f"Twitter num_followers exception: {e}")
            pass
        try:
            friend_count = re.findall(r'(?<="friends_count":)(.*)(?=,"listed_count")', twitter_source_str)
            num_friends = int(friend_count[0])
        except Exception as e:
            num_friends = 0
            print(f"Twitter num_friends exception: {e}")
            pass
        metadata_dict[username] = {'username': username, 'num_friends': num_friends, 'num_followers': num_followers}

    return metadata_dict

def check_twitter_metadata(twitter_metadata, list_of_parents):
    try:
        for username in twitter_metadata.keys():
            if twitter_metadata[username]['num_friends'] == 0:
                return list_of_parents
        return []
    except Exception as e:
        print(f"Exception in check_twitter_metadata: {e}")
        return list_of_parents

def get_telegram_subscribers(client):
    #  OLD SQL Query
    # sql = """WITH RANKED_MESSAGES AS( 
    # SELECT *, ROW_NUMBER() OVER (PARTITION BY username ORDER BY updated DESC) AS rn
    # FROM sneakyscraper.scrape_whotwi.telegram_subscribers),
    # LATEST_SUBSCRIPTIONS AS(
    # SELECT * EXCEPT (rn) FROM RANKED_MESSAGES WHERE rn = 1)
    # SELECT * FROM LATEST_SUBSCRIPTIONS
    # WHERE 
    # projects = TRUE 
    # OR undetermined = True 
    # OR nfts = TRUE 
    # OR people = True """
    # New SQL query (groups only)
    sql = """WITH RANKED_MESSAGES AS( 
    SELECT *, ROW_NUMBER() OVER (PARTITION BY username ORDER BY updated DESC) AS rn
    FROM sneakyscraper.scrape_whotwi.telegram_subscribers),
    LATEST_SUBSCRIPTIONS AS(
    SELECT * EXCEPT (rn) FROM RANKED_MESSAGES WHERE rn = 1)
    SELECT * FROM LATEST_SUBSCRIPTIONS
    WHERE userid < 0
    AND (projects = TRUE 
    OR undetermined = True 
    OR nfts = TRUE 
    OR people = True)"""
    try:
        query_job = client.query(sql)
        records = [dict(row) for row in query_job]
    except Exception as e:
        records = []
        print(f"Error with check_subscription_status: {e}")
        pass

    return records

def send_tg_msg(token, chat_ids, text):
    '''Send message through bot to given chat_id thread

    Inputs
    ------
    token : str
        API token of Telegram bot
    chat_ids : str or list of strs
        Unique chat identifier(s)
    text : str
        Text to send through bot

    Returns
    -------
    json_response : dict
        Dictionary with JSON response data
    
    '''
    # print(f"Inside send_tg_msg with {chat_ids}")
    json_response = {}
    if type(chat_ids) is list:
        if len(chat_ids) > 0:
            for chat_id in chat_ids:
                chat_id = int(chat_id)
                response_text = send_tg_msgs(token, chat_id, text)
                json_response[chat_id] = response_text
    elif type(chat_ids) is str or type(chat_ids) is int:
        chat_ids = int(chat_ids)
        json_response = send_tg_msgs(token, chat_ids, text)

    return json_response

def send_tg_msgs(token, chat_id, text):
    data = {'chat_id': chat_id, 
            'text': text,
            'parse_mode': 'HTML'}
    url = f'https://api.telegram.org/bot{token}/sendMessage'

    send_message = False
    while send_message is False:
        try:
            # print("Sending telegram message...")
            response = requests.post(url, data=data, timeout=5)
            json_response = json.loads(response.text)
            # print(f"response.status_code send_tg_msgs {response.status_code}")
            # print(f"json_response send_tg_msgs {json_response}")
            if json_response['ok'] is True:
                # print("Succesfully sent telegram message...")
                send_message = True
            elif json_response['ok'] is False:
                try:
                    print(f"send_tg_mesgs response: {json_response}, supposed to deliver: {text}")
                    seconds_to_wait = int(json_response['parameters']['retry_after'])
                    print(f"Sleeping for {seconds_to_wait} seconds because of bad response: send_tg_msgs")
                    time.sleep(seconds_to_wait)
                except Exception as e:
                    print(f"Problem with retry wait in send_message... {e}")
                    send_message = True
                    pass
        except Exception as e:
            print(f"Problem with sending telegram message: {e}")
            json_response = []
            send_message = True
            pass
    return json_response

# def send_tg_alerts(list_of_accounts, subscriber_ids):
#     if len(list_of_accounts) > 0:
#         account_types = list_of_accounts[0]['account_type_str']
#         print(f"Processing tg alerts for {account_types} - send_tg_alerts")
#     tg_responses = []
#     for account in list_of_accounts:
#         num_followers = account['num_followers']
#         twitter_url = account['twitter_url']
#         account_type_str = account['account_type_str']
#         tg_token = os.environ.get('SneakyTwitterProjects_bot')
#         response_tg = send_tg_msg(tg_token, subscriber_ids, f"{twitter_url} ({num_followers} followers - {account_type_str})")
#         tg_responses.append(response_tg)
#     return tg_responses

# tg_response = send_tg_updates_OzDaoProjectUpdates(tg_alert_accts, client, token=token)

def send_tg_updates_OzDaoProjectUpdates(accounts_classified, client, token=None):
    '''Send Telegram alerts.

    Inputs
    ------
    client : bigquery.Client()
        BigQuery Client object
    accounts_classified : dict
        Dictionary of classified accounts
    token : str
        Telegram Bot API Token

    Returns
    -------
    str : str
        String indicating that we've sent alerts
    
    '''

    if token is None:
        token = os.environ.get('OzDaoProjectsBot')
    
    OzDaoProjectAlerts_Group_ID = os.environ.get('OzDaoProjectAlerts_Group_ID')

    update_account_types = ['Crypto Project', 'NFT Project', 'Fund/DAO']

    # Get parent scores
    print('Getting parent scores from BigQuery inside send_tg_updates...')
    parent_scores = get_parent_scores_bq(client)

    for new_account, new_account_metadata in accounts_classified.items():
        print(f"Processing new_account: {new_account} for OzDaoProjectUpdates Group")
        print(f"OzDaoProjects new_account metadata: {new_account_metadata}")
        if new_account_metadata['account_type_str'] in update_account_types:

            score_emoji = ''
            parent_account = new_account_metadata['parent_account']
            if parent_account in parent_scores:
                if parent_scores[parent_account] == '🟢':
                    score_emoji = f" - {parent_scores[parent_account]}"

            twitter_username = new_account_metadata['username']
            num_followers = new_account_metadata['num_followers']
            twitter_url = new_account_metadata['twitter_url']
            ozdao_url = f"https://www.ozdao.app/talk/{twitter_username}"
            account_type_str = new_account_metadata['account_type_str']
            account_type_emoji = ''
            if account_type_str == 'Crypto Project':
                account_type_emoji = '📈'
            elif account_type_str == 'NFT Project':
                account_type_emoji = '🎨'
            elif account_type_str == 'Fund/DAO':
                account_type_emoji = '💰'

            try:
                if type(new_account_metadata['creation_date']) is datetime.datetime:
                    account_creation_date = new_account_metadata['creation_date'].strftime("%m/%d/%y")
                else:
                    account_creation_date = new_account_metadata['creation_date']
            except Exception as e:
                print('No creation date!')
                account_creation_date = 'No date!'
                pass

            # tg_message = f"{twitter_url} ({num_followers} followers - {account_type_str}{score_emoji})"
            tg_message = f"""<a href = "{twitter_url}">{twitter_username}</a>\n(<b>{account_type_str}</b> {account_type_emoji} - {num_followers} followers{score_emoji})\nCreation Date: {account_creation_date}"""

            # tg_message = f"""<a href = "{twitter_url}">{twitter_username}</a> \n(<b>{account_type_str}</b> - {num_followers} followers)\nCreation Date: {account_creation_date}"""
            # tg_message_response = send_tg_msg(token, OzDaoProjectAlerts_Group_ID, tg_message)
            tg_message_response = send_tg_msgs_with_url_buttons(token, OzDaoProjectAlerts_Group_ID, tg_message, ozdao_url)
            

    return "Finished Sending alerts to OzDaoProjectAlerts Telegram Group!"

def send_tg_msgs_with_url_buttons(token, chat_id, text, ozdao_url):
    data = {'chat_id': chat_id, 
            'text': text,
           'parse_mode': 'HTML',
           'reply_markup': json.dumps({ "inline_keyboard":
            [
                [
                    { "text": "Force Poll", "callback_data": "poll_vote" },
                    { "text": "Junk", "callback_data": "junk_vote" },
                    { "text": "Gem", "callback_data": "gem_vote" },
                    { "text": "Talk", "url": ozdao_url }
                ]
            ]
        })
           }
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    response = requests.post(url, data=data)
    json_response = json.loads(response.text)
    time.sleep(0.5)
    return json_response





def send_tg_updates(client, accounts_classified, token=None):
    '''Send Telegram alerts.

    Inputs
    ------
    client : bigquery.Client()
        BigQuery Client object
    accounts_classified : dict
        Dictionary of classified accounts
    token : str
        Telegram Bot API Token

    Returns
    -------
    str : str
        String indicating that we've sent alerts
    
    '''

    polls_table = 'sneakyscraper.scrape_whotwi.polls'
    if token is None:
        token = os.environ.get('SneakyTwitterProjects_bot')
    telegram_subscribers = get_telegram_subscribers(client)
    subscriptions = [{'projects': ['project', 'Crypto Project']}, {'undetermined': ['undetermined', 'Non-Crypto Project', 'Fund/DAO']}, 
                    {'nfts': ['nft', 'NFT Project']}, {'people': ['person', 'crypto_person', 'Crypto Person', 'Non-Crypto Person']}]
#     {0: 'Crypto Project',
#     1: 'Non-Crypto Project',
#     2: 'Crypto Person',
#     3: 'Non-Crypto Person',
#     4: 'NFT Project',
#     5: 'Fund/DAO'}

    # Get parent scores
    print('Getting parent scores from BigQuery inside send_tg_updates...')
    parent_scores = get_parent_scores_bq(client)

    # Loop over list of subscriptions
    for subscription in subscriptions:
        print("------------------------------------------------------------------------")
        print(f"Sending alerts for {subscription} subscription")
        tg_user_subscription = list(subscription.keys())[0]
        twitter_user_category = list(subscription.values())[0]
        list_of_accounts = []
        for category in twitter_user_category:
            list_of_accounts += return_account_type(accounts_classified, account_type_str=category)
        print(f"tg_user_subscription: {tg_user_subscription}")
        print(f"twitter_user_category: {twitter_user_category}")
        for subscriber in telegram_subscribers:
            tg_poll_msg_response = send_tg_polls_msgs(subscriber, tg_user_subscription, list_of_accounts, token, client, parent_scores)
            time.sleep(0.5)
            # print(f"tg_poll_msg_response: {tg_poll_msg_response}")
        print(f"Finished sending alerts for {subscription} subscription")
        print("------------------------------------------------------------------------")
    return "Finished Sending TG Alerts!"

def send_tg_polls_msgs(subscriber, subscription, list_of_accounts, token, client, parent_scores):
    '''Send out Telegram messages with polls.

    Inputs
    ------
    subscriber : dict
        Dictionary of subscriber data
    subscription : str
        Subscription string
    list_of_accounts : list
        List of dicts of account metadata
    token : str
        Telegram Bot API Token
    client : bigquery.Client()
        BigQuery Client object
    parent_scores : dict
        Dictionary of parent scores emojis

    Returns
    -------
    exit_str : str
        String indicating that we've sent out alerts to a subscriber.

    '''
    print(f"Sending alerts for {len(list_of_accounts)} accounts to {subscriber['username']}")
    # print('Getting parent scores from BigQuery...')
    # parent_scores = get_parent_scores_bq(client)

    polls_table = 'sneakyscraper.scrape_whotwi.polls'
    userid = subscriber['userid']
    username = subscriber['username']
    if subscriber[subscription] is True:
        for account in list_of_accounts:
            score_emoji = ''
            parent_account = account['parent_account']
            if parent_account in parent_scores:
                score_emoji = f" - {parent_scores[parent_account]}"

            twitter_username = account['username']
            num_followers = account['num_followers']
            twitter_url = account['twitter_url']
            account_type_str = account['account_type_str']
            try:
                if type(account['creation_date']) is datetime.datetime:
                    account_creation_date = account['creation_date'].strftime("%m/%d/%y")
                else:
                    account_creation_date = account['creation_date']
            except Exception as e:
                print('No creation date!')
                account_creation_date = 'No date!'
                pass

            # tg_message = f"{twitter_url} ({num_followers} followers - {account_type_str}{score_emoji})"
            tg_message = f"""<a href = "{twitter_url}">{twitter_username}</a> \n(<b>{account_type_str}</b> - {num_followers} followers{score_emoji}) \nCreation Date: {account_creation_date}"""
            tg_message_response = send_tg_msg(token, userid, tg_message)
            if subscriber['polls'] is True:
                tg_poll_response = send_poll(userid, twitter_username, token)
                if tg_poll_response != []:
                    poll_record = process_poll_response(tg_poll_response)
                    stream_result = stream_to_bq(poll_record, polls_table, client)
    exit_str = f"Finished sending {subscription} alerts to {username}!"
    return exit_str
# ----------------------------------- Telegram Polls ------------------------------
def build_poll_record(poll_poll, stream=True):
    poll_record = {}
    poll_record['id'] = poll_poll['id']
    poll_record['twitter_username'] = re.findall(r"(?<=\s)(\w*?)(?=\?)", poll_poll['question'])[0]

    if stream is True:
        poll_record['answers'] = {'list': [{'item': x['text']} for x in poll_poll['options']]}
        poll_record['responses'] = {'list': [{'item': x['voter_count']} for x in poll_poll['options']]}
    elif stream is False:
        poll_record['answers'] = [x['text'] for x in poll_poll['options']]
        poll_record['responses'] = [x['voter_count'] for x in poll_poll['options']]
    poll_record['is_closed'] = poll_poll['is_closed']
    poll_record['total_voter_count'] = poll_poll['total_voter_count']
    poll_record['type'] = poll_poll['type']
    poll_record['is_anonymous'] = poll_poll['is_anonymous']
    poll_record['allows_multiple_answers'] = poll_poll['allows_multiple_answers']
    poll_record['question'] = poll_poll['question']
    return poll_record

def process_poll_response(tg_json, stream=True, close_after=24):
    update_time = datetime.datetime.utcnow()
    if type(tg_json) is str:
        tg_json = json.loads(tg_json)
    elif type(tg_json) is requests.models.Response:
        tg_json = json.loads(tg_json.text)
    # are we processing a response or an update?
    # print(f"Processing poll: {tg_json}")
    if 'ok' in tg_json and 'result' in tg_json:
        if tg_json['ok'] is True:
            poll_poll = tg_json['result']['poll']
            poll_record = build_poll_record(poll_poll, stream)
#           -------------- Chat Properties -----------------
            poll_chat = tg_json['result']['chat']
            poll_record['chat_id'] = poll_chat['id']
            poll_record['message_id'] = tg_json['result']['message_id']
            if 'username' in poll_chat.keys():
                poll_record['sent_tg_username'] = poll_chat['username']
            elif 'title' in poll_chat.keys():
                poll_record['sent_tg_username'] = poll_chat['title']
            poll_record['sent_tg_type'] = poll_chat['type']
#           -------------- Time Properties -----------------
            date_sent = datetime.datetime.utcfromtimestamp(tg_json['result']['date'])
            date_close = date_sent + datetime.timedelta(hours=close_after)
#           -------------- User Properties -----------------
            
#           -------------- Add datetime information --------------
            if stream is True:
                poll_record['date_sent'] = date_sent.isoformat()
                poll_record['date_close'] = date_close.isoformat()
                poll_record['update_time'] = update_time.isoformat()
            elif stream is False:
                poll_record['date_sent'] = date_sent
                poll_record['date_close'] = date_close
                poll_record['update_time'] = update_time
        elif tg_json['ok'] is False:
            poll_record = {}    
    return poll_record

def send_poll(chat_id, twitter_username, token):
    data = {'chat_id': chat_id, 
            'question': f'What kind of account is {twitter_username}?',
            'options': '["Crypto Project", "Non-Crypto Project", \
            "Crypto Person", "Non-Crypto Person",\
            "NFT Project", "Fund/DAO", "Crypto Other", "Non-Crypto Other", "Private Account"]',
            'is_anonymous': False, 'disable_notification': True}
    url = f'https://api.telegram.org/bot{token}/sendPoll'

    send_poll_again = False
    while send_poll_again is False:
        try:
            response = requests.post(url, data=data, timeout=5)
            json_response = json.loads(response.text)
            if json_response['ok'] is True:
                send_poll_again = True
            elif json_response['ok'] is False:
                try:
                    print(f"send_poll response: {json_response}, supposed to deliver: {data}")
                    seconds_to_wait = 10
                    # seconds_to_wait = int(json_response['parameters']['retry_after'])
                    print(f"Sleeping for {seconds_to_wait} seconds because of bad response: send_poll")
                    time.sleep(seconds_to_wait)
                except Exception as e:
                    print(f"Problem with retry wait in send_poll... {e}")
                    send_poll_again = True
                    pass
        except Exception as e:
            print(f"Problem with sending poll: {e}")
            send_poll_again = True
            response = []
            pass
        
    return response

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

    print(f"Streaming dict to bigquery: {some_dict}")
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


# def group_changes(old_id, response_json):
#     if response_json['ok'] is False:
#         if 'parameters' in response_json:
#             if 'migrate_to_chat_id' in response_json['parameters']:
#                 new_chat_id = response_json['parameters']['new_chat_id']
            # 'parameters': {'migrate_to_chat_id': -1001601219808}

    # {'ok': False, 'error_code': 400, 'description': 'Bad Request: group chat was upgraded to a supergroup chat', 'parameters': {'migrate_to_chat_id': -1001601219808}}



# --------------------------------- End Telegram Polls ----------------------------

def return_account_type(accounts_classified, account_type_str='project'):
    '''Return accounts by type from a dictionary

    Inputs
    ------
    accounts_classified : dict
        Dictionary of Twitter accounts with classification
    account_type_str : str
        Type of account to return from accounts_classified

    Returns
    -------
    list_of_accounts : list
        List of dicts of accounts with account_type_str classification. Each entry is a dict.

    '''

    list_of_accounts = []
    for username in accounts_classified.keys():
        account_c = accounts_classified[username]
        if account_c['account_type_str'] == account_type_str:
            # TODO
            # if account_c['num_followers'] <= 250 and account_c['num_tweets'] <= 100 and account_c['num_friends'] <= 250:
            list_of_accounts.append(accounts_classified[username])    
    return list_of_accounts

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
        filt_props = {'num_followers': 2500, 'num_tweets': 333, 'num_friends': 300}

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


# ------------- ML Preprocessing Stuff --------
def return_fof_ml_str(fof_dict):
    '''Return fof feature str for ML.
    
    Inputs
    ------
    fof_dict : dict
        Dictionary of friends of friends. Key is username, values are metadata
        dictionaries of username's friends
        
    Returns
    -------
    fof_total : dict
        Dictionary with username keys, values are dictionary with fof_text and username as keys.
    
    '''
    
    fof_total = {}
    for parent_un, parent in fof_dict.items():
        total_str = ''
        for x in parent.values():
            temp_str = x['username'] + ' ' + x['list_name'] + ' ' + x['bio'] + ' ' + x['website']
            total_str = total_str + temp_str
        fof_total[parent_un] = {'username': parent_un, 'fof_text': total_str}

    return fof_total

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
        whotwi_temp['num_friends'] = tapi_temp['num_friends']
        whotwi_dict[username] = whotwi_temp

    return whotwi_dict


def merge_metadata_fof_total(fof_dict, metadata_dict):
    '''Return fof feature for crypto classifier.
    
    Inputs
    ------
    fof_dict : dict
        Dictionary of friends of friends. Key is username, values are metadata
        dictionaries of username's friends
    metadata_dict : dict
        Dictionary of metadata of "parents". Key is username, values are
        metadata for username.
        
    Returns
    -------
    merged_dicts : dict
        Dictionary containing user's metadata + fof_metadata feature
        as merged_dicts['username']['fof_text']
    
    '''
    
    fof_total = return_fof_ml_str(fof_dict)
    merged_dicts = {}
    for key in metadata_dict.keys():
        metadata_dict_temp = metadata_dict[key].copy()
        if key in fof_total.keys():
            fof_total_temp = fof_total[key]
        else:
            # sometimes get_fof doesn't work on all accounts
            fof_total_temp = {'username': key, 'fof_text': ' '}
        metadata_dict_temp.update(fof_total_temp)
        merged_dicts[key] = metadata_dict_temp
        
    return merged_dicts

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

    if api_endpoint is None:
        api_endpoint = "us-central1-aiplatform.googleapis.com"
    if location is None:
        location = "us-central1"
    print(f"vertex_pridct api_endpoint: {api_endpoint}, location: {location}")
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

def divide_chunks(l, n):
    '''
    Chunker for vertex pt1
    '''

    # looping till length l
    for i in range(0, len(l), n): 
        yield l[i:i + n]
        
        
def get_chunks(some_list, n):
    '''
    Chunker for vertex pt2
    '''
    list_of_chunks = list(divide_chunks(some_list, n))
    
    return list_of_chunks

def classify_users_ml_scale(project, endpoint_id, instances, location=None, api_endpoint=None, credentials=None):
    '''
    Split requests to Vertex so that we don't try to classify more than 5 accounts at a time
    '''
    # maximum number of predictions per request
    max_vai_preds = 5
    # Check length of instances
    if len(instances['instances']) >= max_vai_preds:
        print('splitting into subinstances...')
        # create new instances
        list_of_instances = get_chunks(instances['instances'], max_vai_preds)
        list_of_predictions = []
        for sub_instance in list_of_instances:
            temp_instance = {'instances': sub_instance}
            test_response = classify_users_ml(project=project, endpoint_id=endpoint_id, instances=temp_instance)
            list_of_predictions = list_of_predictions + test_response
            
    else:
        list_of_predictions = classify_users_ml(project=project, endpoint_id=endpoint_id, instances=instances)
        
    return list_of_predictions
    

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



def test_return_metadata_twitter():
    usernames = ["solsolsolsolso6", "yodges", "yfiyfiyfiyfi", "milesraymond12", "onlymans7", "extrawomangirl", "sbfsbfsbfsbfsbf"]
    test_twitter_metadata = return_metadata_twitter(usernames)
    print(test_twitter_metadata)


if __name__ == "__main__":
    test_return_metadata_twitter()