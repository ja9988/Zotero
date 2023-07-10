# Import statements
import datetime as datetime
import glob
import os
import re
from itertools import islice
import numpy as np
import requests
from bs4 import BeautifulSoup
from joblib import Parallel, delayed
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from dataclasses import dataclass, field
from typing import Dict, Tuple
import time
from dateutil.relativedelta import relativedelta
import random
import logging
from pywebcopy import Crawler, config
from csv import reader
import warnings
# --------------------------------------- Utilities -------------------------------------------- #

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

def gen_random_proxy():
    '''
    
    Inputs
    ------
    None
    
    Returns
    -------
    proxy : dict
        Proxy dictionary to be used with requests module.
    
    '''
    user = 'JRAF6YdA3AHo8uVwiE84qRvc'
    pw = '5nxBKTmC2DkLz5F2doiJC4VE'
    # proxies = ['atlanta.us.socks.nordhold.net', 
    #            'dallas.us.socks.nordhold.net',
    #            'los-angeles.us.socks.nordhold.net', 
    #            'us.socks.nordhold.net']
    proxies = ['amsterdam.nl.socks.nordhold.net', 
                'dublin.ie.socks.nordhold.net',
                'ie.socks.nordhold.net',
                'nl.socks.nordhold.net',
                'se.socks.nordhold.net',
                'stockholm.se.socks.nordhold.net']
    random_proxy = random.choice(proxies)
    proxy_url = f"socks5://{user}:{pw}@{random_proxy}:1080"
    
    proxy = {'https': proxy_url}
    
    return proxy

# def list_chunks(l, n):
#     '''Function that returns a list of n subslists of l.
    
#     Inputs
#     ------
#     l : list
#         List of items
#     n : int
#         Number of splits to make in list
    
#     Returns
#     -------
#     chunk_list : list
#         List of n lists, each a portion of list l
    
#     '''
#     chunk_list = []
#     inds = list(range(0, len(l), len(l) // n))
#     if inds[-1] != len(l):
#         inds = inds + [len(l)]
#     if len(inds) > n + 1:
#         inds.pop(-2)
#     # print(inds)
#     for i in range(len(inds) - 1):
#         temp_list = l[inds[i]:inds[i + 1]]
#         chunk_list.append(temp_list)
#     if n == 1 or n > len(l):
#         chunk_list = l
#     return chunk_list

# def dict_chunks_iter(data, size=1):
#     '''Iterator for returning chunks of a dictionary.
    
#     Inputs
#     ------
#     data : dict
#         Dictionary to split up
#     size : int
#         Length of new sub dictionaries

#     Yields
#     ------
#     dict_chunks_iter : iterator
#         Iterator

#     '''
#     it = iter(data)
#     for i in range(0, len(data), size):
#         yield {k: data[k] for k in islice(it, size)}


# def dict_chunks(data, num_jobs=1):
#     '''Function that returns a list of sub dictionaries

#     Inputs
#     ------
#     data : dict
#         Dictionary to be split
#     num_jobs : int
#         Number of splits to make

#     Returns
#     -------
#     list_of_dicts : list
#         List of num_jobs sub dicts of data
    
#     '''

#     if len(data) % num_jobs == 0:
#         size = len(data) // num_jobs
#         print('here1, size')
#     else:
#         size = len(data) // num_jobs + 1
#         print('here2', size)
#     list_of_dicts = []
#     for item in dict_chunks_iter(data, size):
#         list_of_dicts = list_of_dicts + [item]

#     return list_of_dicts

# def return_chunks(data, num_splits):
#     '''Function to split up a dictionary or list into a list of sub dicts or sub lists.
    
#     Inputs
#     ------
#     data : dict or list
#         Dictionary or list of items
#     num_splits : int
#         Number of times to split the dictionary or list
    
#     Returns
#     -------
#     chunked_data : list
#         List of sub dicts or sub lists of data

#     '''
#     if type(data) is dict:
#         chunked_data = dict_chunks(data, num_splits)
#     if type(data) is list:
#         chunked_data = list_chunks(data, num_splits)

#     return chunked_data

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

def return_all_friends(new_dict):
    ''' Some useless piece of shit.

    Inputs
    ------
    new_dict : dict
        Dictionary of friends of friends without driver object

    Returns
    -------
    all_friends : list
        List of all friends from friends of friends dictionary

    '''
    all_friends = []
    counter = 0
    for key, value in new_dict.items():
        all_friends = all_friends + value['friends']
    return all_friends

def flatten_dict_of_friends(dict_of_friends):
    '''
    
    Input
    -----
    dict_of_friends : dict
        Dictionary of friends where key is username and value is dictionary with 'friends' key where values are lists of friends

    Returns
    -------
    list_of_friends : list
        List of friends from dictionary
    
    
    '''
    list_of_friends = [item for sublist in [[j for j in dict_of_friends[i]['friends']] for i in dict_of_friends if dict_of_friends[i]['friends']] for item in sublist]

    # remove Nonetype
    list_of_friends = [x for x in list_of_friends if x is not None]

    return list_of_friends

def update_master(current_dict, master_dict):
    '''Update master dictionary.
    
    Inputs
    ------
    current_dict : dic
        Dictionary of usernames just crawled with whatever metadata.
    master_dict : dic
        Master dictionary of usernames

    Returns
    -------
    master_dict : dic
        Updated master dictionary of usernames
    
    '''

    current_usernames = list(current_dict.keys())
    master_usernames = list(master_dict.keys())
    new_usernames = list(set(current_usernames).difference(master_usernames))

    for new_username in new_usernames:
        master_dict[new_username] = current_dict[new_username]

    return new_usernames, master_dict

def check_friend_changes(current_dict, old_dict, max_detect_lim=25):
    '''Check the number of new friends added.
    
    Inputs
    ------
    current_dict : dic
        Dictionary of current metadata as returned by get_metadata_bs4
    old_dict : dic
        Dictionary of old metadata as returned by get_metadata_bs4

    Returns
    -------
    new_friends : dict
        Dictionary of number of friend changes from previous crawl
    
    '''
    current_dict = fof_to_get_metadata(current_dict)
    old_dict = fof_to_get_metadata(old_dict)
    new_friends = {}
    for username in list(set(list(current_dict.keys())).intersection(list(old_dict.keys()))):
        old_num_friends = old_dict[username]['num_friends']
        current_num_friends = current_dict[username]['num_friends']
        num_friends_changes = current_num_friends - old_num_friends
        if num_friends_changes < max_detect_lim and num_friends_changes > 0:
            new_friends[username] = {'num_friends': num_friends_changes}
        else:
            #print('No changes, max detect limit exceeded or decrease in friends for account @', username)
            pass
    return new_friends

def clean_master_dict(master_dict):

    master_dict_clean = {}
    for username in master_dict.keys():
        metadata = master_dict[username]
        nfr = metadata['num_friends']
        nfo = metadata['num_followers']
        ntw = metadata['num_tweets']
        ncr = metadata['num_crush']
        nco = metadata['num_crushed_on']
        checksum_1 = nfr + nfo + ntw + ncr + nco

        crd = metadata['creation_date']
        bio = metadata['bio']
        loc = metadata['location']
        wbs = metadata['website']
        blw = [crd, bio, loc, wbs]
        checksum_2 = sum(x is not None for x in blw)

        checksum = checksum_1 + checksum_2

        if checksum == 0:
            print(f"No metadata found for {username}!")
        else:
            master_dict_clean[username] = metadata
    # return cleaned dict
    return master_dict_clean

def fof_to_get_metadata(fof_metadata, get_parents_metadata=False):
    '''Transforms a fof dictionary to a get_metadata type dictionary (flat)

    Inputs
    ------
    fof_metadata_dict : dict
        Metadata dictionary generated by get_friends_of_friends_bs4 functions

    Returns
    -------
    flat_dict : dict
        Metadata dictionary like get_friends_of_friends_bs4 metadata dictionary
    
    '''
    flat_dict = {}
    parent_accounts = list(fof_metadata.keys())
    if get_parents_metadata is True:
        if len(parent_accounts) < 10:
            get_metadata_dict = get_metadata_bs4(parent_accounts, metadata_requests=MetadataRequests().friends_of_friends)
        elif len(parent_accounts) >=10 and len(parent_accounts) < 80:
            print("Going parallel in fof_to_get_metadata 1")
            num_jobs = len(parent_accounts)//2
            get_metadata_dict = get_metadata_bs4_pl(parent_accounts, metadata_requests=MetadataRequests().friends_of_friends, num_jobs=num_jobs)
        else:
            print("Going parallel in fof_to_get_metadata 2")
            num_jobs = 64
            get_metadata_dict = get_metadata_bs4_pl(parent_accounts, metadata_requests=MetadataRequests().friends_of_friends, num_jobs=num_jobs)

        flat_dict.update(get_metadata_dict)
    
    for parent_account in parent_accounts:
        child_accounts_metadata = fof_metadata[parent_account]
        flat_dict.update(child_accounts_metadata)

    return flat_dict

# ------------------------------------- End Utilities ------------------------------------------ #

def get_friends(usernames, chrome_driver_path=None, friend_limit=500, timeout_lim=10):
    '''Function to return friends of twitter_handle.

    Inputs
    ------
    twitter_handle : str
        String of twitter handle to find friends (excluding @ symbol)
    chrome_driver_path : str
        Path to Selenium Chrome driver
    active_driver : selenium.webdriver.chrome.webdriver.WebDriver
        Active Selenium Chrome webdriver session to use
    quit : bool
        Choose to quit Selenium session or not
    friend_limit : int
        Maximum number of friends to try and retrieve
    timeout_lim : int
        Number of seconds to wait for whotwi website to load

    Returns
    -------
    return_dict : dict
        Dictionary containing friends, number of friends and Selenium driver session

    '''

    if chrome_driver_path is None:
        if len(usernames) > 20:
            friends_counts = get_friends_bs4_pl(usernames, friend_limit=friend_limit, num_jobs=len(usernames)//10)
        else:
            friends_counts = get_friends_bs4(usernames, friend_limit=friend_limit, timeout_lim=timeout_lim)
    else:
        if len(usernames) > 20:
            friends_counts = get_friends_selenium_pl(usernames, chrome_driver_path=chrome_driver_path, friend_limit=friend_limit, num_jobs=len(usernames)//10)
        else:
            friends_counts = get_friends_selenium(usernames, chrome_driver_path=chrome_driver_path, friend_limit=friend_limit, timeout_lim=timeout_lim)

    
    return friends_counts

# ---------------------------------------- Selenium -------------------------------------------- #

def get_friends_selenium_driver(username, driver, timeout_lim=10, friend_limit=500):
    '''Function to return friends_count using selenium
    
    Inputs
    ------
    username : str
        Twitter username
    driver : selenium.webdriver.chrome.webdriver.WebDriver
        Selenium Chrome webdriver
    timeout_lim : int
        Number of seconds to wait for page to load in selenium instance
    friend_limit : int
        Maximum number of friends we would like to return

    Returns
    -------
    friends_count : dict
        Dictionary containing friends, num_friends
    
    '''

    # Compose full URL for whotwi website
    full_url = f"https://en.whotwi.com/{username}/friends?view_type=icon"
    # Go to whotwi friends page url
    driver.get(full_url)

    # Wait for page to completely load before extracting friends
    timeout = timeout_lim
    try:
        element_present = EC.presence_of_element_located(
            (By.XPATH, '//html/body/div[3]/div[3]/div[1]/div[2]/div[3]/div/ul/li/a[@href]'))
        WebDriverWait(driver, timeout).until(element_present)
    except TimeoutException:
        print("Timed out waiting for page to load")

    # If whotwi has problems, skip this username
    try:
        # Find number of friends
        num_friends = int(driver.find_elements_by_xpath("//html/body/div[3]/div[3]/div[1]/ul/li[3]/a/div")[0].text)
        # Find friends urls
        friend_elems = driver.find_elements_by_xpath(
            "//html/body/div[3]/div[3]/div[1]/div[2]/div[3]/div/ul/li/a[@href]")
        # Impose limit on number of friends to try and retrieve
        if num_friends > friend_limit:
            friends = [x.get_attribute("href").replace("https://en.whotwi.com/", "") for x in
                       friend_elems[0:friend_limit]]
        else:
            friends = [x.get_attribute("href").replace("https://en.whotwi.com/", "") for x in friend_elems]
    except:
        num_friends = None
        friends = None
        pass

    # Package result into a dictionary with keys "friends", "num_friends"
    friends_count = {"friends": friends, "num_friends": num_friends}

    return friends_count

def get_friends_selenium(username, chrome_driver_path, friend_limit=500, timeout_lim=20):
    '''Function to return friends of username(s).

    Inputs
    ------
    username : str or list or dict
        Twitter username to find friends (excluding @ symbol)
    chrome_driver_path : str
        Path to Selenium Chrome driver
    friend_limit : int
        Maximum number of friends to try and retrieve
    timeout_lim : int
        Number of seconds to wait for whotwi website to load

    Returns
    -------
    friends_counts : dict
        Dictionary with username key, value is dict with keys friends, num_friends

    '''

    # Selenium options (may need to make this more flexible later...)
    options = Options()
    options.add_argument("--headless")
    # Spin up a selenium session
    driver = webdriver.Chrome(executable_path=chrome_driver_path, options=options)
    # Maximize browser window
    driver.maximize_window()

    # Detect type of username and interact with get_friends_selenium_driver appropriately
    friends_counts = {}
    if type(username) is str:
        # Call get_friends_selenium_driver
        friends_count = get_friends_selenium_driver(username, driver, timeout_lim=timeout_lim, friend_limit=friend_limit)
        friends_counts[username] = friends_count
    if type(username) is list:
        for single_username in username:
            # Call get_friends_selenium_driver
            friends_count = get_friends_selenium_driver(single_username, driver, timeout_lim=timeout_lim, friend_limit=friend_limit)
            friends_counts[single_username] = friends_count
    if type(username) is dict:
        for single_username, metadata in username.items():
            if 'num_friends' in metadata.keys():
                num_friends_new = metadata['num_friends']
            # Call get_friends_selenium_driver
            friends_count = get_friends_selenium_driver(single_username, driver, timeout_lim=timeout_lim, friend_limit=num_friends_new)
            friends_counts[single_username] = friends_count

    # Kill selenium session
    driver.quit()

    return friends_counts

def get_friends_selenium_pl(usernames, chrome_driver_path, friend_limit=500, timeout_lim=20, num_jobs=2):
    '''Function to return friends from many accounts using parallel selenium sessions.
    
    Inputs
    ------
    usernames : dict or list
        Dictionary with twitter usernames as keys, num_friends_new as value
        Alternatively, a list of twitter usernames
    chrome_driver_path : str
        Path to Selenium Chrome driver
    num_jobs : int
        Number of parellel selenium sessions to spin up

    Returns
    -------
    friends_counts : dict
        Dictionary with username key, value is dict with keys friends, num_friends


    '''

    # Split up dictionary or list of usernames into num_jobs chunks
    chunked_usernames = return_chunks(usernames, num_jobs)

    # Add input arguments for get_friends_selenium
    input_pl = [(username, chrome_driver_path, friend_limit, timeout_lim) for username in chunked_usernames]

    # Run selenium sessions in parallel
    friends_counts_parallel = Parallel(n_jobs=num_jobs, verbose=10)(delayed(get_friends_selenium)(arg1, arg2, arg3, arg4) for arg1, arg2, arg3, arg4 in input_pl)

    # Throw everything into a single dictionary
    friends_counts = {}
    for d in friends_counts_parallel:
        friends_counts.update(d)

    return friends_counts

# -------------------------------------- End Selenium ------------------------------------------ #


# -------------------------------------- Requests/BS4 ------------------------------------------ #
def get_friends_bs4_driver(username, friend_limit=500, timeout_lim=10):
    '''Function to return friends_count using requests and bs4

    Inputs
    ------
    username : str
        Twitter username
    timeout_lim : int
        Number of seconds to wait for page to load in selenium instance
    friend_limit : int
        Maximum number of friends we would like to return

    Returns
    -------
    friends_count : dict
        Dictionary containing friends, num_friends

    '''
    # Get a random proxy - put this here so each username has a different proxy to simulate
    # real "browsing" lmao
    proxy  = gen_random_proxy()

    # Link for first whotwi page
    whotwi_link = f'https://en.whotwi.com/{username}/friends/user_list?page=1&view_type=icon&refresh=1&no_cache=1'

    # Get first page of friends
    try:
        # Submit GET request
        whotwi_get = requests.get(whotwi_link, headers=RequestHeaders().default, timeout=timeout_lim)
        # whotwi_get = requests.get(whotwi_link, headers=RequestHeaders().default, timeout=timeout_lim, proxies=proxy)
        # Parse with bs4
        whotwi_soup = BeautifulSoup(whotwi_get.text, 'html.parser')
    except:
        pass

    # Get list of friends from first page
    try:
        friends = [friend_url['href'].replace('/', '') for friend_url in whotwi_soup.find_all('a')]
    except:
        friends = [None]

    # Get friends count from first page
    try:
        num_friends_elem = whotwi_soup.find("div", {"class": "user_list_icon_line"})
        num_friends = int(re.findall(r'\d+', num_friends_elem.text)[0])
    except:
        num_friends = 0
    
    # Try to prevent from loading dead pages
    if friend_limit > num_friends and num_friends > 0:
        friend_limit  = num_friends
    
    # If there are 2000 friends on the first page, we need to loop over the pages
    num_friends_first_page = len(friends)
    if num_friends_first_page == 2000:
        # Determine number of pages
        num_pages = friend_limit//2000 + 2
        # Loop over pages
        for page in range(2, num_pages):
            # Create link
            whotwi_link = f'https://en.whotwi.com/{username}/friends/user_list?page={page}&view_type=icon&refresh=1&no_cache=1'
            try:
                # Submit GET request
                if proxy is not None:
                    whotwi_get = requests.get(whotwi_link, headers=RequestHeaders().default, timeout=timeout_lim, proxies = proxy)
                else:
                    whotwi_get = requests.get(whotwi_link, headers=RequestHeaders().default, timeout=timeout_lim)
                # Parse with bs4
                whotwi_soup = BeautifulSoup(whotwi_get.text, 'html.parser')
            except:
                pass
            
            # Check if "Not Found" error in whotwi_get
            not_found_element = whotwi_soup.find('div', {'class': 'alert alert-info'})
            if not_found_element is not None:
                break

            # Otherwise, get list of friends
            try:
                friends_temp = [friend_url['href'].replace('/', '') for friend_url in whotwi_soup.find_all('a')]
            except:
                friends_temp = [None]
            friends = friends + friends_temp

    # Throw it all into a dictionary
    friends_count = {'friends': friends[0:friend_limit], 'num_friends': num_friends}
    
    return friends_count

def get_friends_bs4(username, friend_limit=500, timeout_lim=20):
    '''Function to return friends of username(s).

    Inputs
    ------
    username : str or list or dict
        Twitter username to find friends (excluding @ symbol)
    friend_limit : int
        Maximum number of friends to try and retrieve
    timeout_lim : int
        Number of seconds to wait for whotwi website to load

    Returns
    -------
    friends_counts : dict
        Dictionary with username key, value is dict with keys friends, num_friends

    '''

    # Detect type of username and interact with get_friends_bs4_driver appropriately
    friends_counts_temp = {}
    if type(username) is str:
        # Call get_friends_bs4_driver
        friends_count = get_friends_bs4_driver(username, friend_limit=friend_limit, timeout_lim=timeout_lim)
        friends_counts_temp[username] = friends_count
    if type(username) is list:
        for single_username in username:
            # Call get_friends_bs4_driver
            friends_count = get_friends_bs4_driver(single_username, friend_limit=friend_limit, timeout_lim=timeout_lim)
            friends_counts_temp[single_username] = friends_count
    if type(username) is dict:
        for single_username, metadata in username.items():
            if 'num_friends' in metadata.keys():
                num_friends_new = metadata['num_friends']
            else:
                num_friends_new = friend_limit
            # Call get_friends_bs4_driver
            friends_count = get_friends_bs4_driver(single_username, friend_limit=num_friends_new, timeout_lim=timeout_lim)
            friends_counts_temp[single_username] = friends_count
    
    # Remove Nonetypes:
    friends_counts= {}
    for single_username in friends_counts_temp.keys():
        if friends_counts_temp[single_username]['friends'] is not None:
            friends_counts[single_username] = friends_counts_temp[single_username]

    return friends_counts

def get_friends_bs4_pl(usernames, friend_limit=500, timeout_lim=20, num_jobs=2):
    '''Function to return friends from many accounts using parallel selenium sessions.
    
    Inputs
    ------
    usernames : dict or list
        Dictionary with twitter usernames as keys, metadata dict as value with 'num_friends' key
        Alternatively, a list of twitter usernames
    friend_limit : int
        Maximum number of friends to try and retrieve
    num_jobs : int
        Number of parallel get_friends_bs4 jobs

    Returns
    -------
    friends_counts : dict
        Dictionary with username key, value is dict with keys friends, num_friends


    '''

    # Split up dictionary or list of usernames into num_jobs chunks
    chunked_usernames = return_chunks(usernames, num_jobs)

    # Add input arguments for get_friends_bs4
    input_pl = [(username, friend_limit, timeout_lim) for username in chunked_usernames]

    # Run get_friends_bs4 in parallel
    friends_counts_parallel = Parallel(n_jobs=num_jobs, verbose=10)(delayed(get_friends_bs4)(arg1, arg2, arg3) for arg1, arg2, arg3 in input_pl)

    # Throw everything into a single dictionary
    friends_counts = {}
    for d in friends_counts_parallel:
        friends_counts.update(d)

    return friends_counts



def get_metadata_bs4_driver(username, timeout_lim=10, metadata_requests=None):
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
        metadata_requests = MetadataRequests().default

    # Get a random proxy - put this here so each username has a different proxy to simulate
    # real "browsing" lmao
    proxy  = gen_random_proxy()

    # Headers for bs4
    headers = RequestHeaders().default

    # Counting errors
    max_errors = sum(list(metadata_requests.values()))
    if metadata_requests['twitter_url'] is True:
        max_errors = max_errors - 1

    # Compose full URL for whotwi website
    full_url = f"https://en.whotwi.com/{username}/friends_except_followers"

    # Go to whotwi friends page url

    # for attempt in range(10):
    #     try:
    #         result = sheets_service.spreadsheets().values().update(
    #         spreadsheetId=spreadsheet_id, range=request_range,
    #         valueInputOption='USER_ENTERED', body=body).execute()
    #     except HttpError as err:
    #         if err.resp.status in [403, 500, 503]:
    #             time.sleep(5)
    #     else:
    #         break
    # else:
    #     pass     
    # 
    for attempt in range(10):   
        try:
            whotwi_request = requests.get(full_url, headers=headers, timeout=timeout_lim).text
            soup_whotwi_1 = BeautifulSoup(whotwi_request, 'lxml')
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
            print('-------------ERROR-------------ERROR-------------')
            print('TIMEOUT ERROR: THROTTLED?')
            print('-------------ERROR-------------ERROR-------------')
            print(f'RETRYING... ATTEMPT NUMBER {attempt}')
            print(full_url)
            print(e)
            sleep_time = random.randint(2,7)
            time.sleep(sleep_time)
        else:
            break
    else:
        print('-----------------------Get Metadata Fail-----------------------')
        metadata = MetadataDefaults().get_metadata_fail
        # whotwi_request = requests.get(full_url, headers=headers, timeout=timeout_lim, proxies=proxy).text
    # try:
    #     # Create BeautifulSoup object and parse
    #     soup_whotwi_1 = BeautifulSoup(whotwi_request, 'lxml')
    #     metadata = MetadataDefaults().default
    #     metadata = _username_get_metadata(username, metadata, metadata_requests)
    #     metadata = _userid_get_metadata(soup_whotwi_1, metadata, metadata_requests)
    #     metadata = _list_name_get_metadata(soup_whotwi_1, metadata, metadata_requests)
    #     metadata = _num_friends_get_metadata(soup_whotwi_1, metadata, metadata_requests)
    #     metadata = _num_followers_get_metadata(soup_whotwi_1, metadata, metadata_requests)
    #     metadata = _num_tweets_get_metadata(soup_whotwi_1, metadata, metadata_requests)
    #     metadata = _num_crush_get_metadata(soup_whotwi_1, metadata, metadata_requests)
    #     metadata = _num_crushed_on_get_metadata(soup_whotwi_1, metadata, metadata_requests)
    #     metadata = _creation_date_get_metadata(soup_whotwi_1, metadata, metadata_requests)
    #     metadata = _twitter_url(username, metadata, metadata_requests)
    #     metadata = _bio_get_metadata(soup_whotwi_1, metadata, metadata_requests)
    #     metadata = _location_get_metadata(soup_whotwi_1, metadata, metadata_requests)
    #     metadata = _website_get_metadata(soup_whotwi_1, metadata, metadata_requests)
    #     metadata = _last_tweet_get_metadata(username, metadata, metadata_requests)
    #     metadata = _last_checked_get_metadata(metadata, metadata_requests)
    # except Exception as e:
    #     print('-----------------------Get Metadata Error-----------------------')
    #     print(full_url)
    #     print(e)
    #     metadata = MetadataDefaults().get_metadata_fail
    #     pass

    return metadata

def get_metadata_bs4(username, timeout_lim=10, metadata_requests=None):
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
        metadata_requests = MetadataRequests().default

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
    proxy  = gen_random_proxy()
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

def get_friends_of_friends_bs4_page(username, page, timeout_lim=10, metadata_requests=None, session=None):
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
        # Parse with bs4
        whotwi_soup = BeautifulSoup(whotwi_get.text, 'html.parser')
    except:
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

def get_fof_bs4_page(username, page, timeout_lim=10, metadata_requests=None, session=None):
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
    for attempt in range(10):
        try:
            # Submit GET request
            whotwi_get = session.get(whotwi_link, timeout=timeout_lim)
            # Parse with bs4
            whotwi_soup = BeautifulSoup(whotwi_get.text, 'html.parser')
        except Exception as e:
            print(f'Failed to load page {page} for {username}')
            print(f'Will retry... attempt {attempt} logged')
            sleep_time = random.randint(2,7)
            time.sleep(sleep_time)
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
    except:
        print(f"Missing page {page} for {username}!")
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
    proxy  = gen_random_proxy()
    session = requests.Session()
    # session.proxies.update(proxy)
    session.headers.update(RequestHeaders().default)

    if metadata_requests is None:
        metadata_requests = MetadataRequests().friends_of_friends
    
    fof_metadata  = return_metadata_fof_parallel(username, friend_limit)
    username_page_tuples = return_inputs_fof_parallel(fof_metadata)
    input_pl = [(entry[0], entry[1], timeout_lim, metadata_requests, session) for entry in username_page_tuples]

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
        if len(usernames) > 2:
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

# ------------------------------------- End Requests/BS4 --------------------------------------- #

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
            pass
        else:
            last_tweet_request = requests.get(last_tweet_url, headers=RequestHeaders().default, timeout=10).text
            last_tweet_soup = BeautifulSoup(last_tweet_request, 'lxml')
            try:
                tweet_list_element = last_tweet_soup.find(lambda tag: tag.name == 'ul' and tag.get('class') == ['tweet_list'])
                date_last_tweet_tag_as_str = str(tweet_list_element.find(lambda tag: tag.name == 'li' and tag.get('class') == ['date']))
                date_str_from_url = re.findall(r'archive/(.*?)">', date_last_tweet_tag_as_str)[0]
                metadata['last_tweet'] = datetime.datetime.strptime(date_str_from_url, '%Y/%m/%d')
            except:
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

# ------------------------------------ Twitter Scraping ---------------------------------------- #

def return_metadata_twitter(list_of_parents):
    metadata_dict = {}
    for username in list_of_parents:
        # print(f'Attempting to get metadata for {username}...')
        metadata_dict[username] = return_metadata_twitter_user(username)[username]

    return metadata_dict


def return_metadata_twitter_user(username):
    print(f'Getting twitter metadata for {username}')
    logging.getLogger("pywebcopy").setLevel(logging.CRITICAL)
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)
    twitter_url = f'https://twitter.com/{username}/'
#     config['bypass_robots'] = True
    for attempt in range(5):
        try:
            proxies = gen_random_proxy()
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                kwargs = {'bypass_robots': True, 'debug': False}
                # 'DEBUG': False 
                config.setup_config(twitter_url, 'temp', 'temp', **kwargs)
                # Create a instance of the webpage object
                wp = Crawler()
                # Get the URL
                wp.get(twitter_url, proxy = proxies)
            twitter_source_bytes = wp._source.read()
            twitter_source_str = twitter_source_bytes.decode('utf-8')
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
            metadata_dict_user = {username: {'username': username, 'num_friends': num_friends, 'num_followers': num_followers}}
        except Exception as e:
            sleep_time = random.randint(2,7)
            # print(f'Exception: {e} for {username}')
            # print(f"Attempt number {attempt}, will retry in {sleep_time} seconds")
            time.sleep(sleep_time)
        else:
            print(f"Succesfully retrieved metadata for {username} on attempt {attempt}")
            break
    else:
        print(f"Couldn't retrieve metadata for {username}")
        metadata_dict_user = {username: {'username': username, 'num_friends': 'FAIL', 'num_followers': 'FAIL'}}
    return metadata_dict_user

def return_metadata_twitter_user_pl(usernames, num_jobs=2):
    # Run get_metadata_bs4 in parallel
    twitter_metadata_parallel = Parallel(n_jobs=num_jobs, verbose=10)(delayed(return_metadata_twitter_user)(arg1) for arg1 in usernames)

    # Throw everything into a single dictionary
    twitter_metadata = {}
    for d in twitter_metadata_parallel:
        twitter_metadata.update(d)

    return twitter_metadata

def return_metadata_twitter_user_chunks_pl(usernames, num_jobs=2):
    # Run get_metadata_bs4 in parallel
    chunked_usernames = return_chunks(usernames, num_splits=num_jobs)
    twitter_metadata_parallel = Parallel(n_jobs=num_jobs, verbose=10)(delayed(return_metadata_twitter)(arg1) for arg1 in chunked_usernames)

    # Throw everything into a single dictionary
    twitter_metadata = {}
    for d in twitter_metadata_parallel:
        twitter_metadata.update(d)

    return twitter_metadata


def return_tracked_usernames(filepath):
    with open(filepath, 'r') as read_obj:
        # pass the file object to reader() to get the reader object
        csv_reader = reader(read_obj)
        # Pass reader object to list() to get a list of lists
        list_of_rows = list(csv_reader)
    tracked_usernames = [x[0] for x in list_of_rows]
    tracked_usernames.pop(0)
    return tracked_usernames

# ---------------------------------- End Twitter Scraping -------------------------------------- #

def recursive_friends(parent_account, level=1, parent_limit=20, child_limit=100, timeout_lim=10, num_jobs=1):
    '''

    Inputs
    ------
    parent_account : str
        Twitter username for parent node
    level : int
        Number of recursive levels to crawl
    parent_limit : int
        Maximum number of friends of parent account to track
    child_limit : int
        Maximum number of friends of child accounts to retrieve
    timeout_lim : int
        Number of seconds to wait for whotwi website to loads

    Returns
    -------
    ff_dict : dict
        Dictionary of get_friends dicts, one entry for each child accounts

    '''

    # Get friends from parent account
    parent_dict = get_friends_bs4(parent_account, friend_limit=10000, timeout_lim=timeout_lim)
    # Mother friends
    parent_friends = parent_dict[parent_account]['friends']

    # Create dictionary with key is username and value is dict with key 'friend'
    if num_jobs == 1:
        ff_dict = get_friends_bs4(parent_friends, friend_limit=child_limit, timeout_lim=timeout_lim)
    else:
        ff_dict = get_friends_bs4_pl(parent_friends, friend_limit=child_limit, timeout_lim=timeout_lim, num_jobs=num_jobs)

    return ff_dict


def recursive_friends_count_bs4(parent_account, timeout_lim=60, parent_limit=5000, num_jobs=1):
    '''

    Inputs
    ------
    parent_account : str
        Twitter handle for parent node
    level : int
        Number of recursive levels to crawl
    parent_limit : int
        Maximum number of friends of parent account to track
    timeout_lim : int
        Number of seconds to wait for whotwi website to loads

    Returns
    -------
    ff_dict : dict
        Dictionary of get_friends dicts, one entry for each child accounts

    '''

    # Get friends from parent account
    parent_dict = get_friends_bs4(parent_account, friend_limit=parent_limit, timeout_lim=timeout_lim)
    # Parent friends
    parent_friends = parent_dict[parent_account]['friends']

    # Set metadata_requests
    metadata_requests = MetadataRequests().num_friends
    # Get friends count of child accounts
    if num_jobs == 1:
        ff_dict = get_metadata_bs4(parent_friends, timeout_lim=timeout_lim, metadata_requests=metadata_requests)
    else:
        ff_dict = get_metadata_bs4_pl(parent_friends, timeout_lim=timeout_lim, metadata_requests=metadata_requests, num_jobs=num_jobs)
    return ff_dict

def test():

    # test_account ='yfiyfiyfiyfi'
    # test_account = 'onlymans7'
    test_account = 'extrawomangirl'


    # Time test against get_friends_bs4 + get_metadata
    print('Starting method 1...')
    fof_start = time.time()
    fof_metadata = get_friends_of_friends_bs4(test_account)
    fof_stop = time.time()
    fof_tt = fof_stop - fof_start
    print(f"Total time for fof was: {fof_tt}")

    print('Starting method 2...')
    gfgm_start = time.time()
    print('getting friends....')
    gfgm_friends = get_friends_bs4(test_account, friend_limit=10000, timeout_lim=20)
    gfgm_friends = gfgm_friends[test_account]['friends']
    print('getting metadata....')
    # gfgm_metadata = get_metadata_bs4(gfgm_friends, timeout_lim=20, metadata_requests=MetadataRequests().friends_of_friends)
    gfgm_stop = time.time()
    gfgm_tt = gfgm_stop - gfgm_start
    print(f"Total time for gfgm was: {gfgm_tt}")

    return
def test_get_metadata():
    # test_account = 'extrawomangirl'
    test_account = 'onlymans7'
    result = get_metadata_bs4(test_account, metadata_requests=MetadataRequests().all_reqs)

    print(result)
    return

def test_get_friends_of_friends_bs4_page():
    proxy = gen_random_proxy()
    test_account = 'onlymans7'
    test_page = get_friends_of_friends_bs4_page(test_account, timeout_lim=10, proxy=proxy, page=1)
    print(test_page)

def test_get_fof():
    # test_account = 'NewspaperScrip1'
    test_account ='yfiyfiyfiyfi'
    # test_account = 'onlymans7'
    # test_account = 'onlymans7'
    # Time test against get_friends_bs4 + get_metadata
    print('Starting method 1...')
    fof_start = time.time()
    fof_metadata = get_friends_of_friends_bs4(test_account)
    fof_stop = time.time()
    fof_tt = fof_stop - fof_start
    print(fof_metadata)
    print(f"Total time for fof was: {fof_tt}")
    # for key in fof_metadata.keys():
    #     for key2 in fof_metadata[key].keys():
    #         print(fof_metadata[key][key2])
    #         print('---------------------------------------------')

    # print(fof_metadata.values())

def test_get_fof_pl(test_accounts=None):
    if test_accounts is None:
        test_accounts = ['onlymans7', 'extrawomangirl']
    print('Starting parallel test with proxies...')
    fof_start = time.time()
    fof_metadata = get_friends_of_friends_bs4_pl(test_accounts, num_jobs=2)
    print(fof_metadata)
    fof_stop = time.time()
    fof_tt = fof_stop - fof_start
    print(f"Total time for fof was: {fof_tt}")
    print(len(fof_metadata.keys()))

def test_get_metadata_bs4_driver():
    testing = get_metadata_bs4_driver('onlymans7', timeout_lim=10)
    print(testing)

def test_get_fof_parallel():
    # username = 'onlymans7'
    # friends_list = 'onlymans7'
    # friends_list = ["solsolsolsolso6", "yodges", "yfiyfiyfiyfi", "milesraymond12", "onlymans7", "extrawomangirl", "sbfsbfsbfsbfsbf"]
    # friends_list = ['onlymans7', '_aklil0', 'yfiyfiyfiyfi']
    friends_list = {'onlymans7': {'num_friends': 7}, '_aklil0': {'num_friends': 105}, 'yfiyfiyfiyfi': {'num_friends': 55}}
    start_time = time.time()
    test_fof_dict = get_fof_parallel(friends_list, friend_limit=5000, num_jobs=40)
    stop_time = time.time()
    total_time = stop_time - start_time
    # test_fof_list = get_fof_parallel(username)
    # print(test_fof_list[0])
    print(type(test_fof_dict))
    print(len(test_fof_dict))
    print(test_fof_dict.keys())
    num_tot_keys = 0
    for key in test_fof_dict.keys():
        num_sub_keys = len(test_fof_dict[key].keys())
        print(f"username {key} has {num_sub_keys} entries")
        num_tot_keys += num_sub_keys
    print(f"Succesfully scraped metadata from {num_tot_keys} accounts in {total_time} seconds")
    # flatten dict
    flattened_dict = {}
    for key in test_fof_dict.keys():
        flattened_dict.update(test_fof_dict[key])

    print(f"Flattened dict has length {len(flattened_dict)}")
    print(f"With {len(set(list(flattened_dict.keys())))} unique keys")

    # print(len(test_fof_list))
    

def test_return_fof_parallel_inputs():
    friends_list = ['onlymans7', '_aklil0', 'yfiyfiyfiyfi']
    # friends_list = 'onlymans7'
    fof_parallel_metadata = return_metadata_fof_parallel(friends_list, friend_limit=1000)
    print(fof_parallel_metadata)
    test_fof_parallel_inputs = return_inputs_fof_parallel(fof_parallel_metadata)
    print(test_fof_parallel_inputs)
    print(len(test_fof_parallel_inputs))
    return test_fof_parallel_inputs

def test_twitter_pl():
    # location of usernames we are tracking
    usernames_file = '/Users/mar2194/Downloads/bquxjob_429768b7_17bffacd9a4.csv'
    # list of tracked usernames
    tracked_usernames = return_tracked_usernames(usernames_file)
    start_time = time.time()
    # get twitter metadata in parallel
    twitter_metadata = return_metadata_twitter_user_pl(tracked_usernames, num_jobs=10)
    stop_time = time.time()
    total_time = stop_time - start_time
    print(f"Scraped metadata for {len(tracked_usernames)} accounts in {total_time} seconds")

    # save results
    np.save("twitter_metadata_parallel_test.npy", twitter_metadata)

    return

def test_twitter_serial():
# location of usernames we are tracking
    usernames_file = '/Users/mar2194/Downloads/bquxjob_429768b7_17bffacd9a4.csv'
    # list of tracked usernames
    tracked_usernames = return_tracked_usernames(usernames_file)[0:200]
    start_time = time.time()
    # get twitter metadata in parallel
    twitter_metadata = return_metadata_twitter(tracked_usernames)
    stop_time = time.time()
    total_time = stop_time - start_time
    print(f"Scraped metadata for {len(tracked_usernames)} accounts in {total_time} seconds")

    # save results
    np.save("twitter_metadata_serial_test.npy", twitter_metadata)

    return

    # return_metadata_twitter_user_chunks_pl
def test_twitter_pl_chunks():
    # location of usernames we are tracking
    usernames_file = '/Users/mar2194/Downloads/bquxjob_429768b7_17bffacd9a4.csv'
    # list of tracked usernames
    tracked_usernames = return_tracked_usernames(usernames_file)[0:200]
    start_time = time.time()
    # get twitter metadata in parallel
    twitter_metadata = return_metadata_twitter_user_chunks_pl(tracked_usernames, num_jobs=10)
    stop_time = time.time()
    total_time = stop_time - start_time
    print(f"Scraped metadata for {len(tracked_usernames)} accounts in {total_time} seconds")

    # save results
    np.save("twitter_metadata_parallel_chunks_test_all.npy", twitter_metadata)

    return   

if __name__ == '__main__':
    # test_get_fof()
    # test_get_metadata()
    # test_get_friends_of_friends_bs4_page()
    # test_get_fof_pl()
    # test_get_metadata_bs4_driver()
    # test_get_fof_parallel()
    # test_return_fof_parallel_inputs()
    # test_twitter_pl()
    test_twitter_pl_chunks()
    # test_twitter_pl()
    # test_twitter_serial()