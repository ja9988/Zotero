from bs4 import BeautifulSoup
import random
import time
import datetime
from dataclasses import dataclass, field
import requests
from dateutil.relativedelta import relativedelta
from typing import Dict
import re
import base64
import json
# from twitter_api_tools import get_metadata_tapi


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

def check_message():
    #TODO: check message type

    return None


def get_fof_bs4_page(username, page, timeout_lim=20, metadata_requests=None):
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
            whotwi_get = requests.get(whotwi_link, timeout=timeout_lim)
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

def check_twitter_metadata(twitter_metadata, list_of_parents):
    try:
        for username in twitter_metadata.keys():
            if twitter_metadata[username]['num_friends'] == 0:
                return list_of_parents
        return []
    except Exception as e:
        print(f"Exception in check_twitter_metadata: {e}")
        return list_of_parents

# def decode_data_dict(data_dict):
#     dict_str = base64.b64decode(data_dict).decode("utf-8").strip()
#     the_dict = json.loads(dict_str)
#     return the_dict

# def read_brick(inbound_brick):
#     '''Read and direct an incoming brick.
    
    
#     '''
#     brick_attributes = inbound_brick['message']['attributes']
#     brick_data = inbound_brick['message']['data']

#     brick_type = brick_attributes['brick_type']
#     brick_number = int(brick_attributes['brick_number'])
#     brick_batch_total = int(brick_attributes['brick_batch_total'])
#     brick_batch_uuid = brick_attributes['brick_batch_uuid']
#     brick_origin = brick_attributes['brick_origin']
    

#     if brick_type == 'grandparents':
#         print('Grandparents brick received. Run grandparents logic.')
#         decoded_grandparents = decode_data_dict(brick_data)
#         print(f'Grandparents received were: {decoded_grandparents}')
#         sstatus = process_grandparents(decoded_grandparents)
#         print(f"Grandparents processed: {sstatus}")
#         message = 'grandparents'
#     elif brick_type == 'parents':
#         print('Parents brick received. Run parents logic.')
#         decoded_parents = decode_data_dict(brick_data)
#         print(f'Parents received were: {decoded_parents}')
#         message = 'parents'
#     else:
#         print('Unknown brick type received. Returning 200.')
#         message = 'unknown'


#     return message

    


# def process_grandparents(decoded_grandparents):
#     # Metadata to request from Twitter API
#     metadata_requests = MetadataRequests().all_reqs
#     # Twitter API Token
#     bearer_token = os.environ.get('twitter_api_bearer_staging')
#     # Get Parent metadata using the Twitter API V2
#     twitter_metadata = get_metadata_tapi(decoded_grandparents, metadata_requests, bearer_token)
#     print(f"Twitter metadata of grandparent accounts has length: {len(twitter_metadata)}")

#     # Validate the Twitter API Metadata
#     twitter_metadata_check = check_twitter_metadata(twitter_metadata, decoded_grandparents)
#     if len(twitter_metadata_check) == 0:
#         print('Twitter metadata was healthy and will be used to speed up crawling')
#         print(twitter_metadata)
#         fof_parent_input = twitter_metadata
#     else:
#         print('Unfortunately Twitter couldnt be scraped, proceeding with whotwi...')
#         # fof_parent_input = decoded_grandparents
#         # send another brick out

#     # Generate brick messages for parent accounts
#     username_page_tuples = return_inputs_fof_parallel(fof_parent_input)
#     # Print out the tuples
#     print(f"username page tuples are: {username_page_tuples}")

#     return "Sucess"


# def return_inputs_fof_parallel(usernames_metadata):
#     '''Returns a list of tuples for use in parallel calls to get_fof_bs4_page

#     Inputs
#     ------
#     usernames_metadata : dict
#         Parent account usernames with friend limit as num_friends

#     Returns
#     -------
#     page_tuples : list
#         List of tuples where list[entry][0] is username and list[entry][1] is 
#         a whotwi page number
    
    
#     '''
#     # Gather metadata to create page counts, etc
#     # usernames_metadata = return_metadata_fof_parallel(usernames, friend_limit)
#     page_tuples = []
#     for username in usernames_metadata.keys():
#         num_friends = usernames_metadata[username]['num_friends']
#         num_pages = return_num_pages(num_friends)
#         if num_pages > 101:
#             num_pages = 101
#         for page in range(1, num_pages):
#             page_tuples.append((username, page))



#     return page_tuples


# def return_num_pages(num_friends):
#     ''' Function to return valid num_pages for range() calls when scraping fof

#     Inputs
#     ------
#     num_friends : int
#         Number of friends

#     Returns
#     -------
#     num_pages : int
#         Number of pages + 1 to retrieve given num_friends
    
#     '''
#     if num_friends%50 == 0:
#         adder = 1
#     else:
#         adder = 2
        
#     num_pages = num_friends//50 + adder
#     return num_pages