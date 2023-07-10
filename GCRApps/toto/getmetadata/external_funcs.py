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
            metadata = _username_char_ind(metadata)
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

def letter_to_num(input_str):
    numbers = [ord(character.lower()) - 96 for character in input_str]
    numbers = [x if 1 <= x <= 26 else 0 for x in numbers]
    if len(numbers) == 0:
        numbers = [0]
    return numbers

def _username_char_ind(metadata):
    username = metadata['username']
    char_ind = letter_to_num(username)[0]
    metadata['char_ind'] = char_ind

    return(metadata)