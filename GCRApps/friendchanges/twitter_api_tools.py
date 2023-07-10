from external_funcs import MetadataDefaults, MetadataRequests
import re
import requests
import datetime
def build_tapi_url(usernames, metadata_requests):
    '''
    
    Inputs
    ------
    usernames : list
        List of usernames to lookup using V2 of the official Twitter API
    metadata_requests : dict
        Dictionary of metadata requested for each account.
    
    Returns
    -------
    full_tapi_url : str
        Full Twitter API V2 URLs

    '''
    usernames_url = build_tapi_users_url(usernames)
    metadata_url = build_tapi_user_metadata_url(metadata_requests)
    full_tapi_url = f"https://api.twitter.com/2/users/by?{usernames_url}&{metadata_url}"
    return full_tapi_url

def build_tapi_users_url(usernames):
    '''
    
    Inputs
    ------
    usernames : list
        List of usernames to lookup using V2 of the official Twitter API
        
    Returns
    -------
    usernames_url : str
        Sub-url for Twitter API V2
    
    '''
    
    if len(usernames) > 100:
        usernames = usernames[:100]
        print('Too many usernames in build_tapi_users_url...')
        
    usernames_url = "usernames="
    for username in usernames[:-1]:
        usernames_url = usernames_url + username + ","
    usernames_url = usernames_url + usernames[-1]
    
    return usernames_url

def build_tapi_user_metadata_url(metadata_requests):
    '''Build metadata request portion of Twitter V2 API URL
    
    Inputs
    ------
    metadata_requests : dict
        Dictionary of metadata requested for each account.
        
    Returns
    -------
    tapi_url : str
        Sub-url for Twitter V2 API
        
    '''
    
    tapi_url = _check_tapi_url('')
    tapi_url = _username_tapi_url(tapi_url, metadata_requests)
    tapi_url = _userid_tapi_url(tapi_url, metadata_requests)
    tapi_url = _list_name_tapi_url(tapi_url, metadata_requests)
    tapi_url = _num_friends_tapi_url(tapi_url, metadata_requests)
    tapi_url = _num_followers_tapi_url(tapi_url, metadata_requests)
    tapi_url = _num_tweets_tapi_url(tapi_url, metadata_requests)
    tapi_url = _creation_date_tapi_url(tapi_url, metadata_requests)
    tapi_url = _bio_tapi_url(tapi_url, metadata_requests)
    tapi_url = _location_tapi_url(tapi_url, metadata_requests)
    tapi_url = _website_tapi_url(tapi_url, metadata_requests)
    
    return tapi_url

def get_metadata_tapi(usernames, metadata_requests, bearer_token):
    '''
    
    Inputs
    ------
    usernames : list
        List of Twitter usernames. No more than 30,000 usernames/function call
        Or no more than 300 calls per 15 mins. 
        See: https://developer.twitter.com/en/docs/twitter-api/users/lookup/api-reference/get-users-by
    metadata_requests : dict
        Dictionary of metadata requested for each account.
    bearer_token : str
        Bearer Token for Twitter V2 API
        
    Returns
    -------
    tapi_metadatas : dict
        Dictionary of metadata organized like scrape_whotwi.get_friends_of_friends_bs4
    
    '''
    if metadata_requests is None:
        metadata_requests = MetadataRequests().all_reqs

    if len(usernames) > 30000:
        usernames = usernames[:30000]
    
    if len(usernames) > 100:
        lol_usernames = [usernames[i:i + 100] for i in range(0, len(usernames), 100)]
    else:
        lol_usernames = [usernames]
        
#     Throw everything into a single dictionary
    tapi_metadatas = {}
    for usernames_sublist in lol_usernames:
        tapi_metadata = get_metadata_tapi_driver(usernames_sublist, metadata_requests, bearer_token)
        tapi_metadatas.update(tapi_metadata)
        
    return tapi_metadatas


def get_metadata_tapi_driver(usernames, metadata_requests, bearer_token):
    '''
    
    Inputs
    ------
    usernames : list
        List of usernames to lookup using V2 of the official Twitter API
    metadata_requests : dict
        Dictionary of metadata requested for each account.
    bearer_token : str
        Bearer Token for Twitter V2 API
        
    Returns
    -------
    friends_metadata : dict
        Dictionary with username key, value is dict of metadata
    
    '''
    
    # Build URL for Twitter V2 API
    full_tapi_url = build_tapi_url(usernames, metadata_requests)
    # Build headers for request
    headers={"Authorization": f"Bearer {bearer_token}"}
    
    # Submit request
    tapi_response_raw = requests.get(full_tapi_url, 
                                 headers={"Authorization": f"Bearer {bearer_token}"})
    
    if tapi_response_raw.status_code != 200:
        raise Exception(
            f"There was a problem getting metadata from Twitter: {tapi_response_raw.status_code} {tapi_response_raw.text}")
    
    tapi_responses = tapi_response_raw.json()
    
    
    # Place data into dictionary in get_fof/get_friends_of_friends/get_metadata format
    tapi_metadata = {}
    for tapi_response in tapi_responses['data']:
        metadata = MetadataDefaults().default
        username = tapi_response['username']
        tapi_metadata[username] = return_metadata_tapi(tapi_response, metadata, metadata_requests)

    return tapi_metadata

def return_metadata_tapi(tapi_response, metadata, metadata_requests):
    '''Build dictionary for an individual user returned by Twitter API V2
    
    Inputs
    ------
    tapi_response : dict
        Dictionary of metadata returned by Twitter API V2
    metadata : dict
        Dictionary of metadata
    metadata_requests : dict
        Dictionary of metadata requested for each account.
        
    Returns
    -------
    tapi_url : str
        Sub-url for Twitter V2 API
        
    '''
    
    if metadata is None:
        metadata = MetadataDefaults().default
    
    metadata = _username_tapi_metadata(tapi_response, metadata, metadata_requests)
    metadata = _userid_tapi_metadata(tapi_response, metadata, metadata_requests)
    metadata = _list_name_tapi_metadata(tapi_response, metadata, metadata_requests)
    metadata = _num_friends_tapi_metadata(tapi_response, metadata, metadata_requests)
    metadata = _num_followers_tapi_metadata(tapi_response, metadata, metadata_requests)
    metadata = _num_tweets_tapi_metadata(tapi_response, metadata, metadata_requests)
    metadata = _creation_date_tapi_metadata(tapi_response, metadata, metadata_requests)
    metadata = _bio_tapi_metadata(tapi_response, metadata, metadata_requests)
    metadata = _location_tapi_metadata(tapi_response, metadata, metadata_requests)
    metadata = _website_tapi_metadata(tapi_response, metadata, metadata_requests)
    metadata = _twitter_url_tapi_metadata(tapi_response, metadata, metadata_requests)
    
    return metadata


# --------------------------- Twitter API V2 URL Building -------------------------------
def _check_tapi_url(tapi_url):
    if not tapi_url:
        tapi_url = "user.fields="
    elif not tapi_url.startswith("user.fields="):
        tapi_url = "user.fields=" + tapi_url
    return tapi_url

def _username_tapi_url(tapi_url, metadata_requests):
    tapi_url = _check_tapi_url(tapi_url) 
    
    if metadata_requests['username'] is True:
        if "username" not in re.split('[,=]', tapi_url):
            if tapi_url[-1] == '=' or tapi_url[-1] == ',':
                tapi_url = tapi_url + "username"
            else:
                tapi_url = tapi_url + ",username"
        
    return tapi_url

def _userid_tapi_url(tapi_url, metadata_requests):
    tapi_url = _check_tapi_url(tapi_url) 
    
    if metadata_requests['userid'] is True:
        if "id" not in re.split('[,=]', tapi_url):
            if tapi_url[-1] == '=' or tapi_url[-1] == ',':
                tapi_url = tapi_url + "id"
            else:
                tapi_url = tapi_url + ",id"
        
    return tapi_url

def _list_name_tapi_url(tapi_url, metadata_requests):
    tapi_url = _check_tapi_url(tapi_url) 
    
    if metadata_requests['list_name'] is True:
        if "name" not in re.split('[,=]', tapi_url):
            if tapi_url[-1] == '=' or tapi_url[-1] == ',':
                tapi_url = tapi_url + "name"
            else:
                tapi_url = tapi_url + ",name"
        
    return tapi_url

def _num_friends_tapi_url(tapi_url, metadata_requests):
    tapi_url = _check_tapi_url(tapi_url) 
        
    if metadata_requests['num_friends'] is True:
        if "public_metrics" not in re.split('[,=]', tapi_url):
            if tapi_url[-1] == '=' or tapi_url[-1] == ',':
                tapi_url = tapi_url + "public_metrics"
            else:
                tapi_url = tapi_url + ",public_metrics"
        
    return tapi_url

def _num_followers_tapi_url(tapi_url, metadata_requests):
    tapi_url = _check_tapi_url(tapi_url) 
        
    if metadata_requests['num_followers'] is True:
        if "public_metrics" not in re.split('[,=]', tapi_url):
            if tapi_url[-1] == '=' or tapi_url[-1] == ',':
                tapi_url = tapi_url + "public_metrics"
            else:
                tapi_url = tapi_url + ",public_metrics"
        
    return tapi_url

def _num_tweets_tapi_url(tapi_url, metadata_requests):
    tapi_url = _check_tapi_url(tapi_url) 

    if metadata_requests['num_tweets'] is True:
        if "public_metrics" not in re.split('[,=]', tapi_url):
            if tapi_url[-1] == '=' or tapi_url[-1] == ',':
                tapi_url = tapi_url + "public_metrics"
            else:
                tapi_url = tapi_url + ",public_metrics"
        
    return tapi_url

def _creation_date_tapi_url(tapi_url, metadata_requests):
    tapi_url = _check_tapi_url(tapi_url) 
        
    if metadata_requests['creation_date'] is True:
        if "created_at" not in re.split('[,=]', tapi_url):
            if tapi_url[-1] == '=' or tapi_url[-1] == ',':
                tapi_url = tapi_url + "created_at"
            else:
                tapi_url = tapi_url + ",created_at"
    return tapi_url

def _bio_tapi_url(tapi_url, metadata_requests):
    tapi_url = _check_tapi_url(tapi_url) 
        
    if metadata_requests['bio'] is True:
        if "description" not in re.split('[,=]', tapi_url):
            if tapi_url[-1] == '=' or tapi_url[-1] == ',':
                tapi_url = tapi_url + "description"
            else:
                tapi_url = tapi_url + ",description"
    return tapi_url

def _location_tapi_url(tapi_url, metadata_requests):
    tapi_url = _check_tapi_url(tapi_url) 
        
    if metadata_requests['location'] is True:
        if "location" not in re.split('[,=]', tapi_url):
            if tapi_url[-1] == '=' or tapi_url[-1] == ',':
                tapi_url = tapi_url + "location"
            else:
                tapi_url = tapi_url + ",location"
    return tapi_url

def _website_tapi_url(tapi_url, metadata_requests):
    tapi_url = _check_tapi_url(tapi_url) 
        
    if metadata_requests['website'] is True:
        if "url" not in re.split('[,=]', tapi_url):
            if tapi_url[-1] == '=' or tapi_url[-1] == ',':
                tapi_url = tapi_url + "url,entities"
            else:
                tapi_url = tapi_url + ",url,entities"
    return tapi_url
# ------------------------- End Twitter API V2 URL Building -----------------------------

# ------------------------ Twitter API V2 Metadata Validation ---------------------------
def _username_tapi_metadata(tapi_response, metadata, metadata_requests):
    
    if metadata_requests['username'] is True:
        if 'username' in tapi_response:
            metadata['username'] = tapi_response['username']
        else:
            metadata['errors'] += 1
    
    return metadata

def _userid_tapi_metadata(tapi_response, metadata, metadata_requests):
    
    if metadata_requests['userid'] is True:
        if 'id' in tapi_response:
            metadata['userid'] = tapi_response['id']
        else:
            metadata['errors'] += 1
        
    return metadata

def _list_name_tapi_metadata(tapi_response, metadata, metadata_requests):
    
    if metadata_requests['list_name'] is True:
        if 'name' in tapi_response:
            metadata['list_name'] = tapi_response['name']
        else:
            metadata['errors'] += 1
        
    return metadata

def _num_friends_tapi_metadata(tapi_response, metadata, metadata_requests):
        
    if metadata_requests['num_friends'] is True:
        if "public_metrics" in tapi_response:
            public_metrics = tapi_response['public_metrics']
            if 'following_count' in public_metrics:
                metadata['num_friends'] = public_metrics['following_count']
            else:
                metadata['errors'] += 1
        else:
            metadata['errors'] += 1
        
    return metadata

def _num_followers_tapi_metadata(tapi_response, metadata, metadata_requests):
        
    if metadata_requests['num_followers'] is True:
        if "public_metrics" in tapi_response:
            public_metrics = tapi_response['public_metrics']
            if 'followers_count' in public_metrics:
                metadata['num_followers'] = public_metrics['followers_count']
            else:
                metadata['errors'] += 1
        else:
            metadata['errors'] += 1
        
    return metadata

def _num_tweets_tapi_metadata(tapi_response, metadata, metadata_requests):

    if metadata_requests['num_tweets'] is True:
        if "public_metrics" in tapi_response:
            public_metrics = tapi_response['public_metrics']
            if 'tweet_count' in public_metrics:
                metadata['num_tweets'] = public_metrics['tweet_count']
            else:
                metadata['errors'] += 1
        else:
            metadata['errors'] += 1
        
    return metadata

def _creation_date_tapi_metadata(tapi_response, metadata, metadata_requests):
        
    if metadata_requests['creation_date'] is True:
        if "created_at" in tapi_response:
            try:
                creation_date_ = tapi_response['created_at'].replace('Z','')
                creation_date = datetime.datetime.fromisoformat(creation_date_)
                metadata['creation_date'] = creation_date
            except Exception as e:
                metadata['creation_date'] = tapi_response['created_at']
                print(f"Problem with _creation_date_tapi_metadata: {e}")
        else:
            metadata['errors'] += 1
            
    return metadata

def _bio_tapi_metadata(tapi_response, metadata, metadata_requests):
        
    if metadata_requests['bio'] is True:
        if "description" in tapi_response:
            metadata['bio'] = tapi_response['description']
        else:
            metadata['errors'] += 1
    return metadata

def _location_tapi_metadata(tapi_response, metadata, metadata_requests):
        
    if metadata_requests['location'] is True:
        if "location" in tapi_response:
            metadata['location'] = tapi_response['location']
        else:
            metadata['errors'] += 1
    return metadata

def _website_tapi_metadata(tapi_response, metadata, metadata_requests):
        
    if metadata_requests['website'] is True:
        if "url" in tapi_response:
            try:
                website = tapi_response['entities']['url']['urls'][0]['expanded_url']
            except:
                website = tapi_response['url']
                pass
            metadata['website'] = website
        else:
            metadata['errors'] += 1
    return metadata

def _twitter_url_tapi_metadata(tapi_response, metadata, metadata_requests):
        
    if metadata_requests['twitter_url'] is True:
        metadata['twitter_url'] = f"https://www.twitter.com/{tapi_response['username']}"
    return metadata
# ---------------------- End Twitter API V2 Metadata Validation -------------------------