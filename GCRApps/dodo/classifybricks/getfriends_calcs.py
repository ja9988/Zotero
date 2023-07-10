import datetime
from google.api_core.datetime_helpers import DatetimeWithNanoseconds

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



def return_inputs_get_friends_parallel(metadata_dict, start_page=2, max_friends=500):
    '''Returns a list of tuples for use in parallel calls to get_fof_bs4_page

    Inputs
    ------
    metadata_dict : dict
        Parent account usernames with friend limit as num_friends

    Returns
    -------
    pages : list
        List of dictionaries where dict['username] is username and dict['page'] is 
        a whotwi page number
    
    
    '''

    # Metadata dict input example:
    # {'num_friends': 7, 'num_followers': 2, 'num_tweets': 0, 
    # 'num_crush': 0, 'num_crushed_on': 0, 
    # 'creation_date': '2022-05-27T15:10:18+00:00', 
    # 'twitter_url': 'https://www.twitter.com/ArtBlocksEngine', 
    # 'bio': 'Fueling the evolution of creativity with generative minting \
    #     technology by @artblocks_io', 
    #     'location': 'Marfa, TX', 'website': 'artblocks.io', 
    #     'list_name': 'Art Blocks Engine', 'errors': 1, 
    #     'max_errors': 12, 'username': 'ArtBlocksEngine', 
    #     'userid': 1530204712309604353, 
    #     'last_tweet': '2000-01-01T00:00:00+00:00', 
    #     'last_checked': '2022-08-02T04:02:53.654004+00:00', 
    #     'parent_account': 'artblocks_io', 
    #     'username_char_ind': 1, 
    #     'brick_batch_uuid': 'ae4e0804-ff64-4b03-88ac-c226b9974957', 
    #     'account_type_str': 'NFT Project'}
    # Gather metadata to create page counts, etc
    # usernames_metadata = return_metadata_fof_parallel(usernames, friend_limit)

# for key in new_old_metadata.keys():
#                     if type(new_old_metadata[key]) is datetime.datetime:
#                         new_old_metadata[key] = new_old_metadata[key].isoformat()
#                     elif type(new_old_metadata[key]) is DatetimeWithNanoseconds:
#                         new_old_metadata[key] = new_old_metadata[key].isoformat()

    for key in metadata_dict.keys():
        if type(metadata_dict[key]) is datetime.datetime:
            metadata_dict[key] = metadata_dict[key].isoformat()
        elif type(metadata_dict[key]) is DatetimeWithNanoseconds:
            metadata_dict[key] = metadata_dict[key].isoformat()

    all_pages_temp = []
    username = metadata_dict['username']
        # pages_temp = []
    num_friends = metadata_dict['num_friends']
    num_pages = return_num_pages(num_friends)
    if num_pages > 101:
        num_pages = 101
    total_friends = 0
    
    for page in range(start_page, num_pages, 1):
        num_friends_per_page = friends_per_page(page, num_friends)
        total_friends = total_friends + num_friends_per_page
        
        if total_friends < max_friends:

            temp_dict = {'username': username, 'page': page, 'num_pages': num_pages-1, 'num_friends_per_page': num_friends_per_page, 'metadata_dict': metadata_dict}
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