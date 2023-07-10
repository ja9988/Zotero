def usernames_str_check_master_temp(flattened_dict):
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


def char_inds_check_master_temp(flattened_dict):
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

def userids_str_check_master_temp(flattened_dict):
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


def check_master_userid(flattened_dict, client):
    userids_str = userids_str_check_master_temp(flattened_dict)
    sql = f"""
    SELECT username, userid
    FROM
    sneakyscraper.scrape_whotwi.master_clean
    WHERE
    userid in {userids_str}"""
    try:
        query_job = client.query(sql)
        records = [dict(row) for row in query_job]
    except Exception as e:
        records = []
        print(f"Error with check_master_userid: {e}")
        pass
    return records

def check_master_username(flattened_dict, client):
    usernames = usernames_str_check_master_temp(flattened_dict)
    numbers = char_inds_check_master_temp(flattened_dict)
    sql = f"""WITH PARTITIONED_USERNAMES AS(
    SELECT username, userid
    FROM sneakyscraper.scrape_whotwi.master_clean
    WHERE 
    username_char_ind in {numbers}),
    RANKED_MESSAGES AS( 
    SELECT * 
    FROM PARTITIONED_USERNAMES
    WHERE
    username IN {usernames})
    SELECT * FROM RANKED_MESSAGES;"""
    try:
        query_job = client.query(sql)
        records = [dict(row) for row in query_job]
    except Exception as e:
        records = []
        print(f"Error with check_master: {e}")
        pass
    return records

def check_master_temp_fncn(flattened_dict, client, by='userid'):
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
            records = check_master_userid(flattened_dict, client)
    elif by == 'username':
        if len(flattened_dict) > 0:
            records = check_master_username(flattened_dict, client)      
    elif by == 'both':
        if len(flattened_dict) > 0:
            records_userid = check_master_userid(flattened_dict, client)
            records_username = check_master_username(flattened_dict, client)
            records = records_userid + records_username
        
    return records

def new_accounts_userid(new_friends_flat, records_from_master):
    # Unique userids returned from latest scrape
    just_checked_list = list(set([new_friends_flat[key]['userid'] for key in new_friends_flat.keys()]))
    
    # Unique userids returned from master
    old_friends_list = list(set([x['userid'] for x in records_from_master]))
    
    # Userids of accounts that weren't in master
    new_friends_list = list(set(just_checked_list) - set(old_friends_list))
    
    # Userids of accounts that were already in master
    already_friends_list = list(set(old_friends_list) - set(new_friends_list))
    
    # Loop over new_friends_flat looking for userids that we already found
    # or are new
    new_friends_dict = {}
    already_friends_dict = {}
    for username, metadata in new_friends_flat.items():
        if metadata['userid'] in new_friends_list:
            new_friends_dict[username] = metadata
        elif metadata['userid'] in already_friends_list:
            already_friends_dict[username] = metadata
        
    return new_friends_dict, already_friends_dict

def new_accounts_username(new_friends_flat, records_from_master):
    # Unique usernames returned from latest scrape
    just_checked_list = list(set([new_friends_flat[key]['username'] for key in new_friends_flat.keys()]))
    
    # Unique usernames returned from master
    old_friends_list = list(set([x['username'] for x in records_from_master]))
    
    # Usernames of accounts that weren't in master
    new_friends_list = list(set(just_checked_list) - set(old_friends_list))
    
    # Userids of accounts that were already in master
    already_friends_list = list(set(old_friends_list) - set(new_friends_list))
    
    # Loop over new_friends_flat looking for usernames that we already found
    # or are new
    new_friends_dict = {}
    already_friends_dict = {}
    for username, metadata in new_friends_flat.items():
        if metadata['username'] in new_friends_list:
            new_friends_dict[username] = metadata
        elif metadata['username'] in already_friends_list:
            already_friends_dict[username] = metadata
        
    return new_friends_dict, already_friends_dict

def new_accounts_temp(new_friends_flat, records_from_master, by='userid'):
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
        new_friends_dict, already_friends_dict = new_accounts_userid(new_friends_flat, records_from_master)
        return new_friends_dict, already_friends_dict            
    elif by == 'username':
        new_friends_dict, already_friends_dict = new_accounts_username(new_friends_flat, records_from_master)
        return new_friends_dict, already_friends_dict        
    elif by == 'both':
        new_friends_dict = {}
        already_friends_dict = {}
        new_friends_dict_id, already_friends_dict_id = new_accounts_userid(new_friends_flat, records_from_master)
        new_friends_dict_un, already_friends_dict_un = new_accounts_username(new_friends_flat, records_from_master)
        new_friends_usernames = list(set(list(new_friends_dict_id.keys())).intersection(list(new_friends_dict_un.keys())))
        already_friends_usernames = list(set(list(already_friends_dict_id.keys()) + list(already_friends_dict_un.keys())))
        
        for username in new_friends_usernames:
            if username in new_friends_dict_id.keys():
                new_friends_dict[username] = new_friends_dict_id[username]
            elif username in new_friends_dict_un.keys():
                new_friends_dict[username] = new_friends_dict_un[username]
        
        for username in already_friends_usernames:
            if username in already_friends_dict_id.keys():
                already_friends_dict[username] = already_friends_dict_id[username]
            elif username in already_friends_dict_un.keys():
                already_friends_dict[username] = already_friends_dict_un[username] 
                
        
        return new_friends_dict, already_friends_dict

def get_new_accounts_temp(child_batch_dict, client, by='both'):
    if by == 'userid':
        records_from_master = check_master_temp_fncn(child_batch_dict, client, by='userid')
        # records_from_master = check_master(child_batch_dict, client, by='userid')
        new_friends_dict, already_friends_dict = new_accounts_temp(child_batch_dict, records_from_master, by='userid')
    elif by == 'username':
        records_from_master = check_master_temp_fncn(child_batch_dict, client, by='username')
        new_friends_dict, already_friends_dict = new_accounts_temp(child_batch_dict, records_from_master, by='username')
    elif by == 'both':
        records_from_master = check_master_temp_fncn(child_batch_dict, client, by='both')
        new_friends_dict, already_friends_dict = new_accounts_temp(child_batch_dict, records_from_master, by='both')

    
    return new_friends_dict, already_friends_dict