from google.cloud import bigquery
import datetime
import requests
# functions to be used by the routes

# retrieve all the names from the dataset and put them into a list
def get_names(source):
    names = []
    for row in source:
        # lowercase all the names for better searching
        name = row["name"].lower()
        names.append(name)
    return sorted(names)

# find the row that matches the id in the URL, retrieve name and photo
def get_actor(source, id):
    for row in source:
        if id == str( row["id"] ):
            name = row["name"]
            photo = row["photo"]
            # change number to string
            id = str(id)
            # return these if id is valid
            return id, name, photo
    # return these if id is not valid - not a great solution, but simple
    return "Unknown", "Unknown", ""

# find the row that matches the name in the form and retrieve matching id
def get_id(source, name):
    for row in source:
        # lower() makes the string all lowercase
        if name.lower() == row["name"].lower():
            id = row["id"]
            # change number to string
            id = str(id)
            # return id if name is valid
            return id
    # return these if id is not valid - not a great solution, but simple
    return "Unknown"

# retrieve all signed up users
def check_signups(twitter_username, telegram_username):
    sql = f"""
    SELECT
    *
    FROM
    sneakyscraper.oz_dao.waitlist
    WHERE
    twitter_username = "{twitter_username}"
    OR
    telegram_username = "{telegram_username}"
    
    """
    project_id = 'sneakyscraper'
    client = bigquery.Client(project=project_id)

    query_job = client.query(sql)
    sql_list = [dict(row) for row in query_job]
    
    
    return sql_list

def check_signup(twitter_username, telegram_username):
    records = check_signups(twitter_username, telegram_username)

    if len(records) == 0:
        new_user = True
    else:
        new_user = False

    return new_user

def store_user(user_signup):
    record_to_write = [user_signup]
    project_id = 'sneakyscraper'
    client = bigquery.Client(project=project_id)
    errors = client.insert_rows_json('sneakyscraper.oz_dao.waitlist', record_to_write)

    if len(errors) == 0:
        success = True
    else:
        success = False

    return success

def store_ip_address(user_signup):
    record_to_write = [user_signup]
    project_id = 'sneakyscraper'
    client = bigquery.Client(project=project_id)
    errors = client.insert_rows_json('sneakyscraper.oz_dao.ip_addresses', record_to_write)

    if len(errors) == 0:
        success = True
    else:
        success = False

    return success

def get_user(slipper):
    sql = f"""
    WITH users_records AS (
    SELECT
    twitter_username, telegram_username, sent_tg_id, slipper, position, status, update_time
    FROM
    sneakyscraper.oz_dao.waitlist
    WHERE
    LOWER(slipper) = "{slipper.lower().strip()}"),
    ordered_messages AS (
    SELECT m.*, ROW_NUMBER() OVER (PARTITION BY slipper ORDER BY update_time DESC) AS rn
    FROM users_records AS m
    )
    SELECT * EXCEPT (rn) FROM ordered_messages WHERE rn = 1
    """
    project_id = 'sneakyscraper'
    client = bigquery.Client(project=project_id)

    query_job = client.query(sql)
    user_data = [dict(row) for row in query_job]
    
    
    return user_data

def check_twitter_username(twitter_username):
    username_url = f"https://twitter.com/{twitter_username}"
    headers = {'User-Agent': 
           'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:70.0) Gecko/20100101 Firefox/70.0 PyWebCopyBot/6.3.0',
           'Accept':'*/*',
           'Accept-Language': 'en-US,en;q=0.9'}

    username_response = requests.get(username_url, headers=headers)
    username_response_code = username_response.status_code

    if username_response_code == 200:
        valid_username = True
    else:
        valid_username = False

    return valid_username