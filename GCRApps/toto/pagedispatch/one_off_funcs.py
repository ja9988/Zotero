# from process_message import stream_to_bq 
import datetime

def stream_to_bq2(some_dict, table_id, client):
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


def return_num_pages_one_off(num_friends):
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
    
    if num_pages > 100:
        num_pages = 100
    
    return num_pages

def get_parent_network_batch(client):

    sql = """
    WITH latest_n_friends AS(SELECT username AS parent_account, num_friends, num_followers
    FROM
    sneakyscraper.scrape_whotwi.last_n_friends_copy
    QUALIFY ROW_NUMBER() OVER(PARTITION BY userid ORDER BY last_checked DESC) = 1),

    rows_in_master_clean_ AS(
    (SELECT parent_account, username FROM sneakyscraper.scrape_whotwi.master_clean)
    UNION ALL
    (SELECT parent_account, username FROM sneakyscraper.scrape_whotwi.master_friends
    WHERE parent_account IN (SELECT parent_account FROM sneakyscraper.scrape_whotwi.master_clean))
    UNION ALL
    (SELECT parent_account, username FROM sneakyscraper.scrape_whotwi.master_friends_expanded
    WHERE parent_account IN (SELECT parent_account FROM sneakyscraper.scrape_whotwi.master_clean))),

    rows_in_master_clean AS(
    SELECT LOWER(parent_account) AS parent_account, COUNT(DISTINCT(username)) AS num_friends_recorded
    FROM
    rows_in_master_clean_
    GROUP BY LOWER(parent_account)),

    combined_records AS(
    SELECT lnf.*, rimc.num_friends_recorded
    FROM
    latest_n_friends AS lnf
    JOIN
    rows_in_master_clean AS rimc
    ON
    lnf.parent_account = rimc.parent_account),

    accounts_to_grab AS(SELECT *, 
    IF(num_friends<=5000,(num_friends - num_friends_recorded),(5000-num_friends_recorded)) AS first_n_friends_to_grab
    FROM
    combined_records
    WHERE 
    (num_friends_recorded <= 5000
    AND
    (num_friends - num_friends_recorded) > 0)
    ORDER BY
    num_friends_recorded DESC)


    SELECT * FROM accounts_to_grab
    WHERE
    first_n_friends_to_grab >= 50
    AND
    parent_account NOT IN (SELECT DISTINCT(parent_account) FROM sneakyscraper.scrape_whotwi.graph_expanded)
    AND
    parent_account NOT IN (SELECT parent_account FROM sneakyscraper.scrape_whotwi.one_off_attempts)
    QUALIFY SUM(first_n_friends_to_grab) OVER(ORDER BY first_n_friends_to_grab DESC) <= 60000
    ORDER BY first_n_friends_to_grab DESC"""
    
    query_job = client.query(sql)
    results = [dict(row) for row in query_job]
    return results

def page_tuples_oneoff(parent_metadata):
    max_pages = return_num_pages_one_off(parent_metadata['num_friends'])
    num_pages_to_grab = return_num_pages_one_off(parent_metadata['first_n_friends_to_grab'])
    start_page = max_pages-num_pages_to_grab
    if start_page <=0:
        start_page = 1
    pages_to_get = list(range(start_page,max_pages))
    page_tuples = []
    for page in pages_to_get:
        page_tuples.append({'username': parent_metadata['parent_account'], 'page': page, 'num_friends_per_page': 50, 'one_off': 'True'})
    return page_tuples


def return_one_off_bricks(client):
    attempts_table = "sneakyscraper.scrape_whotwi.one_off_attempts"
    one_off_bricks = []
    parents_metadata = get_parent_network_batch(client)
    parent_accounts_ = [{'parent_account': x['parent_account']} for x in parents_metadata]
    for parent_account_ in parent_accounts_:
        stream_parent_attempts = stream_to_bq2(parent_account_,attempts_table,client)

    for parent_metadata in parents_metadata:
        parent_brick = page_tuples_oneoff(parent_metadata)
        one_off_bricks = one_off_bricks + parent_brick

    return one_off_bricks
