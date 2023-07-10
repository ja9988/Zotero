import uuid
from google.cloud import bigquery

def query_to_table(sql, client, table=None, write_disposition=None, create_disposition=None, range_partition=None):
    
    if table is None:
        result = uuid.uuid4()
        table = "sneakyscraper.scrape_whotwi." + result.hex
    elif table.count(".") == 2 and table[0] != "." and table[-1] != ".":
        table = table
    elif table.count(".") == 1 and table[0] != "." and table[-1] != ".":
        if table.split(".")[0] == "sneakyscraper":
            result = uuid.uuid4()
            table = table + result.hex
        elif table.split(".")[0] == "scrape_whotwi":
            table = "sneakyscraper." + table
    elif table.count(".") == 0:
        table = "sneakyscraper.scrape_whotwi." + table
        
    
    if create_disposition is None:
        create_disposition = "CREATE_IF_NEEDED"
    elif create_disposition == "needed":
        create_disposition = "CREATE_IF_NEEDED"
    elif create_disposition == "never":
        create_disposition = "CREATE_NEVER"
    elif len(create_disposition) == 0:
        create_disposition = "CREATE_IF_NEEDED"
    
        
    
    if write_disposition is None:
        write_disposition = "WRITE_APPEND"
    elif write_disposition == "overwrite":
        write_disposition = "WRITE_TRUNCATE"
    elif write_disposition == "append":
        write_disposition = "WRITE_APPEND"
    elif len(write_disposition) == 0:
        write_disposition = "WRITE_APPEND"
        
    if range_partition is not None:
        print(f"range_partition dict: {range_partition}")
        partition_column = range_partition['column']
        start_range = range_partition['start']
        stop_range = range_partition['stop']
        interval = range_partition['interval']
        range_partition_ = bigquery.RangePartitioning(
        field=partition_column,
        range_=bigquery.PartitionRange(start=start_range, end=stop_range, interval=interval)
        )
    else:
        range_partition_ = None
    
    print(f"range_partition_ object: {range_partition_}")
        
    job_config = bigquery.QueryJobConfig(write_disposition=write_disposition,
                                    destination=table,
                                    create_disposition=create_disposition,
                                        range_partitioning=range_partition_)
    
    query = sql
    errors = []
    try:
        query_job = client.query(query, job_config=job_config)
        query_job.result()
        
    except Exception as e:
        errors.append(f"{e}")
    
    return {f"table": table, "errors": errors}


def remake_all_records_by_userid(client):
    query = """WITH 
    master_graph AS(

    (SELECT username, list_name, bio, userid, last_checked, num_followers, num_friends, 
    account_type_str, creation_date
    FROM
    sneakyscraper.scrape_whotwi.master_clean
    WHERE userid != 0)
    UNION ALL
    (SELECT username, list_name, bio, userid, last_checked, num_followers, num_friends,
    '' AS account_type_str, creation_date
    FROM
    sneakyscraper.scrape_whotwi.graph)
    UNION ALL
    (SELECT username, list_name, bio, userid, last_checked, num_followers, num_friends,
    '' AS account_type_str, creation_date
    FROM
    sneakyscraper.scrape_whotwi.master_friends)
    UNION ALL
    (SELECT username, list_name, bio, userid, last_checked, num_followers, num_friends,
    '' AS account_type_str, creation_date
    FROM
    sneakyscraper.scrape_whotwi.master_followers)
    UNION ALL
    (SELECT username, list_name, bio, userid, last_checked, num_followers, num_friends,
    '' AS account_type_str, creation_date
    FROM
    sneakyscraper.scrape_whotwi.master_friends_expanded)),

    master_graph_with_int AS(
    SELECT *, CAST(SUBSTR(CAST(userid AS STRING), -3) AS INTEGER) AS end_userid_digits
    FROM
    master_graph)

    SELECT * FROM master_graph_with_int"""

    range_partion_ex = {'column': 'end_userid_digits', 'start': 0, 'stop': 999, 'interval': 1}

    table_info = query_to_table(query, client, table='sneakyscraper.scrape_whotwi.all_records_by_userid', write_disposition='WRITE_TRUNCATE', range_partition=range_partion_ex)

    return table_info


# def remake_all_records_by_userid(client):
#     query = """WITH 
#     master_graph AS(

#     (SELECT username, list_name, bio, userid, last_checked, num_followers, num_friends
#     FROM
#     sneakyscraper.scrape_whotwi.master_clean
#     WHERE userid != 0)
#     UNION ALL
#     (SELECT username, list_name, bio, userid, last_checked, num_followers, num_friends
#     FROM
#     sneakyscraper.scrape_whotwi.graph)
#     UNION ALL
#     (SELECT username, list_name, bio, userid, last_checked, num_followers, num_friends
#     FROM
#     sneakyscraper.scrape_whotwi.master_friends)
#     UNION ALL
#     (SELECT username, list_name, bio, userid, last_checked, num_followers, num_friends
#     FROM
#     sneakyscraper.scrape_whotwi.master_followers)
#     UNION ALL
#     (SELECT username, list_name, bio, userid, last_checked, num_followers, num_friends
#     FROM
#     sneakyscraper.scrape_whotwi.master_friends_expanded)),

#     master_graph_with_int AS(
#     SELECT *, CAST(SUBSTR(CAST(userid AS STRING), -3) AS INTEGER) AS end_userid_digits
#     FROM
#     master_graph)

#     SELECT * FROM master_graph_with_int"""

#     range_partion_ex = {'column': 'end_userid_digits', 'start': 0, 'stop': 999, 'interval': 1}

#     table_info = query_to_table(query, client, table='sneakyscraper.scrape_whotwi.all_records_by_userid', write_disposition='WRITE_TRUNCATE', range_partition=range_partion_ex)

#     return table_info



def remake_username_userids_table(client):
    query_for_username_by_character = """
    WITH master_graph AS(

    (SELECT username, userid, last_checked
    FROM
    sneakyscraper.scrape_whotwi.master_clean
    WHERE userid != 0)
    UNION ALL
    (SELECT username, userid, last_checked
    FROM
    sneakyscraper.scrape_whotwi.graph)
    UNION ALL
    (SELECT username, userid, last_checked
    FROM
    sneakyscraper.scrape_whotwi.master_friends)
    UNION ALL
    (SELECT username, userid, last_checked
    FROM
    sneakyscraper.scrape_whotwi.master_followers)
    UNION ALL
    (SELECT username, userid, last_checked
    FROM
    sneakyscraper.scrape_whotwi.master_friends_expanded)),

    master_graph_latest_userid_ AS(
    SELECT *, ROW_NUMBER() OVER(PARTITION BY LOWER(username) ORDER BY last_checked DESC) AS rn
    FROM master_graph),

    master_graph_latest_userid AS(
    SELECT * EXCEPT(rn)
    FROM
    master_graph_latest_userid_
    WHERE
    rn = 1),


    master_graph_with_ascii_ind AS(
    SELECT *, ASCII(UPPER(username)) AS username_ascii_ind
    FROM
    master_graph_latest_userid)

    SELECT * FROM master_graph_with_ascii_ind

    """
    range_partion_ex_2 = {'column': 'username_ascii_ind', 'start': 48, 'stop': 95, 'interval': 1}

    table_info_2 = query_to_table(query_for_username_by_character, client, table='sneakyscraper.scrape_whotwi.username_userid_lookup', write_disposition='WRITE_TRUNCATE', range_partition=range_partion_ex_2)

    return table_info_2


