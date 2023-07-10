import os
import requests
from flask import Flask, request
from external_funcs import *
from google.cloud import bigquery
from twitter_api_tools import get_metadata_tapi
from send_brick import dispatch_bricks

app = Flask(__name__)

@app.route("/check_friend_changes/", methods=["POST"])
def check_friend_changes():
    process_limit = 20
    time_now = datetime.datetime.now()
    ip = requests.get('https://api.ipify.org').text
    print(f"Time: {time_now.isoformat()}")
    minute_now = time_now.minute
    second_now = time_now.second
    print(f'minute_now: {minute_now}, second_now: {second_now}')
    # Enforce scheduling to avoid duplicate jobs sent from cloud scheduler
    if minute_now in [0, 10, 20, 30, 40, 50] and second_now < 20:
    # if minute_now:
        print(f'Valid request sent at {time_now.isoformat()}, we are visible on {ip}')
        # Get list of usernames to run fof on from cloud scheduler job
        print('Getting JSON from Cloud Scheduler...')
        parent_accounts = request.get_json(force=True)
        print(parent_accounts)
        # Create BigQuery Client
        print('Creating BigQuery Client...')
        project_id = 'sneakyscraper'
        client = bigquery.Client(project=project_id)
        # Get last friends from BQ
        print('Getting last n friends from BigQuery...')
        last_n_friends_bq_dict = get_last_n_friends_bq_dict(client)
        print(f"last_n_friends_bq_dict is length: {len(last_n_friends_bq_dict)}")
        # last_n_friends_list = list(last_n_friends_bq_dict.keys())
        # print(f"last_n_friends_list: {last_n_friends_list}")

        
        # --------------------Twitter API V2 Parents Metadata --------------------
        # Get metadata (num_friends) on parent accounts to speed up crawling
        print('Getting num_friends for parents from Twitter using API V2')
        # MetadataRequests for Twitter API V2
        metadata_requests = MetadataRequests().all_reqs
        # Twitter API Token
        bearer_token = os.environ.get('twitter_api_bearer_development')
        # Get Parent metadata using the Twitter API V2
        twitter_metadata = get_metadata_tapi(parent_accounts, metadata_requests, bearer_token)
        print(f"Twitter metadata of grandparent accounts has length: {len(twitter_metadata)}")
        # Validate the Twitter API Metadata
        twitter_metadata_check = check_twitter_metadata(twitter_metadata, parent_accounts)
        if len(twitter_metadata_check) == 0:
            print('Twitter metadata was healthy and will be used to speed up crawling')
            fof_parent_input = twitter_metadata
        else:
            print('Unfortunately Twitter couldnt be scraped, proceeding with whotwi...')
            fof_parent_input = parent_accounts
        # ------------------ End Twitter API V2 Parents Metadata ------------------


        # ---------------- Scrape Whotwi for Parents FOF Metadata -----------------
        # Scrape parent accounts in parallel
        print('Beggining scraping parents...')
        # parent_fof is friends of friends of parent accounts
        parent_fof = get_fof_parallel(fof_parent_input, num_jobs=process_limit)
        # Flatten the dictionary so that each key is a child of parent username
        # print('Flattening dictionary...')
        last_n_friends_dict = flatten_dict(parent_fof)
        # print(f"Finished flattening last_n_friends_dict")
        # ---- Testing ----
        # Convert the dict of parent accounts metadata from BigQuery to a list for use with
        # Twitter API V2
        last_n_friends_bq_list = list(last_n_friends_bq_dict.keys())
        print("Starting Twitter API Request (Parents)...")
        # Get metadata of children of parents using Twitter API V2
        last_n_friends_dict_twitter = get_metadata_tapi(last_n_friends_bq_list, metadata_requests, bearer_token)
        print("Finished Twitter API Request (Parents)")
        print(f"last_n_friends Twitter of parent accounts has length: {len(last_n_friends_dict_twitter)}")
        # -- End Testing ---
        # ---- Checking Changes in Children of Parents vs Whotwi/Twitter API --
        print('Checking changes in last_n_friends (BQ V Whotwi)...')
        # changes_to_ship is a dictionary with num_friends = change in num_friends from Whotwi Data
        changes_to_ship = detect_changes_dict(last_n_friends_dict, last_n_friends_bq_dict)
        print(changes_to_ship)
        print(f'There were {len(changes_to_ship)} changes detected when using whotwi...')
        print('Checking changes in last_n_friends (BQ V Twitter)...')
        # changes_to_ship_tapi is a dictionary with num_friends = change in num_friends from Twitter API Data
        changes_to_ship_tapi = detect_changes_dict(last_n_friends_dict_twitter, last_n_friends_bq_dict)
        print(changes_to_ship_tapi)
        print(f'There were {len(changes_to_ship_tapi)} changes detected when using the Twitter API...')
        # Merge num_friends from Whotwi and Twitter API to put on BigQuery
        print('Attempting to log whotwi vs twitter num_friends')
        try:
            table_id_tapi_whotwi = 'sneakyscraper.scrape_whotwi.tapi_whotwi_compared'
            tapi_whotwi_compared = compare_tapi_whotwi_bq(last_n_friends_dict_twitter, last_n_friends_dict, last_n_friends_bq_dict)
            num_batches_tapi_whotwi = len(tapi_whotwi_compared)
            if num_batches_tapi_whotwi > 20:
                num_batches_tapi_whotwi = 20
            stream_to_tapi_whotwi = batch_stream_bigquery(tapi_whotwi_compared, client, table_id_tapi_whotwi, num_batches=num_batches_tapi_whotwi)
            print(f"Response from tapi-whotwi comparison: {stream_to_tapi_whotwi}")
        except Exception as e:
            print(f"Problem with writing tapi-whotwi comparison to BQ: {e}")
            pass
        # -- End Checking Changes in Children of Parents vs Whotwi/Twitter API -
        # ------------- End Scrape Whotwi for Parents FOF Metadata ----------------

        # ---------- Get metadata of new friends added by child accounts ----------
        print(f"Scraping metadata of new accounts... from changes_to_ship with length: {len(changes_to_ship)}")
        # Were there any friendship changes detected in child accounts?
        max_changes_to_ship = 600
        if len(changes_to_ship) > max_changes_to_ship:
            print('Cropping changes to ship!')
            changes_to_ship = {k: changes_to_ship[k] for k in list(changes_to_ship)[:max_changes_to_ship]}

        if len(changes_to_ship) > 0:
            # Set num_jobs for get_fof_parallel to get metadata of new friends of child accounts
            if len(changes_to_ship) < process_limit:
                num_jobs = len(changes_to_ship)
            else:
                num_jobs = process_limit
            # Get metadata of new friends of child accounts
            print(f'num_jobs for finding new_friends is {num_jobs}')
            new_friends = get_fof_parallel(changes_to_ship, num_jobs=num_jobs)
            # Flatten dictionary of new_friends
            new_friends_flat = flatten_dict(new_friends)
            # Get metadata from Twitter API and merge with accounts_classified
            print('Attempting to get missing metadata for new accounts from Twitter API')
            try:
                new_friends_tapi = get_metadata_tapi(list(new_friends_flat.keys()), MetadataRequests().all_reqs, bearer_token)
                new_friends_flat = merge_tapi_whotwi_dicts(new_friends_tapi, new_friends_flat)
            except Exception as e:
                print(f"Problem with getting missing metadata from Twitter {e}")
                pass

            # new_friends_flat = {}
            # for key in new_friends.keys():
            #     new_friends_flat.update(new_friends[key])
            print(f"New friends flat dict has length {len(new_friends_flat)} with {len(set(list(new_friends_flat.keys())))} unique keys: {new_friends_flat.keys()}")
            # print(f"With {len(set(list(flattened_dict.keys())))} unique keys")
            print("Checking master...")
            # ----------- Check sneakyscraper.scrape_whotwi.master -----------
            records_from_master = check_master(new_friends_flat, client)
            print('Records from check_master:', records_from_master, 'Now finding new accounts:')
            # Get actual new accounts that aren't in master
            accounts_for_master, already_friends_dict = new_accounts(new_friends_flat, records_from_master)
            
            # Write previously seen accounts to graph table
            try:
                for_graph_dict = add_char_ind(already_friends_dict)
                num_batches_g = len(for_graph_dict)
                if num_batches_g > 20:
                    num_batches_g = 20

                table_id_graph = "sneakyscraper.scrape_whotwi.graph"
                stream_to_graph = batch_stream_bigquery(for_graph_dict, client, table_id_graph, num_batches=num_batches_g) 
            except Exception as e:
                print(f'Problem writing to graph {e}')
            
            print(f"Actual new accounts: {accounts_for_master.keys()}, now classifying...")
            print(f"Previously found accounts: {already_friends_dict.keys()}")

            # -------------------------- TEMP --------------------------
            print("-----------____________SCHEMAS____________-----------")
            try:
                previous_acct_temp = list(already_friends_dict.keys())[0]
                previous_acct_schema = already_friends_dict[previous_acct_temp].keys()
                print(f"Previous accounts dict schema: {previous_acct_schema}")
            except:
                pass

            try:
                for_master_temp = list(accounts_for_master.keys())[0]
                for_master_schema = accounts_for_master[for_master_temp].keys()
                print(f"For master accounts dict schema: {for_master_schema}")
            except:
                pass
            # -------------------------- TEMP --------------------------


            # ---------------- New Account Classification Old Way ----------------
            try:
                num_jobs = len(accounts_for_master)
                if num_jobs > 19:
                    num_jobs = 19
                if num_jobs > 0:
                    classified_accounts = check_accounts_pl(accounts_for_master, 'names_db.txt', num_jobs=num_jobs)
                    accounts_classified = classified_accounts['get_metadata_dict']
                    print(f"Classified accounts keys: {accounts_classified.keys()}")
                    print(f"Classified accounts: {accounts_classified}")
                else:
                    accounts_classified = {}
                    pass
            except Exception as e:
                accounts_classified = {}
                print(f'Problem with classifying accounts... {e}')
                pass
            # -------------- End New Account Classification Old Way --------------

            # ------------------- Update Master and TG Alerts --------------------
            # If there were any accounts classified (in case of error)
            if len(accounts_classified) > 0:
                # Add character index for master table partitioning
                for_master_classified_dict = add_char_ind(accounts_classified)

                # tg_alert_accts passed filter, overflow_accts didn't
                print('Splitting classified accounts...')
                tg_alert_accts, overflow_accts = filt_accounts_by_metadata(for_master_classified_dict)
                print(f"TG alert accounts: {tg_alert_accts}")
                #Get friends of friends of accounts that passed filter
                try:
                    num_jobs = len(tg_alert_accts)
                    print(f'Attempting to get fof of {num_jobs} filtered accts')
                    if num_jobs > process_limit:
                        num_jobs = process_limit
                    if num_jobs > 0:
                        # Get friends of friends of accounts that passed filter
                        fof_after_filt = get_fof_parallel(tg_alert_accts, num_jobs=num_jobs, friend_limit=5000)

                        try:
                            fof_after_filt_flat = flatten_dict(fof_after_filt)
                            fof_after_filt_flat = add_char_ind(fof_after_filt_flat)
                            num_batches_mf = len(fof_after_filt_flat)
                            if num_batches_mf > 20:
                                num_batches_mf = 20
                            table_id_master_friends = "sneakyscraper.scrape_whotwi.master_friends"
                            stream_to_master_friends = batch_stream_bigquery(fof_after_filt_flat, client, table_id_master_friends, num_batches=num_batches_mf) 
                        except Exception as e:
                            print(f'Problem writing to master_friends {e}')
                        
                        # Stream fof of master to BigQuery
                        


                        # Try merging dicts
                        print('Attempting to merge fof metadata for new accounts with other metadata for new accounts...')
                        test_merge = merge_metadata_fof_total(fof_after_filt, tg_alert_accts)
                        test_merge_entry_1 = list(test_merge.keys())[0]
                        print(f"test_merge keys: {test_merge[test_merge_entry_1].keys()}")
                        print(f"test_merge_entry_1: {test_merge[test_merge_entry_1]}")
                        print(f"fof_after_filt.keys(): {fof_after_filt.keys()}")

                        print('Attempting to create metadata dict to send to Vertex AI...')
                        try:
                            test_vertex = convert_dict_to_vertex(fof_after_filt, tg_alert_accts)
                            print(f"test_vertex keys: {test_vertex.keys()}")
                            print(f"test_vertex: {test_vertex}")
                        except Exception as e:
                            print(f"Problem with creating dictionary for Vertex API: {e}")
                            test_vertex = {'instances': [{'username': '1Stigile',
                                            'list_name': 'STIGILE 1',
                                            'bio': ' ',
                                            'website': ' ',
                                            'username_list_name_bio_website': '1Stigile STIGILE 1    ',
                                            'fof_text': '1Stigile STIGILE 1    zhusu Zhu Su CEO/CIO at 3ac | Investing in crypto, DeFi, NFTs, @DeribitExchange, @DeFianceCapital, @StarryNight_Cap'}]}
                            pass

                        # Try getting classifications from Vertex API
                        print('Attempting to classify with Vertex AI API!')
                        try:
                            print('Sending request to Vertex AI API')
                            start_time_ml = time.time()
                            # predict_custom_trained_model_sample(project='sneakyscraper', endpoint_id='user-classifier-api-1', instance_dict=json.dumps(clean_dict), credentials=credentials)
                            # test_response = classify_users_ml(project='sneakyscraper', endpoint_id='865403611989934080', instances=test_vertex)
                            test_response = classify_users_ml_scale(project='sneakyscraper', endpoint_id='865403611989934080', instances=test_vertex)
                            end_time_ml = time.time()
                            tt_ml = end_time_ml - start_time_ml
                            print(f"Total time predicting on cloud run was: {tt_ml}")
                            print(f"Classifications were: {test_response}")
                            print(f"TG alert acounts were: {tg_alert_accts.keys()}")
                        except Exception as e:
                            print(f"Problem with sending predictions to Vertex API: {e}")
                            pass
                    else:
                        print('No accounts passed filter!')
                except Exception as e:
                    print(f'Failed to get fof of filtered accts {e}')
                    pass

                # TODO ML Classification on tg_alert_accts on test_merge dict
                try:
                    print('Attempting to add Vertex AI classifications')
                    for ml_classification in test_response:
                        username = list(ml_classification.keys())[0]
                        account_type_str_vertex = list(ml_classification.values())[0]


                        old_metadata = for_master_classified_dict[username].copy()
                        old_metadata['account_type_str'] = account_type_str_vertex
                        # Replace old classifications with new ones for accounts that passed
                        # the filter
                        for_master_classified_dict[username] = old_metadata
                        tg_alert_accts[username] = old_metadata
                        # print(f"username: {username}, account_type_str_vertex: {account_type_str_vertex}, dict_entry: {for_master_classified_dict[username]['account_type_str']}")
                    print("Finished getting Vertex AI classifications...")
                except Exception as e:
                    print(f"Problem with replacing account_type_str with Vertex result: {e}")
                    pass

# ---------------------------------------------------------
                # print('Packaging bricks for get followers:')
                # get_followers_bricks = []
                # for account in tg_alert_accts.keys():
                #     metadata = tg_alert_accts[account]
                #     # create messages for get_followers:
                #     if metadata['account_type_str'] in ['Crypto Project', 'NFT Project', 'Fund/DAO']:
                #         brick_temp = {'username': metadata['username'], 'num_followers': metadata['num_followers']}
                #         get_followers_bricks.append(brick_temp)

                # print("Preparing to dispatch bricks for getfollowers...")
                # brick_type = 'dispatch_getfollowers'
                # project_id = "sneakyscraper"
                # topic_id = "pagedispatch"
                # if len(get_followers_bricks) > 0:
                #     print('There are followers bricks to send...')
                #     try:
                #         dispatchbricks_resp = dispatch_bricks(get_followers_bricks, project_id, topic_id, brick_origin='friendchanges',
                #         brick_type=brick_type)
                #     except Exception as e:
                #         print("problem with dispatching bricks!")
                #     print(f"response dispatching bricks: {dispatchbricks_resp}")
                #     # print(f"{metadata['username']} was sent to master as {metadata['account_type_str']}")
# ---------------------------------------------------------

                table_id_master = "sneakyscraper.scrape_whotwi.master_clean"
                table_id_last_n_friends = "sneakyscraper.scrape_whotwi.last_n_friends_copy"
                print('Attempting to send telegram alerts...')
                num_alert_accts = len(tg_alert_accts)
                print(f"Number of alerts to send: {num_alert_accts}")
                max_alerts = 60
                if num_alert_accts > max_alerts:
                    tg_alert_accts = {k: tg_alert_accts[k] for k in list(tg_alert_accts)[:max_alerts]}

                # ------- Send TG Alerts --------
                try:
                    # OzDaoProjectAlerts_Group_ID
                    token = os.environ.get('OzDaoProjectsBot')
                    tg_response = send_tg_updates_OzDaoProjectUpdates(tg_alert_accts, client, token=token)
                    print("tg_response OzDaoProjectUpdates:", tg_response)
                except Exception as e:
                    print(f"Problem with sending OzDaoProjectsBot Telegram alerts {e}")
                    pass
                # ----- End Send TG Alerts ------


                # ------- Send TG Alerts --------
                try:
                    token = os.environ.get('SneakyTwitterProjects_bot')
                    tg_response = send_tg_updates(client, tg_alert_accts, token=token)
                    print(tg_response)
                except Exception as e:
                    print(f"Problem with sending Telegram alerts {e}")
                    pass
                # ----- End Send TG Alerts ------

                
                print('Attempting to update master...')
                # -------- Update Master --------
                try:
                    num_batches = len(accounts_classified)
                    if num_batches > 20:
                        num_batches = 20
                        print('------------------------------ Streaming to master ------------------------------')
                    stream_to_master = batch_stream_bigquery(for_master_classified_dict, client, table_id_master, num_batches=num_batches)    
                except Exception as e:
                    print(f"Problem updating master: {e}")
                    pass
                # ------ End Update Master ------

                print('Updating last_n_friends after TG alerts and updating master...')
                # Update last_n_friends on BigQuery (Latest metdata for children of parents)
                stream_to_last_n_friends = batch_stream_bigquery(last_n_friends_dict, client, table_id_last_n_friends, num_batches=20)
            # If there were no accounts classified because of an error
            else:
                print("There were no changes in master detected... Exiting")
                print('Updating last_n_friends on BigQuery')
                table_id_last_n_friends = "sneakyscraper.scrape_whotwi.last_n_friends_copy"
                stream_to_last_n_friends = batch_stream_bigquery(last_n_friends_dict, client, table_id_last_n_friends, num_batches=20)
        # If no changes were detected in friends of children of parents (last_n_friends)
        else:
            print("There were no changes in last_n_friends detected... Exiting")
            print('Updating last_n_friends on BigQuery without changes in last_n_friends')
            table_id_last_n_friends = "sneakyscraper.scrape_whotwi.last_n_friends_copy"
            stream_to_last_n_friends = batch_stream_bigquery(last_n_friends_dict, client, table_id_last_n_friends, num_batches=20)
        # -------- End Get metadata of new friends added by child accounts --------

    else:
        print(f"Job attempt at {time_now.isoformat()} which is outside the regular schedule. Exiting...")
    print(f'Valid request sent at {time_now.isoformat()}, we are visible on {ip}')
    return ("", 204)
    
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))