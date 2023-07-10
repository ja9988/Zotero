import os
from flask import Flask
from google.cloud import bigquery
import datetime
from process_tables import remake_all_records_by_userid, remake_username_userids_table

app = Flask(__name__)

# Create BigQuery Client
print('Creating BigQuery Client...')
project_id = 'sneakyscraper'
client = bigquery.Client(project=project_id)

# https://ozdaobotinteractive-4vq2p727ya-uc.a.run.app/5325641015:AAHPKBRwHqE0TKxRxVkkBR32nGvjXevDE0I

@app.route(f"/", methods=["GET"])
def index():
    time_now = datetime.datetime.now()
    print(f"Starting container at: {time_now.isoformat()}")
    try:
        all_records_by_userid_resp = remake_all_records_by_userid(client)
        print(f"all_records_by_userid_resp: {all_records_by_userid_resp}")
        username_userids_resp = remake_username_userids_table(client)
        print(f"username_userids_resp: {username_userids_resp}")
    except Exception as e:
        print(f"problem writing tables: {e}")
        pass
    time_now_2 = datetime.datetime.now()
    print(f"Stopping container at: {time_now_2.isoformat()}")

    return ("", 204)
    # return f"check_friend_changes returned an object of type {type(cfc_json)}"



if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
