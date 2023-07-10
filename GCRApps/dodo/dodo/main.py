from flask import Flask, render_template, redirect, url_for, request, make_response
#from flask_bootstrap import Bootstrap
from modules import store_ip_address
import os
import datetime
import json
app = Flask(__name__)

# Flask-WTF requires an enryption key - the string can be anything
app.config['SECRET_KEY'] = 'C2HWGVoMGfNTBsrYQg8EcMrdTimkZfAb'

# Flask-Bootstrap requires this line
#Bootstrap(app)

# with Flask-WTF, each web form is represented by a class
# "NameForm" can change; "(FlaskForm)" cannot
# see the route for "/" and "index.html" to see how this is used

# all Flask routes below

@app.route('/docs', methods=['GET'])
def docs():
    # Signup Dictionary
    user_signup = {}
    # print(request.headers)
    # inbound_brick = request.get_json(force=True)
    # print(f"inbound_brick: {inbound_brick}")
    request_headers = request.headers
    if request_headers:
        headers = json.dumps(dict(request_headers))
    else:
        headers = ''

    user_signup['headers'] = headers
    print(f"request headers: {request_headers}")
    # if 'python' in request_headers['User-Agent']:
    #     print('yes!')
    if request.environ.get('HTTP_X_FORWARDED_FOR') is None:
        ip_address = request.environ['REMOTE_ADDR']
        print(f"ip_address_REMOTE_ADDR: {ip_address}")
    else:
        ip_address = request.environ['HTTP_X_FORWARDED_FOR'] # if behind a proxy
        print(f"ip_address_HTTP_X_FORWARDED_FOR: {ip_address}")

    # Set user ip_address
    user_signup['ip_address'] = ip_address + "_toto"
    # Set update_time
    update_time = datetime.datetime.now().isoformat()
    user_signup['update_time'] = update_time

    # Store ip_address + update_time to BigQuery
    stored_ip_address = store_ip_address(user_signup)
    if stored_ip_address is False:
        print('Error storing ip address')

    
    # you must tell the variable 'form' what you named the class, above
    # 'form' is the variable name used in this template: index.html

    return render_template('index.html')

@app.route('/')
def index():
    return redirect(url_for('docs'))

@app.route('/beta_1', methods=['GET', 'POST'])
def beta_1():
    return render_template('404.html'), 404

@app.route('/beta_1/updates/people', methods=['POST'])
def updates_people():
    resp = make_response("Toto is coming home soon!")
    resp.headers['Server'] = 'toto.ozdao.app'
    resp.headers['API Key'] = 'followtheyellowbrickroad'
    return resp

@app.route('/beta_1/updates/projects', methods=['POST'])
def updates_projects():
    request_headers = None
    if request_headers:
        api_request_headers = json.dumps(dict(request_headers))
        print("api_request_headers:", api_request_headers)


    api_request = request.get_json(force=True)
    print("api_request:", api_request)
    api_request_keys = api_request.keys()
    print("api_request_keys:", api_request_keys)
    api_request_options_keys = api_request['options'].keys()
    print("api_request_options_keys:", api_request_options_keys)

    # resp = make_response("Toto is coming home soon!")
    # resp.headers['Server'] = 'toto.ozdao.app'
    # resp.headers['API Key'] = 'followtheyellowbrickroad'
    return "", 200

    # WORKS 1 ------------------------------
    # headers = {"Content-Type": "application/json"}

    # data = {'TOTO': 'is coming home soon!'}
    # response = app.response_class(
    #     response=json.dumps(data),
    #     status=200,
    #     mimetype='application/json'
    # )
    # return response
    # END WORKS 1 -------------------------

#     @app.route("/")
# def home():
#     resp = flask.Response("Foo bar baz")
#     resp.headers['Access-Control-Allow-Origin'] = '*'
#     return resp
# 2 routes to handle errors - they have templates too
    

# @app.errorhandler(404)
# def page_not_found(e):
#     return render_template('404.html'), 404
#
# @app.errorhandler(500)
# def internal_server_error(e):
#     return render_template('500.html'), 500

# keep this as is
if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
