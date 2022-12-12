import os
import json
import pandas as pd
from slack import WebClient
from slack.errors import SlackApiError
from sqlalchemy import create_engine
from flask import Flask, request, jsonify

# Initialize a Flask app to host the API
app = Flask(__name__)

# Set up database
engine = create_engine(os.environ['DATABASE_URL'])

# Set up Slack client
client = WebClient(token=os.environ['SLACK_API_TOKEN'])

# Set up API route
@app.route('/threads', methods=['GET'])
def get_threads():
    # Get date range from query parameters
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    # Get threads from database
    threads = pd.read_sql_query(
        'SELECT * FROM threads WHERE ts >= %s AND ts <= %s' % (start_date, end_date),
        con=engine
    )

    # Return threads as JSON
    return jsonify(threads.to_dict(orient='records'))

# Set up webhook
@app.route('/webhook', methods=['POST'])
def webhook():
    # Get request data
    data = request.get_json()

    # Check if request is from Slack
    if data['token'] == os.environ['SLACK_VERIFICATION_TOKEN']:
        # Get channel ID
        channel_id = data['event']['channel']

        # Get channel name
        channel_name = data['event']['channel_name']

        # Get thread info
        try:
            response = client.conversations_replies(
                channel=channel_id,
                ts=data['event']['ts']
            )
        except SlackApiError as e:
            # You will get a SlackApiError if "ok" is False
            assert e.response["ok"] is False
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
            print(f"Got an error: {e.response['error']}")

        # Get thread data
        thread_data = response['messages']

        # Iterate through thread data
        for thread in thread_data:
            # Get thread info
            thread_info = {
                'channel': channel_name,
                'ts': thread['ts'],
                'user': thread['user'],
                'text': thread['text']
            }

            # Add thread to database
            thread_df = pd.DataFrame(thread_info, index=[0])
            thread_df.to_sql('threads', con=engine, if_exists='append', index=False)

    return jsonify(success=True)

# Run the app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=os.environ['PORT'])
