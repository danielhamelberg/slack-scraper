import unittest
import os
import json
import pandas as pd
from slack import WebClient
from slack.errors import SlackApiError
from sqlalchemy import create_engine
from flask import Flask, request, jsonify

class TestThreads(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.engine = create_engine(os.environ['DATABASE_URL'])
        self.client = WebClient(token=os.environ['SLACK_API_TOKEN'])

    def test_get_threads(self):
        @self.app.route('/threads', methods=['GET'])
        def get_threads():
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')

            threads = pd.read_sql_query(
                'SELECT * FROM threads WHERE ts >= %s AND ts <= %s' % (start_date, end_date),
                con=self.engine
            )

            return jsonify(threads.to_dict(orient='records'))

        with self.app.test_request_context('/threads?start_date=1&end_date=2'):
            resp = get_threads()
            self.assertEqual(resp.status_code, 200)
            self.assertIsNotNone(resp.data)

    def test_webhook(self):
        request_data = {
            'token': os.environ['SLACK_VERIFICATION_TOKEN'],
            'event': {
                'channel': '1234',
                'channel_name': 'test',
                'ts': '12345'
            }
        }

        @self.app.route('/webhook', methods=['POST'])
        def webhook():
            data = request.get_json()

            if data['token'] == os.environ['SLACK_VERIFICATION_TOKEN']:
                channel_id = data['event']['channel']
                channel_name = data['event']['channel_name']

                try:
                    response = self.client.conversations_replies(
                        channel=channel_id,
                        ts=data['event']['ts']
                    )
                except SlackApiError as e:
                    assert e.response["ok"] is False
                    assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
                    print(f"Got an error: {e.response['error']}")

                thread_data = response['messages']

                for thread in thread_data:
                    thread_info = {
                        'channel': channel_name,
                        'ts': thread['ts'],
                        'user': thread['user'],
                        'text': thread['text']
                    }

                    thread_df = pd.DataFrame(thread_info, index=[0])
                    thread_df.to_sql('threads', con=self.engine, if_exists='append', index=False)

            return jsonify(success=True)

        with self.app.test_request_context('/webhook', data=json.dumps(request_data), content_type='application/json'):
            resp = webhook()
            self.assertEqual(resp.status_code, 200)
            self.assertIsNotNone(resp.data)

if __name__ == '__main__':
    unittest.main()
