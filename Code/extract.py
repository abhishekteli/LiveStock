import requests, logging, os, json
from dotenv import load_dotenv
from confluent_kafka import Producer

class ExtractStock:
    def __init__(self):
        logging.getLogger('py4j').setLevel(logging.ERROR)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.config = {
            'bootstrap.servers': 'localhost:9092',
            'auto.offset.reset': 'earliest'
        }
        self.producer = Producer(self.config)
        self.topic_gainers = 'Gainers'
        self.topic_losers = 'Losers'
        self.topic_active = 'Active'

        load_dotenv()

    def getRawData(self, trend_type):
        try:
            url = os.getenv("URL")
            querystring = {"trend_type": trend_type, "country": "us", "language": "en"}
            headers = {
                "X-RapidAPI-Key": os.getenv("X-RapidAPI-Key"),
                "X-RapidAPI-Host": os.getenv("X-RapidAPI-Host")
            }
            response = requests.get(url, headers=headers, params=querystring)

            data = response.json()['data']['trends']
            if not data:
                print('Market is Closed', end='')
            else:
                if trend_type == "GAINERS":
                    self.producer.produce(self.topic_gainers,
                                          key=trend_type.encode('utf-8'),
                                          value=json.dumps(data).encode('utf-8'))
                elif trend_type == "LOSERS":
                    self.producer.produce(self.topic_losers,
                                          key=trend_type.encode('utf-8'),
                                          value=json.dumps(data).encode('utf-8'))
                else:
                    self.producer.produce(self.topic_active,
                                          key=trend_type.encode('utf-8'),
                                          value=json.dumps(data).encode('utf-8'))

                self.producer.flush()

        except requests.exceptions.RequestException as req_err:
            logging.error(f'Request error occured: {req_err}')
        except ValueError as json_err:
            logging.error(f'JSON decode error: {json_err}')
        except Exception as e:
            logging.error(f'An unexcepted error occured: {e}')

    def processData(self, trend_type):
        self.getRawData(trend_type)

