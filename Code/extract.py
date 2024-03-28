import requests, logging, os, json
from datetime import date, datetime
from dotenv import load_dotenv
from confluent_kafka import Producer

logging.getLogger('py4j').setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class ExtractStock:
    def __init__(self):
        self.basedir = (f"/Users/abhishekteli/Documents/Projects/StockDataAnalysis/RawData/year={date.today().year}/"
                        f"month={date.today().month}/day={date.today().day}/")
        self.config = {
            'bootstrap.servers': 'localhost:9092',
            'auto.offset.reset': 'earliest'
        }
        self.producer = Producer(self.config)
        self.topic_gainers = 'Gainers'
        self.topic_losers = 'Losers'

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
                self.producer.produce(self.topic_gainers,
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

