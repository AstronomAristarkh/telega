import requests
from gcn_kafka import Consumer
import os
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
TOKEN = 'TOKEN'
chat_id = os.getenv('chat_id')

consumer = Consumer(client_id=os.getenv('client_id'),
                    client_secret=os.getenv('client_secret'))

# Subscribe to topics and receive alerts
consumer.subscribe(['gcn.notices.einstein_probe.wxt.alert'])
while True:
    for message in consumer.consume(timeout=1):
        if message.error():
            print(message.error())
            continue
        # Print the topic and message ID
        print(f'topic={message.topic()}, offset={message.offset()}')
        value = message.value()
        print(value)
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={value}"
        print(requests.get(url).json()) # Эта строка отсылает сообщение