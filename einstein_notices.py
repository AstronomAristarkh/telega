import requests
from gcn_kafka import Consumer


TOKEN = "7587835515:AAGANBs7N9DiLO-hD4slIqEwrBF-v3MOtMs"
chat_id = ["1088194493"]

consumer = Consumer(client_id='5g8a3hoef77ledgt7kl5ae2ef3',
                    client_secret='5n5knm0cf1d4u8pfott6iav2b6j593s29au303vrtmljtggkanl')

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