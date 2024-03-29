import csv
import json
from pykafka import KafkaClient

kafkaClient = KafkaClient(hosts="localhost:9092")
topic = kafkaClient.topics["homework"]

with open("events.csv", mode='a', newline='')as file:
    writer = csv.writer(file)
    cons = topic.get_simple_consumer()
    for message in cons:
        if message is not None:
            message_data = json.loads(message.value.decode('utf-8'))
            writer.writerow([
                message_data['account_id'],
                message_data['date'],
                message_data['amount'],
                message_data['transaction_code'],
                message_data['symbol'],
                message_data['price'],
                message_data['total'],
            ])
            print(f"message inverted.")

