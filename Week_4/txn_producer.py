from pymongo.mongo_client import MongoClient
from pykafka import KafkaClient
import json
import time
import random

# Kafka client oluşturma
kafka_client = KafkaClient(hosts="localhost:9092")
topic = kafka_client.topics['ufuk_homework']

# MongoDB bağlantısı oluşturma
m_client = MongoClient("mongodb+srv://cetingokhan:cetingokhan@cluster0.e1cjhff.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
m_db = m_client["sample_analytics"]
m_collection = m_db["transactions"]

# Kafka producer oluşturma
producer = topic.get_sync_producer()

while True:
    for i in range(random.randint(5, 10)):
        random_doc = m_collection.find_one()
        if random_doc:  
            transactions = random_doc["transactions"]
            transactions_count = len(transactions)
            min_selected_count = 1
            selected_count = random.randint(min_selected_count, transactions_count)
            selected_transactions = random.sample(transactions, selected_count)

            selected_transactionarray = []
            for transaction in selected_transactions:
                selected_transaction = {
                    "account_id": random_doc["account_id"],
                    "date": transaction["date"].strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "amount": transaction["amount"],
                    "transaction_code": transaction["transaction_code"],
                    "symbol": transaction["symbol"],
                    "price": transaction["price"],
                    "total": transaction["total"]
                }
                producer.produce(json.dumps(selected_transaction).encode('utf-8'))

    time.sleep(2)