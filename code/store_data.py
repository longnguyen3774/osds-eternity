
from pymongo import MongoClient
from bs4_etherscan_scraper import collect_data_block

client = MongoClient('mongodb://localhost:27017')
client.drop_database('ether_db')
db = client['ether_db']

transactions_collection = db['transactions']

last_block = 0
i = last_block+1
while True:
    data = collect_data_block(i)
    if i > 1000000:
        if data:
            transactions_collection.insert_many(data)
            i += 1
        else:
            break
    transactions_collection.insert_many(data)