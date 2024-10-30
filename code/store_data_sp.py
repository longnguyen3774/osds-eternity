
from pymongo import MongoClient
from bs4_etherscan_scraper import collect_data_block

client = MongoClient('mongodb://localhost:27017')
db = client['ether_db']

transactions_collection = db['transactions']

for i in range(18949821, 18958622):
    transactions = collect_data_block(i)
    if not transactions:
        continue
    transactions_collection.insert_many(transactions)