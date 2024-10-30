import pymongo
from pymongo import MongoClient
from bs4_etherscan_scraper import collect_data_block

# Kết nối với MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client['ether_db']
transaction_collection = db['transactions']


def last_block():
   max_block = transaction_collection.aggregate([
       {'$group': {'_id': None, 'maxBlock': {'$max': '$block'}}}
   ])
   for block in max_block:
       print(block['maxBlock'])

def update_data():
    next_block = (last_block() or 0 )+ 1
    while True:
        transactions = collect_data_block(next_block)
        if not transactions:
            print("Không còn giao dịch mới")
            break
        transaction_collection.insert_many(transactions)
        print(f"Đã thêm block {next_block}.")
        next_block += 1
if __name__ == "__main__":
    update_data()