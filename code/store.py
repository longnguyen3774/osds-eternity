from pymongo import MongoClient
from datetime import datetime
from bs4_etherscan_scraper import collect_data_block

def store_data(block_number):
    data = collect_data_block(block_number)
    client = MongoClient('mongodb://localhost:27017')
    db = client['ether_db']
    transactions_collection = db['transactions']
    if not data:
        print(f"No data collected for block {block_number}.")
        return 0
    try:
        result = transactions_collection.insert_many(data)
        print(f"Insert {len(result.inserted_ids)} documents into {transactions_collection}")
        return len(result.inserted_ids)
    except Exception as e:
        print(f"Errol inserting data into MongoDB: {e}")
        return 0


if __name__ == "__main__":
    block_number = 2100720
    insert =  store_data(block_number)
    print(f"Total documents inserted: {insert}")
