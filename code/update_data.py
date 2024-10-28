from turtledemo.penrose import start

from selenium.webdriver.common.by import By
from datetime import datetime
from selenium import webdriver
from pymongo import MongoClient
from etherscan_scraper import collect_data_block

client = MongoClient('mongodb://localhost:27017')
client.drop_database('ether_db')
db = client['ether_db']

transactions_collection = db['transactions']

#driver = webdriver.Chrome()
def get_max_block(collection):
    max_block = collection.find_one(sort = [("block", -1)], projection = {"block": 1})
    return max_block["block"] if max_block else 0


def update_blocks_and_collect(driver, collection, start_block=None, end_block=None):
    """
    Liên tục cào dữ liệu giao dịch cho các số block tăng dần.
    """
    # Lấy số block hiện tại từ MongoDB hoặc bắt đầu từ block được chỉ định
    current_block = start_block if start_block is not None else get_max_block(collection) + 1

    while not end_block or current_block <= end_block:
        print(f"Cào dữ liệu cho block số: {current_block}")

        # Cào dữ liệu cho block hiện tại
        transactions = collect_data_block(driver, current_block)

        if transactions:
            collection.insert_many(transactions)
            print(f"Đã thêm {len(transactions)} giao dịch cho block {current_block}")

        current_block += 1  # Tăng số block lên 1 để tiếp tục


if __name__ == "__main__":
    driver = webdriver.Chrome()
    client = MongoClient("mongodb://localhost:27017/")
    db = client["etherscan_data"]
    collection = db["transactions"]

    # Số block bắt đầu (có thể thay đổi)
    update_blocks_and_collect(driver, collection, start_block=21005719, end_block=21005730)

    driver.quit()
