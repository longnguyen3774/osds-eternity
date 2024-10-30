import pymongo
from pymongo import MongoClient
from bs4_etherscan_scraper import collect_data_block


# Kết nối với MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client['etherscan_data']
transaction_collection = db['transactions']


def last_block():
    max_block = transaction_collection.aggregate([
        {'$group': {'_id': None, 'maxBlock': {'$max': '$block'}}}
    ])
    for block in max_block:
        print(block['maxBlock'])
        return block['maxBlock']
    return 0


def update_data():
    next_block = (last_block() or 0) + 1
    empty_block = 0  # Biến đếm số lần thu thập block rỗng
    collected_blocks = 0  # Biến đếm số block đã thu thập

    while True:
        transactions = collect_data_block(next_block)

        if not transactions:
            empty_block += 1
            print(f"Block {next_block} rỗng.")
        else:
            transaction_collection.insert_many(transactions)
            collected_blocks += 1  # Tăng số block đã thu thập
            print(f"Đã thêm block {next_block}.")
            empty_block = 0  # Reset biến đếm nếu thu thập thành công

        # Kiểm tra điều kiện dừng
        if empty_block >= 5:  # Dừng nếu gặp block rỗng liên tiếp 5 lần
            print("Đã gặp block rỗng 5 lần liên tiếp. Dừng thu thập.")
            break

        if collected_blocks >= 20:  # Dừng nếu thu thập 20 block mới
            print("Đã thu thập 20 block mới. Dừng thu thập.")
            break

        next_block += 1


if __name__ == "__main__":
    update_data()