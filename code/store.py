from collect_data import transactions_collection
from update import find_new_data
from etherscan_scraper import collect_data

new_data = collect_data()
mongo = 'mongodb://localhost:27017/'
db = 'ether_db'
collection = 'transactions'
new_transactions = find_new_data(mongo, db, collection, new_data)
if new_transactions:
    print(f"Tìm thấy {len(new_transactions)} giao dịch mới")
    transactions_collection.insert_many(new_transactions)
    print("Đã lưu các giao dịch mới vào mongodb")
else:
    print("Không có giao dịch mới")