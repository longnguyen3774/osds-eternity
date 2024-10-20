from pymongo import MongoClient


def find_new_data(mongo, db, collection, new_data):
    # Kết nối đến MongoDB
    client = MongoClient(mongo)
    db = client[db]
    collection = db[collection]

    # Lấy dữ liệu cũ từ MongoDB
    existing_data = list(collection.find({}, {'_id': 0}))  # Bỏ qua trường '_id'

    # Kiểm tra cấu trúc dữ liệu
    print(existing_data)  # Gỡ lỗi: in ra dữ liệu đã có để xem cấu trúc

    # Tạo tập hợp các 'Transaction Hash' đã có
    existing_hashes = {item['transaction_hash'] for item in existing_data if 'transaction_hash' in item}

    # Lọc ra các giao dịch mới (chưa có trong dữ liệu cũ)
    new_transactions = [tx for tx in new_data if tx['transaction_hash'] not in existing_hashes]

    return new_transactions
