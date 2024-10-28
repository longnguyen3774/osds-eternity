from datetime import datetime

from pymongo import MongoClient
from etherscan_scraper import collect_data_page
from etherscan_scraper import collect_data_block

# Kết nối đến MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['ether_db']
transactions_collection = db['transactions']

################################################
transactions_collection.update_one({'txn_fee': 0.00021361}, {'$set': {'amount': 0.1}})
for doc in transactions_collection.find({'txn_fee': 0.00021361}):
    print(doc)

# 1 Tổng lượng ETH giao dịch trong ngày/tháng/năm
print("Tổng lượng ETH giao dịch trong ngày:")
get_total_eth_by_date = transactions_collection.aggregate([
    {'$group': {'_id': {
                'year': { '$year': '$age' },
                'month': { '$month': '$age' },
                'day': { '$dayOfMonth': '$age' }},
            'totalETH': { '$sum': '$amount' }}}])

for total in get_total_eth_by_date:
    print(total)

# 2 Phí giao dịch trung bình trong ngày
print("\nPhí giao dịch trung bình trong ngày:")
get_avg_fee_by_date = transactions_collection.aggregate([
    {'$group': {
            '_id': {
                'year': { '$year': '$age' },
                'month': { '$month': '$age' },
                'day': { '$dayOfMonth': '$age' }},
            'avgFee': { '$avg': '$txn_fee' }}}])

for fee in get_avg_fee_by_date:
    print(fee)

# 3 Khối nào có lượng ETH giao dịch lớn nhất
print("\nKhối có lượng ETH giao dịch lớn nhất:")
get_block_with_max_eth = transactions_collection.aggregate([
    {'$group': {
            '_id': '$block',
            'totalETH': { '$sum': '$amount' }}},
    { '$sort': { 'totalETH': -1 } },
    { '$limit': 1 }])

for block in get_block_with_max_eth:
    print(block)

# 4 Số lượng giao dịch từng ngày/tháng/năm
print("\nSố lượng giao dịch từng ngày:")
get_transaction_count_by_date = transactions_collection.aggregate([
    {'$group': {
            '_id': {
                'year': { '$year': '$age' },
                'month': { '$month': '$age' },
                'day': { '$dayOfMonth': '$age' }},
            'count': { '$sum': 1 }}}])

for transaction_count in get_transaction_count_by_date:
    print(transaction_count)

# 5 Tháng/năm có số lượng giao dịch nhiều nhất
print("\nTháng/năm có số lượng giao dịch nhiều nhất:")
get_month_with_max_transaction = transactions_collection.aggregate([
    {'$group': {
            '_id': {
                'year': { '$year': '$age' },
                'month': { '$month': '$age' }},
            'count': { '$sum': 1 }}},
    { '$sort': { 'count': -1 } },
    { '$limit': 1 }])

for max_transaction in get_month_with_max_transaction:
    print(max_transaction)

# 6 Tính tổng số lượng giao dịch từ tất cả các địa chỉ gửi (from) đã thực hiện
print("\nTổng số lượng giao dịch từ tất cả các địa chỉ gửi:")
result = transactions_collection.aggregate([
    { '$group': { '_id': None, 'total_transactions': { '$sum': 1 }}}])

for r in result:
    print(r['total_transactions']) #in ra only 1 giá trị là tổng SL giao dịch từ all các địa chỉ gửi

# 7 Địa chỉ nhận (to) phổ biến
# Đếm số lần mỗi địa chỉ nhận được giao dịch
print("\nĐịa chỉ nhận phổ biến nhất:")
count_to = transactions_collection.aggregate([
    { '$group': { '_id': '$to', 'receive_count': { '$sum': 1 }}},
    { '$sort': { 'receive_count': -1 }},
    { '$limit': 5 }]) #giới hạn chỉ lấy 5 giao dịch

for count in count_to:
    print(count)

# 8 Tìm giao dịch có phí thấp nhất
print("\nGiao dịch có phí thấp nhất:")
get_transaction_with_lowest_fee = transactions_collection.find().sort('txn_fee', 1).limit(1)

for lowest_fee in get_transaction_with_lowest_fee:
    print(lowest_fee)

# 9 Tìm giao dịch có phí cao nhất
print("\nGiao dịch có phí cao nhất:")
get_transaction_with_highest_fee = transactions_collection.find().sort('txn_fee', -1).limit(1)

for highest_fee in get_transaction_with_highest_fee:
    print(highest_fee)

# 10 Tìm giao dịch mới nhất
print("\nGiao dịch mới thực hiện gần đây nhất:")
new_transaction = transactions_collection.find().sort('age', -1).limit(1)

for new in new_transaction:
    print(new)

# Đóng kết nối MongoDB khi hoàn thành
client.close()