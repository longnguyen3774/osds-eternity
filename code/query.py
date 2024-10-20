from pymongo import MongoClient
from etherscan_scraper import collect_data_page
from etherscan_scraper import collect_data_block

# Kết nối đến MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['ether_db']
transactions_collection = db['transactions']

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

# 5 Số lượng giao dịch từng ngày/tháng/năm
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

# 6 Tháng/năm có số lượng giao dịch nhiều nhất
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

# Đóng kết nối MongoDB khi hoàn thành
client.close()