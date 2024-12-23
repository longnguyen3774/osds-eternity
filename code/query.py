from datetime import datetime

from pymongo import MongoClient
from bs4_etherscan_scraper import collect_data_page
from bs4_etherscan_scraper import collect_data_block

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
    { '$sort': { 'totalETH': -1 }},
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
    { '$sort': { 'count': -1 }},
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

for count_t in count_to:
    print(count_t)

# 8 Địa chỉ gửi (from) phổ biến
# Đếm số lần mỗi địa chỉ gửi được giao dịch
print("\nĐịa chỉ gửi phổ biến nhất:")
count_from = transactions_collection.aggregate([
    { '$group': { '_id': '$from', 'receive_count': { '$sum': 1 }}},
    { '$sort': { 'receive_count': -1 }},
    { '$limit': 5 }]) #giới hạn chỉ lấy 5 giao dịch

for count_f in count_from:
    print(count_f)

# 9 Đếm số lượng giao dịch có giá trị ETH lớn hơn trung bình
    # Tính giá trị trung bình của các giao dịch
average_result = transactions_collection.aggregate([
    { '$group': { '_id': None, 'average_amount': { '$avg': '$amount' }}}])

average_amount = list(average_result)[0]['average_amount']

    # Đếm số giao dịch có giá trị lớn hơn giá trị trung bình
count = transactions_collection.count_documents({ 'amount': { '$gt': average_amount }})
print(f"\nSố lượng giao dịch có giá trị lớn hơn trung bình ({average_amount} ETH): {count}")

# 10 Tìm các địa chỉ gửi (from) có tổng giá trị ETH gửi đi cao nhất
print("\nCác địa chỉ gửi có tổng giá trị ETH gửi đi cao nhất:")
result = transactions_collection.aggregate([
    { '$group': { '_id': '$from', 'total_eth_sent': { '$sum': '$amount' }}},
    { '$sort': { 'total_eth_sent': -1 }}, #sắp xếp từ cao đến thấp
    { '$limit': 5 }]) #chỉ lấy 5 giá trị

for r in result:
    print(r)

# 11 Tìm giao dịch có phí thấp nhất
print("\nGiao dịch có phí thấp nhất:")
get_transaction_with_lowest_fee = transactions_collection.find().sort('txn_fee', 1).limit(1)

for lowest_fee in get_transaction_with_lowest_fee:
    print(lowest_fee)

# 12 Tìm giao dịch có phí cao nhất
print("\nGiao dịch có phí cao nhất:")
get_transaction_with_highest_fee = transactions_collection.find().sort('txn_fee', -1).limit(1)

for highest_fee in get_transaction_with_highest_fee:
    print(highest_fee)

# 13 Tìm giao dịch mới nhất
print("\nGiao dịch mới thực hiện gần đây nhất:")
new_transaction = transactions_collection.find().sort('age', -1).limit(1)

for new in new_transaction:
    print(new)

# 14 Đếm số lượng giao dịch có phi giao dịch thấp hơn 0,001 ETH
count_transaction_lower_than = transactions_collection.count_documents({ 'txn_fee': { '$lt': 0.001 }})
print(f"\nSố lượng giao dịch có phí thấp hơn 0.001 ETH: {count_transaction_lower_than}")

# 15 Đếm tất cả giao dịch có địa chỉ nhận (to) là trống
null_transaction = transactions_collection.count_documents({ 'to': { '$in': [None, '']}})
print(f"\nSố lượng giao dịch không có địa chỉ nhận (to): {null_transaction}")

# 16 Lấy danh sách tất cả các địa chỉ gửi (from) không trùng lặp
print("\nDanh sách các địa chỉ gửi không trùng lặp:")
from_address = transactions_collection.distinct('from')

for address in from_address:
    print(address)

# 17 Tìm các giao dịch có địa chỉ nhận (to) và gửi (from) giống nhau
print("\nGiao dịch mà địa chỉ gửi và nhận giống nhau:")
get_transaction_with_same_address = transactions_collection.find({ '$expr': { '$eq': ['$from', '$to'] }})

for same_address in get_transaction_with_same_address:
    print(same_address)

# 18 Đếm số lượng giao dịch có giá trị ETH bằng 0
count_transaction_equal_0 = transactions_collection.count_documents({ 'amount': 0 })
print(f"\nSố lượng giao dịch có giá trị ETH bằng 0: {count_transaction_equal_0}")

# 19 Đếm số lượng địa chỉ nhận (to) duy nhất trong toàn bộ dữ liệu
count_to_address = len(transactions_collection.distinct('to'))
print(f"\nSố lượng địa chỉ nhận duy nhất trong dữ liệu: {count_to_address}")

# 20 Tính tổng số lượng giao dịch có giá trị ETH bằng hoặc lớn hơn 1
transactions_equal_to_or_greater_than_1 = transactions_collection.count_documents({ 'amount': { '$gte': 1 }})
print(f"\nSố lượng giao dịch có giá trị từ 1 ETH trở lên: {transactions_equal_to_or_greater_than_1}")

# Đóng kết nối MongoDB khi hoàn thành
client.close()