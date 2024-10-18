from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime
from pymongo import MongoClient


def connect_mongodb():
    """
    Kết nối đến MongoDB và trả về collection.
    """
    client = MongoClient("mongodb://localhost:27017/")  # Kết nối đến MongoDB server
    db = client['etherscan_data']  # Tạo hoặc kết nối đến database 'etherscan_data'
    collection = db['transactions']  # Tạo hoặc kết nối đến collection 'transactions'
    return collection


def insert_transaction(transaction, collection):
    """
    Thêm một giao dịch vào MongoDB.
    """
    try:
        # Chỉ thêm giao dịch nếu chưa tồn tại (dựa trên transaction_hash)
        collection.update_one(
            {'transaction_hash': transaction['transaction_hash']},
            {'$setOnInsert': transaction},
            upsert=True
        )
    except Exception as e:
        print(f"Lỗi khi lưu giao dịch: {e}")


def collect_data(page=1):
    """
    Thu thập dữ liệu giao dịch từ trang Etherscan cho một trang cụ thể.
    """
    try:
        # Kết nối đến MongoDB
        collection = connect_mongodb()

        # Khởi tạo Chrome WebDriver
        driver = webdriver.Chrome()

        # Truy cập trang Etherscan
        driver.get('https://etherscan.io/txs?p=' + str(page))

        # Lấy tất cả các thẻ 'tr', bỏ qua hàng tiêu đề (index 0)
        tr_tags = driver.find_elements(By.TAG_NAME, 'tr')[1:]

        # Danh sách lưu trữ dữ liệu giao dịch
        transactions = []

        # Duyệt qua từng hàng giao dịch ('tr' tag)
        for tr_tag in tr_tags:
            try:
                # Tìm các thẻ 'td' trong hàng hiện tại
                td_tags = tr_tag.find_elements(By.TAG_NAME, 'td')

                # Khởi tạo dictionary để lưu thông tin giao dịch
                transaction = dict()

                # Thu thập các chi tiết giao dịch
                transaction_hash = td_tags[1].text  # Transaction hash
                method = td_tags[2].text  # Transaction method
                block = td_tags[3].text  # Block number

                # Lấy thời gian giao dịch từ thẻ 'span' và chuyển đổi sang datetime
                age = str(td_tags[5].find_element(By.TAG_NAME, 'span').get_attribute('data-bs-title'))
                age = datetime.strptime(age, '%Y-%m-%d %H:%M:%S')

                # Lấy địa chỉ gửi và nhận
                _from = str(td_tags[7].find_element(By.TAG_NAME, 'a').get_attribute('href').replace(
                    'https://etherscan.io/address/', ''))
                _to = str(td_tags[9].find_element(By.TAG_NAME, 'a').get_attribute('href').replace(
                    'https://etherscan.io/address/', ''))

                # Lấy số tiền và phí giao dịch
                amount = float(td_tags[10].text.replace(' ETH', ''))
                txn_fee = float(td_tags[11].text)

                # Đưa các thông tin vào dictionary
                transaction['transaction_hash'] = transaction_hash
                transaction['method'] = method
                transaction['block'] = block
                transaction['age'] = age
                transaction['from'] = _from
                transaction['to'] = _to
                transaction['amount'] = amount
                transaction['txn_fee'] = txn_fee

                # Lưu giao dịch vào MongoDB
                insert_transaction(transaction, collection)

                # Thêm giao dịch vào danh sách
                transactions.append(transaction)

            except Exception as e:
                print(e)

        # Đóng trình duyệt sau khi thu thập dữ liệu
        driver.quit()

        # Trả về danh sách các giao dịch đã thu thập
        return transactions

    except Exception as e:
        print(e)
        return []


# Gọi hàm và in kết quả
if __name__ == "__main__":
    transactions = collect_data()
    print(transactions)
