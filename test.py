from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pymongo import MongoClient
import time

# Kết nối đến MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["etherscan_db"]
collection = db["transactions"]

def insert_transaction(data):
    # Chỉ thêm giao dịch nếu nó chưa tồn tại
    if not collection.find_one({"hash": data["hash"]}):
        collection.insert_one(data)

def extract_data(driver):
    # Tìm tất cả các hàng trong bảng
    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
    transaction_list = []
    for row in rows:
        # Lấy tất cả các ô trong hàng
        cells = row.find_elements(By.TAG_NAME, "td")
        transaction_data = {
            "hash": cells[1].text,
            "method": cells[2].text,
            "block": cells[3].text,
            "age": cells[4].text,
            "from": cells[5].text,
            "to": cells[7].text,
            "amount": cells[8].text,
            "txn_fee": cells[9].text
        }
        transaction_list.append(transaction_data)
    return transaction_list

def scrape_all_pages(driver):
    all_transactions = []
    while True:
        # Lấy dữ liệu của trang hiện tại
        transactions = extract_data(driver)
        for transaction in transactions:
            insert_transaction(transaction)  # Thêm vào MongoDB
        all_transactions.extend(transactions)

        try:
            # Tìm nút "Next" và bấm để chuyển trang
            next_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.LINK_TEXT, "Next"))
            )
            next_button.click()
            time.sleep(2)  # Đợi trang tải
        except:
            # Nếu không tìm thấy nút "Next", dừng vòng lặp
            break
    return all_transactions

def main():
    # Khởi tạo WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get("https://etherscan.io/txs")

    try:
        all_transactions = scrape_all_pages(driver)
        print(f"{len(all_transactions)} giao dịch đã được thu thập và lưu trữ vào MongoDB.")
    finally:
        driver.quit()  # Đóng trình duyệt sau khi hoàn tất

if __name__ == "__main__":
    main()
