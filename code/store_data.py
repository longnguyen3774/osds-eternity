from selenium import webdriver
from pymongo import MongoClient
from etherscan_scraper import collect_data_block

client = MongoClient('mongodb://localhost:27017')
client.drop_database('ether_db')
db = client['ether_db']

transactions_collection = db['transactions']

driver = webdriver.Chrome()
transactions_collection.insert_many(collect_data_block(driver, 21005717))
transactions_collection.insert_many(collect_data_block(driver, 21005718))
driver.quit()