from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime


def collect_data(page=1):
    """
    Collect transaction data from the Etherscan website for a given page.

    Parameters:
    page (int): The page number to collect data from. Defaults to 1.

    Returns:
    list[dict]: A list of dictionaries, where each dictionary contains details of a transaction.

    Each transaction contains:
        - 'transaction_hash': Hash of the transaction (str)
        - 'method': Method used in the transaction (str)
        - 'block': Block number associated with the transaction (str)
        - 'age': Age of the transaction (datetime)
        - 'from': Address of the sender (str)
        - 'to': Address of the recipient (str)
        - 'amount': Amount of ETH transferred (float)
        - 'txn_fee': Transaction fee (float)

    Raises:
    Exception: If there is an issue with the webpage or element access.
    """

    try:
        # Initialize the Chrome WebDriver
        driver = webdriver.Chrome()

        # Open the specified Etherscan transactions page
        driver.get('https://etherscan.io/txs?p=' + str(page))

        # Find all 'tr' elements on the page, skip the header row (index 0)
        tr_tags = driver.find_elements(By.TAG_NAME, 'tr')[1:]

        # List to store transaction data
        transactions = []

        # Iterate over each transaction row ('tr' tag)
        for tr_tag in tr_tags:
            try:
                # Find all 'td' elements in the current row
                td_tags = tr_tag.find_elements(By.TAG_NAME, 'td')

                # Initialize a dictionary to store the transaction data
                transaction = dict()

                # Collect individual transaction details
                transaction_hash = td_tags[1].text  # Transaction hash
                method = td_tags[2].text  # Transaction method
                block = td_tags[3].text  # Block number

                # Extract the age of the transaction from the 'span' tag and convert to datetime
                age = str(td_tags[5].find_element(By.TAG_NAME, 'span').get_attribute('data-bs-title'))
                age = datetime.strptime(age, '%Y-%m-%d %H:%M:%S')

                # Extract sender's address by removing the base URL part
                _from = str(td_tags[7].find_element(By.TAG_NAME, 'a').get_attribute('href').replace(
                    'https://etherscan.io/address/', ''))

                # Extract recipient's address by removing the base URL part
                _to = str(td_tags[9].find_element(By.TAG_NAME, 'a').get_attribute('href').replace(
                    'https://etherscan.io/address/', ''))

                # Extract the amount transferred and convert to float, removing 'ETH'
                amount = float(td_tags[10].text.replace(' ETH', ''))

                # Extract the transaction fee and convert to float
                txn_fee = float(td_tags[11].text)

                # Populate the transaction dictionary
                transaction['transaction_hash'] = transaction_hash
                transaction['method'] = method
                transaction['block'] = block
                transaction['age'] = age
                transaction['from'] = _from
                transaction['to'] = _to
                transaction['amount'] = amount
                transaction['txn_fee'] = txn_fee

                # Append the transaction to the list
                transactions.append(transaction)

            except Exception as e:
                print(e)

        # Close the browser after collecting data
        driver.quit()

        # Return the collected transactions
        return transactions

    except Exception as e:
        print(e)
        return []


# Call the function and print the result
if __name__ == "__main__":
    print(collect_data())
