from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime


def collect_data_page(driver, block_number, page=1):
    """
    Collect transaction data from the Etherscan website for a given block and page.

    Parameters:
    driver (webdriver): Selenium WebDriver instance to control the browser.
    block_number (int): The block number from which to collect transaction data.
    page (int): The page number to collect data from. Defaults to 1.

    Returns:
    list[dict]: A list of dictionaries, where each dictionary contains details of a transaction.

    Each transaction contains:
        - 'transaction_hash': Hash of the transaction (str)
        - 'method': Method used in the transaction (str)
        - 'block': Block number associated with the transaction (int)
        - 'age': Age of the transaction (datetime)
        - 'from': Address of the sender (str)
        - 'to': Address of the recipient (str)
        - 'amount': Amount of ETH transferred (float)
        - 'txn_fee': Transaction fee (float)

    Raises:
    Exception: If there is an issue with the webpage or element access.
    """

    try:
        # Open the specified Etherscan transactions page
        driver.get(f'https://etherscan.io/txs?block={block_number}&ps=100&p={page}')

        # Find all 'tr' elements on the page, skip the header row (index 0)
        transaction_rows = driver.find_elements(By.TAG_NAME, 'tr')[1:]

        # List to store transaction data
        transactions = []

        # Iterate over each transaction row ('tr' tag)
        for row in transaction_rows:
            try:
                # Find all 'td' elements in the current row
                transaction_columns = row.find_elements(By.TAG_NAME, 'td')

                # Initialize a dictionary to store the transaction data
                transaction = {}

                # Extract individual transaction details
                transaction_hash = transaction_columns[1].text  # Transaction hash
                method = transaction_columns[2].text  # Transaction method

                # Extract the block number and convert to integer
                block = int(transaction_columns[3].text)

                # Extract the age of the transaction from the 'span' tag and convert to datetime
                age_str = transaction_columns[5].find_element(By.TAG_NAME, 'span').get_attribute('data-bs-title')
                age = datetime.strptime(age_str, '%Y-%m-%d %H:%M:%S')

                # Extract sender's address by removing the base URL part
                sender_address = transaction_columns[7].find_element(By.TAG_NAME, 'a').get_attribute('href').replace(
                    'https://etherscan.io/address/', '')

                # Extract recipient's address by removing the base URL part
                recipient_address = transaction_columns[9].find_element(By.TAG_NAME, 'a').get_attribute('href').replace(
                    'https://etherscan.io/address/', '')

                # Extract the amount transferred and convert to float, accounting for ETH, gwei, or wei units
                amount_str = transaction_columns[10].text
                if amount_str.endswith('ETH'):
                    amount = float(amount_str.replace(' ETH', ''))
                elif amount_str.endswith('gwei'):
                    amount = float(amount_str.replace(' gwei', '')) * 1e-9
                else:
                    amount = float(amount_str.replace(' wei', '')) * 1e-18

                # Extract the transaction fee and convert to float
                txn_fee = float(transaction_columns[11].text)

                # Populate the transaction dictionary
                transaction['transaction_hash'] = transaction_hash
                transaction['method'] = method
                transaction['block'] = block
                transaction['age'] = age
                transaction['from'] = sender_address
                transaction['to'] = recipient_address
                transaction['amount'] = amount
                transaction['txn_fee'] = txn_fee

                # Append the transaction to the list
                transactions.append(transaction)

            except Exception as e:
                # Catch and log any exceptions for this row
                print(f"Error parsing transaction row: {e}")

        # Return the collected transactions
        return transactions

    except Exception as e:
        # Log any exceptions encountered while loading the page
        print(f"Error accessing the page: {e}")
        return []


def collect_data_block(driver, block_number):
    """
    Collect transaction data for all pages associated with a given block on Etherscan.

    Parameters:
    driver (webdriver): Selenium WebDriver instance to control the browser.
    block_number (int): The block number from which to collect transaction data.

    Returns:
    list[dict]: A list of dictionaries, where each dictionary contains details of all transactions within the block.
    """

    page = 1  # Start from the first page
    all_transactions = []

    # Loop through each page until there are no more transactions
    while True:
        # Collect data for the current page
        transactions = collect_data_page(driver=driver, block_number=block_number, page=page)

        if transactions:
            # If transactions exist, add them to the main list and increment the page
            all_transactions.extend(transactions)
            page += 1
        else:
            # Stop when no more transactions are found on the next page
            break

    return all_transactions


# Main execution block
if __name__ == "__main__":
    # Initialize the WebDriver
    _driver = webdriver.Chrome()

    # Collect transaction data for a specific block
    _block_number = 21005715  # Example block number
    _transactions = collect_data_block(_driver, _block_number)

    # Print the collected transaction data
    print(_transactions)

    # Close the WebDriver session
    _driver.quit()