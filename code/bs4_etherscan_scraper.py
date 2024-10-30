import urllib.request
from bs4 import BeautifulSoup
from datetime import datetime

def collect_data_page(block_number, page=1):
    """
    Collect transaction data from the Etherscan website for a specific block and page using urllib and BeautifulSoup.

    Parameters:
    block_number (int): The block number from which to collect transaction data.
    page (int): The page number to collect data from. Defaults to 1.

    Returns:
    list[dict]: A list of dictionaries, where each dictionary contains details of a transaction.

    Each transaction dictionary includes:
        - 'transaction_hash' (str): Hash of the transaction
        - 'method' (str): Method used in the transaction
        - 'block' (int): Block number associated with the transaction
        - 'age' (datetime): Age of the transaction
        - 'from' (str): Address of the sender
        - 'to' (str): Address of the recipient
        - 'amount' (float): Amount of ETH transferred
        - 'txn_fee' (float): Transaction fee

    Raises:
    Exception: If there is an issue with the webpage or accessing an element.
    """
    # Construct the Etherscan URL with specified block and page
    url = f'https://etherscan.io/txs?block={block_number}&ps=100&p={page}'

    # Set headers to emulate a standard web browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    req = urllib.request.Request(url, headers=headers)

    try:
        with urllib.request.urlopen(req) as response:
            page_content = response.read()
    except urllib.error.URLError as e:
        print(f"Error fetching page {page}: {e}")
        return []

    # Parse page content with BeautifulSoup
    soup = BeautifulSoup(page_content, 'html.parser')
    transaction_rows = soup.find_all('tr')[1:]  # Skip the header row

    # If the page contains no transactions, exit early
    if len(transaction_rows) == 1: return []

    transactions = []
    # Loop over each transaction row and extract details
    for row in transaction_rows:
        try:
            columns = row.find_all('td')

            # Extract data fields from transaction columns
            transaction_hash = columns[1].text.strip()
            method = columns[2].text.strip()
            block = int(columns[3].text.strip())

            # Parse age as datetime
            age_str = columns[5].find('span').get('data-bs-title')
            age = datetime.strptime(age_str, '%Y-%m-%d %H:%M:%S')

            # Get sender and recipient addresses
            sender_address = columns[7].find('a').get('href').replace('/address/', '')
            recipient_address = columns[9].find('a').get('href').replace('/address/', '')

            # Parse amount and handle units
            amount_str = columns[10].text.strip().replace(',', '')
            if 'ETH' in amount_str:
                amount = float(amount_str.split()[0])
            elif 'gwei' in amount_str:
                amount = float(amount_str.split()[0]) * 1e-9
            else:
                amount = float(amount_str.split()[0]) * 1e-18

            txn_fee = float(columns[11].text.strip())

            transaction = {
                'transaction_hash': transaction_hash,
                'method': method,
                'block': block,
                'age': age,
                'from': sender_address,
                'to': recipient_address,
                'amount': amount,
                'txn_fee': txn_fee,
            }

            transactions.append(transaction)

        except Exception as e:
            print(f"Error parsing transaction row: {e}")

    return transactions


def collect_data_block(block_number):
    """
    Collect transaction data for all pages associated with a given block on Etherscan.

    Parameters:
    block_number (int): The block number from which to collect transaction data.

    Returns:
    list[dict]: A list of dictionaries, each containing details of all transactions within the block.
    """
    page = 1
    all_transactions = []
    print(f'Collecting transactions for block {block_number}...')
    while True:
        transactions = collect_data_page(block_number, page)
        if transactions:
            all_transactions.extend(transactions)
            print(f'Collected page {page}!')
            page += 1
        else:
            break
    print(f'Block {block_number}: Collection complete!')
    return all_transactions


# Main execution block
if __name__ == "__main__":
    example_block_number = 21005715  # Example block number
    collected_transactions = collect_data_block(example_block_number)

    # Print the collected transaction data
    print(collected_transactions)
