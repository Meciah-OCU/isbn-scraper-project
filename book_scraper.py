import os
import time
import logging
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from google.cloud import pubsub_v1, firestore
from flask import Flask, request

app = Flask(__name__)

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
CREDENTIALS_PATH = os.getenv('CREDENTIALS_PATH', '/workspace/isbn-scraper-credentials.json')
GOOGLE_SHEET_NAME = os.getenv('GOOGLE_SHEET_NAME', 'Book_Data')
CHROME_DRIVER_PATH = os.getenv('CHROME_DRIVER_PATH', 'C:/Users/mecia/OneDrive/Documents/chromedriver.exe')
PROJECT_ID = os.getenv('PROJECT_ID', 'isbn-scraper-container-project')
PUBSUB_SUBSCRIPTION = os.getenv('PUBSUB_SUBSCRIPTION', 'isbn-processor-subscription')
CONCURRENT_BROWSERS = 3
DELAY = 10
PAGE_LOAD_TIMEOUT = 60
PORT = int(os.environ.get("PORT", 8080))

# Set up Google Sheets credentials
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
client = gspread.authorize(creds)

# Open Google Sheet
sheet = client.open(GOOGLE_SHEET_NAME).sheet1

def init_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_service = ChromeService(executable_path=CHROME_DRIVER_PATH)
    driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver

def fetch_bookscouter_data(driver, isbn: str) -> str:
    try:
        url = f"https://bookscouter.com/book/{isbn}"
        driver.get(url)
        time.sleep(DELAY)
        
        price_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//*[contains(@class, 'VendorPriceContainer_vobfuvz')]"))
        )
        
        price = price_element.text.split('$')[1].strip()
        
        if not price or not price[0].isdigit():
            logging.warning(f"Invalid price for ISBN {isbn}: {price}")
            return 'N/A'
        
        return price
    except TimeoutException:
        logging.error(f"Timeout error for ISBN {isbn}")
        return 'N/A'
    except NoSuchElementException:
        logging.error(f"Element not found for ISBN {isbn}")
        return 'N/A'
    except Exception as e:
        logging.error(f"Error processing ISBN {isbn}: {str(e)}")
        return 'N/A'

def fetch_restricted_inventory_data(driver, isbn: str) -> str:
    try:
        url = "https://www.restrictedinventory.com/#"
        driver.get(url)
        time.sleep(DELAY)
        
        search_bar = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="restrictiondata-asin"]'))
        )
        search_bar.clear()
        search_bar.send_keys(isbn)
        
        search_button = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="find-asin"]/a'))
        )
        search_button.click()
        
        time.sleep(DELAY)
        
        try:
            not_profitable_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="model_txt"]'))
            )
            if "This item is not profitable enough to sell." in not_profitable_element.text:
                return 'Not profitable'
        except (TimeoutException, NoSuchElementException):
            pass
        
        profit_element = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="profit"]'))
        )
        profit_text = profit_element.text
        profit = profit_text.strip('$')
        
        if not profit or not profit[0].isdigit():
            return 'N/A'
        
        return profit
    except TimeoutException:
        logging.error(f"Timeout error for ISBN {isbn}")
        return 'N/A'
    except NoSuchElementException:
        logging.error(f"Element not found for ISBN {isbn}")
        return 'N/A'
    except StaleElementReferenceException:
        logging.error(f"Stale element reference for ISBN {isbn}")
        return 'N/A'
    except Exception as e:
        logging.error(f"Error processing ISBN {isbn}: {str(e)}")
        return 'N/A'

def process_isbn(isbn: str, row_index: int) -> dict:
    driver = init_driver()
    try:
        bookscouter_price = fetch_bookscouter_data(driver, isbn)
        restricted_inventory_profit = fetch_restricted_inventory_data(driver, isbn)
        
        return {
            'ISBN': isbn,
            'BookScouter': bookscouter_price,
            'RestrictedInventory': restricted_inventory_profit,
            'RowIndex': row_index
        }
    finally:
        driver.quit()

def process_batch(isbn_data: list) -> list:
    with ThreadPoolExecutor(max_workers=CONCURRENT_BROWSERS) as executor:
        results = list(executor.map(lambda x: process_isbn(x[0], x[1]), isbn_data))
    return results

def update_google_sheet(results):
    for result in results:
        sheet.update_cell(result['RowIndex'], 5, result['BookScouter'])  # Update column E
        sheet.update_cell(result['RowIndex'], 6, result['RestrictedInventory'])  # Update column F
    logging.info(f"Updated Google Sheet with {len(results)} results")

def process_pubsub_message(message):
    isbn_batch = message.data.decode('utf-8').split(',')
    results = process_isbn_batch(isbn_batch)
    update_google_sheet(results)
    message.ack()

def listen_for_messages(project_id, subscription_name):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=process_pubsub_message)
    
    try:
        streaming_pull_future.result()
    except Exception as e:
        streaming_pull_future.cancel()
        logging.error(f"Listening for messages failed: {e}")

def update_processing_status(isbn, status):
    db = firestore.Client()
    doc_ref = db.collection('isbn_processing_status').document(isbn)
    doc_ref.set({
        'status': status,
        'timestamp': firestore.SERVER_TIMESTAMP
    })

def check_processing_status(isbn):
    db = firestore.Client()
    doc_ref = db.collection('isbn_processing_status').document(isbn)
    doc = doc_ref.get()
    return doc.to_dict() if doc.exists else None

def process_isbn_batch(isbn_batch):
    results = []
    for isbn, row_index in isbn_batch:
        result = process_isbn(isbn, row_index)
        results.append(result)
        update_processing_status(isbn, 'processed')
    return results

@app.route('/', methods=['POST'])
def process_request():
    logging.info("Received request to process ISBNs")
    listen_for_messages(PROJECT_ID, PUBSUB_SUBSCRIPTION)
    return "Processing complete", 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=PORT)