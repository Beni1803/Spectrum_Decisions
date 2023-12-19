import json
import logging
import contextlib
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

# Set up structured logging with timestamps
logging.basicConfig(level=logging.INFO)

# Configure webdriver_manager logging level
logging.getLogger('webdriver_manager').setLevel(logging.WARNING)

# Constants
LAST_VISITED_FILE = 'Backend\last_visited.json'
BASE_URL = "https://ised-isde.canada.ca"

# Context manager for WebDriver
@contextlib.contextmanager
def webdriver_context():
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service)
        yield driver
    except Exception as e:
        logging.error(f"Error initializing WebDriver: {e}")
        raise
    finally:
        if 'driver' in locals():
            driver.quit()

def fetch_and_parse_url(url, driver):
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'html')))
        return BeautifulSoup(driver.page_source, 'html.parser')
    except Exception as e:
        logging.error(f"Error fetching URL: {e}")
        return None

def get_last_visited():
    try:
        with open(LAST_VISITED_FILE, 'r') as file:
            data = json.load(file)
            return data.get('last_visited')
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.warning(f"Error reading last visited file: {e}")
        return None

def save_last_visited(href):
    try:
        with open(LAST_VISITED_FILE, 'w') as file:
            json.dump({'last_visited': href}, file)
    except IOError as e:
        logging.error(f"Error saving last visited link: {e}")

def extract_decision_links(soup, last_visited):
    logging.info("Extracting decision links...")
    decision_rows = soup.select('tbody tr[role="row"]')
    logging.info(f"Found {len(decision_rows)} rows.")
    links = []
    for row in decision_rows:
        link = row.find('a', href=True)
        if link:
            href = link['href']
            full_href = f'https://ised-isde.canada.ca{href}' if not href.startswith('http') else href
            title = link.get_text(strip=True)
            if full_href == last_visited:
                logging.info("Reached the last visited link. Stopping extraction.")
                break
            links.append(full_href)
    logging.info(f"Extracted {len(links)} new links.")
    return links[::-1]  # Reverse to process oldest first

def fetch_decision_page(url, driver):
    logging.info(f"Fetching decision page: {url}")
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'html')))
        return BeautifulSoup(driver.page_source, 'html.parser')
    except Exception as e:
        logging.error(f"Error fetching decision page: {e}")
        return None


def format_frequency_range(freq_range):
    # Regular expression to match various formats including concatenated frequencies
    pattern = r"(\d{3,4})- ?(\d{3,4})|(\d{3,4})/ ?(\d{3,4})|(\d{3,4})-(\d{3,4})(\d{3,4})-(\d{3,4})"
    matches = re.findall(pattern, freq_range)

    formatted_ranges = []
    for match in matches:
        nums = [m for m in match if m]
        if len(nums) == 2:
            formatted_ranges.append('{}-{}'.format(nums[0], nums[1]))
        elif len(nums) == 4:
            formatted_ranges.append('{}-{} {}-{}'.format(nums[0], nums[1], nums[2], nums[3]))

    return ' '.join(formatted_ranges)

def extract_data(soup):
    if not soup:
        return []

    table = soup.find('table', {'class': 'table-bordered'})  # Assuming 'table-bordered' is part of the class list for the table
    caption = table.find('caption', {'class': 'bg-primary'}) if table else None
    table_title = caption.get_text(strip=True) if caption else "Unknown Table Title"

    rows = table.find_all('tr') if table else []
    data_list = []

    if rows:
        headers = [header.get_text(strip=True).replace('\xa0', ' ') for header in rows[0].find_all('th')]
        
        for row in rows[1:]:
            cols = row.find_all('td')
            data = {headers[i]: col.get_text(strip=True).replace('\xa0', ' ') for i, col in enumerate(cols)}
            
            # Format frequency ranges
            if 'Frequency range (MHz)' in data:
                data['Frequency range (MHz)'] = format_frequency_range(data['Frequency range (MHz)'])
            if 'Frequency range (MHz) of the primary licence' in data:
                data['Frequency range (MHz) of the primary licence'] = format_frequency_range(data['Frequency range (MHz) of the primary licence'])
            if 'Frequency range (MHz) of the subordinate licence' in data:
                data['Frequency range (MHz) of the subordinate licence'] = format_frequency_range(data['Frequency range (MHz) of the subordinate licence'])

            # Split tier and geographic area fields
            tier_fields = [
                'Tier number and geographic area of the licence', 
                'Tier number and geographic area of the subordinate licence',
                'Tier number and geographic area of the primary licence'
            ]
            for field in tier_fields:
                if field in data:
                    tier_geo = data[field]
                    parts = tier_geo.split(' ', 1)
                    if len(parts) == 2:
                        data['Tier number'] = parts[0]
                        data['Geographic area of the licence'] = parts[1]
                    del data[field]

            # Append the table title to the data
            data['Table Title'] = table_title

            data_list.append(data)
    
    return data_list


def main():
    with webdriver_context() as driver:
        last_visited = get_last_visited()
        main_soup = fetch_and_parse_url(BASE_URL + '/site/spectrum-management-telecommunications/en/spectrum-allocation/spectrum-licensing/decisions-licence-transfers-commercial-mobile-spectrum', driver)
        
        if main_soup:
            decision_links = extract_decision_links(main_soup, last_visited)

            if decision_links:
                for decision_link in decision_links:
                    decision_soup = fetch_decision_page(decision_link, driver)
                    decision_data = extract_data(decision_soup)
                    for data in decision_data:
                        logging.info(json.dumps(data, indent=4))
                    save_last_visited(decision_link)
                logging.info("Extracted data from all decision links.")
            else:
                logging.info("No new links to visit since the last check.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)