"""
IMPORTANT USAGE NOTES:

1. Before running this script, you must manually open Chrome with debugging enabled:
   - Open a command prompt and run:

     "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="C:\ChromeDebug"

   This will open a new Chrome profile with remote debugging enabled.

2. In the newly opened Chrome:
   - Go to: https://datanalysis.morningstar.com.au/af/login
   - Log in with your Morningstar account.
   - Navigate to the Search Tool / ASX Announcements PDF search.
   - Perform the search to generate the list of PDF announcements.

3. Once the PDF list is visible in Chrome, run this Python script.
   - The script will connect to the Chrome instance via the debugger and download the PDFs.
   - Make sure not to close the Chrome window while the script is running.
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import os
import time
import random
''''''
options = Options()
options.debugger_address = "127.0.0.1:9222"
driver = webdriver.Chrome(options=options)

# Create a folder "pdf_downloads" in the current directory if it doesn't exist
default_download_path = os.path.join(os.getcwd(), "pdf_downloads")
os.makedirs(default_download_path, exist_ok=True)

print(driver.title)  # yes its working
print(os)
# Locate the tbody
tbody = driver.find_element(By.XPATH, '//*[@id="search_results"]')

# Get all <tr> rows under it
rows = tbody.find_elements(By.TAG_NAME, "tr")

# Group every 2 rows
data = []
for i in range(0, len(rows), 2):
    try:
        row1 = rows[i]
        row2 = rows[i + 1]

        # Example: extract all <td> text from each row
        cols1 = [td.text.strip() for td in row1.find_elements(By.TAG_NAME, "td")]
        cols2 = [td.text.strip() for td in row2.find_elements(By.TAG_NAME, "td")]

        # Try to find the <a> tag in row2 (PDF link)
        try:
            link_element = row2.find_element(By.XPATH, ".//a[@href and contains(@href, '.pdf')]")
            file_link = link_element.get_attribute("href")

            # Extract filename from link
            filename = file_link.split('/')[-1].split('#')[0]  # safely get the filename
        except:
            file_link = None
            filename = None

        if file_link and filename:
            print(f"[INFO] Clicking to download: {filename}")
            link_element.click()

            # Wait for download to complete
            file_path = os.path.join(default_download_path, filename)
            max_wait = 30
            waited = 0
            while not os.path.exists(file_path) and waited < max_wait:
                time.sleep(1)
                waited += 1
            if os.path.exists(file_path):
                print(f"[✓] Downloaded {filename}")
            else:
                print(f"[ERROR] Download timed out: {filename}")

            # Pause randomly
            sleep_time = random.uniform(1, 3)
            time.sleep(sleep_time)

        combined = cols1 + cols2 + [file_link, filename]
        data.append(combined)
    except IndexError:
        print(f"Skipping incomplete row pair at index {i}")

# Print results
for row in data:
    print(row)

num_data_columns = len(data[0]) - 2
columns = [f'col{i+1}' for i in range(num_data_columns)] + ['file_link', 'filename']

df = pd.DataFrame(data, columns=columns)
df.to_csv("pfsdfsall.csv", index=False)