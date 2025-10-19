"""
ASX Materials Companies Scraper
-------------------------------
This script automates the process of scraping data for ASX-listed companies
in the Materials sector (particularly mining and gold companies).

It performs the following steps:
1. Downloads the official ASX company list CSV.
2. Extracts company names, ASX codes, and GICS industry classifications.
3. Filters for companies in the "Materials" sector.
4. Queries Yahoo Finance for each company’s financial statistics.
5. Applies rate limiting and random delays to respect Yahoo Finance traffic policies.
6. Saves results in both JSON and CSV formats for analysis.

Outputs:
- asx_materials_data_progress.json  → interim progress file
- asx_materials_companies_data.json → full structured data
- asx_materials_companies_data.csv  → flattened summary table

Logging:
- Detailed info is logged to the console (INFO level).
- Errors are reported with context for debugging.
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
import random
from typing import List, Dict
import json
import re
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_asx_companies() -> List[Dict]:
    """
    Scrape ASX company list from ASX website.
    Returns a list of dictionaries with company info.
    """
    logger.info("Fetching ASX company list...")

    url = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Save to temporary file and read with pandas
        with open('temp_asx_companies.csv', 'wb') as f:
            f.write(response.content)

        # Try different approaches to read the CSV
        df = None

        # First, let's examine the file structure
        print("Examining CSV file structure...")
        with open('temp_asx_companies.csv', 'r', encoding='utf-8') as f:
            first_lines = [f.readline().strip() for _ in range(5)]
            print("First 5 lines of CSV:")
            for i, line in enumerate(first_lines):
                print(f"Line {i}: {line[:100]}...")  # Show first 100 chars

        # Try reading with different header positions
        for header_row in [0, 1, 2, 3]:
            try:
                print(f"Trying header at row {header_row}...")
                df = pd.read_csv('temp_asx_companies.csv', header=header_row)
                print(f"Columns found: {list(df.columns)}")

                # Check if we have the expected columns (flexible matching)
                columns = [col.strip() for col in df.columns]

                # Look for company name column
                company_col = None
                for col in columns:
                    if 'company' in col.lower() and 'name' in col.lower():
                        company_col = col
                        break

                # Look for ASX code column
                code_col = None
                for col in columns:
                    if 'asx' in col.lower() and 'code' in col.lower():
                        code_col = col
                        break

                # Look for GICS industry column
                gics_col = None
                for col in columns:
                    if 'gics' in col.lower() and 'industry' in col.lower():
                        gics_col = col
                        break

                if company_col and code_col and gics_col:
                    print(f"✓ Found valid structure at header row {header_row}")
                    print(f"Company column: '{company_col}'")
                    print(f"Code column: '{code_col}'")
                    print(f"GICS column: '{gics_col}'")

                    # Rename columns for consistency
                    df = df.rename(columns={
                        company_col: 'company_name',
                        code_col: 'asx_code',
                        gics_col: 'gics_industry_group'
                    })
                    break

            except Exception as e:
                print(f"Failed with header row {header_row}: {e}")
                continue

        if df is None:
            raise Exception("Could not parse CSV with any header configuration")

        # Clean up the dataframe
        df = df.dropna(subset=['company_name', 'asx_code'])

        companies = []
        for _, row in df.iterrows():
            try:
                companies.append({
                    'name': str(row['company_name']).strip(),
                    'code': str(row['asx_code']).strip(),
                    'gics_industry_group': str(row['gics_industry_group']).strip() if pd.notna(
                        row['gics_industry_group']) else 'Unknown'
                })
            except Exception as e:
                print(f"Error processing row: {e}")
                continue

        print(f"Found {len(companies)} ASX companies")

        logger.info(f"Found {len(companies)} ASX companies")

        # Clean up temp file
        try:
            os.remove('temp_asx_companies.csv')
        except Exception as e:
            logger.warning(f"Failed to remove temp file: {e}")

        return companies

    except Exception as e:
        logger.error(f"Error fetching ASX companies: {e}")
        logger.info(
            "You can try downloading the CSV manually from: https://www.asx.com.au/markets/companies/listed-companies")
        return []

def is_materials_company(gics_industry_group: str) -> bool:
    """
    Determine if a company is in the materials sector based on GICS industry group.
    """
    return gics_industry_group.strip().lower() == 'materials'

class YahooFinanceScraper:
    def __init__(self, max_requests_per_minute=20):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        self.max_requests_per_minute = max_requests_per_minute
        self.request_times = []

    def __del__(self):
        """Ensure session is closed when object is destroyed."""
        self.session.close()

    def get_yahoo_symbol(self, asx_code: str) -> str:
        """Convert ASX code to Yahoo Finance symbol format."""
        return f"{asx_code}.AX"

    def _rate_limit_check(self):
        """Implement rate limiting to be respectful to Yahoo Finance."""
        current_time = time.time()

        # Remove requests older than 1 minute
        self.request_times = [t for t in self.request_times if current_time - t < 60]

        # If we've made too many requests in the last minute, wait
        if len(self.request_times) >= self.max_requests_per_minute:
            wait_time = 60 - (current_time - self.request_times[0]) + 1
            logger.info(f"Rate limit reached. Waiting {wait_time:.1f} seconds...")
            time.sleep(wait_time)

        # Add current request time
        self.request_times.append(current_time)

        # Add random delay between 2-5 seconds
        time.sleep(random.uniform(2, 5))

    def scrape_statistics_page(self, symbol: str) -> Dict:
        """Scrape Yahoo Finance statistics page for a given symbol."""
        url = f"https://finance.yahoo.com/quote/{symbol}/key-statistics"

        try:
            logger.info(f"Scraping statistics for {symbol}")
            self._rate_limit_check()

            response = self.session.get(url, timeout=15)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            statistics = {}

            # Look for tables containing financial statistics
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True)
                        value = cells[1].get_text(strip=True)
                        if key and value:
                            statistics[key] = value

            # Key metrics to extract
            key_metrics = [
                'Market Cap', 'Enterprise Value', 'Trailing P/E', 'Forward P/E',
                'Price/Sales', 'Price/Book', 'Enterprise Value/Revenue',
                'Enterprise Value/EBITDA', 'Beta', 'Return on Assets',
                'Return on Equity', 'Revenue', 'Quarterly Revenue Growth',
                'Gross Profit', 'EBITDA', 'Net Income', 'Diluted EPS',
                'Total Cash', 'Total Debt', 'Book Value Per Share',
                'Shares Outstanding', 'Float', 'Avg Vol (3 month)',
                'Avg Vol (10 day)', '52 Week High', '52 Week Low',
                'Dividend Yield', 'Payout Ratio', 'Profit Margin',
                'Operating Margin', 'Quarterly Earnings Growth'
            ]

            # Enhanced search for specific metrics
            page_text = soup.get_text()
            for metric in key_metrics:
                if metric not in statistics:
                    patterns = [
                        rf'{re.escape(metric)}\s*[:\-]?\s*([^\n\r]*)',
                        rf'{re.escape(metric)}\s*</?\w*>\s*([^\n\r<]*)',
                    ]
                    for pattern in patterns:
                        match = re.search(pattern, page_text, re.IGNORECASE)
                        if match:
                            value = match.group(1).strip()
                            if value and len(value) < 50:
                                statistics[metric] = value
                                break

            # Specific search for Shares Outstanding
            shares_patterns = [
                r'Shares Outstanding\s*[:\-]?\s*([\d,\.]+[KMB]?)',
                r'Outstanding Shares\s*[:\-]?\s*([\d,\.]+[KMB]?)',
                r'Total Shares Outstanding\s*[:\-]?\s*([\d,\.]+[KMB]?)'
            ]
            for pattern in shares_patterns:
                if 'Shares Outstanding' not in statistics:
                    match = re.search(pattern, page_text, re.IGNORECASE)
                    if match:
                        statistics['Shares Outstanding'] = match.group(1).strip()
                        break

            return statistics

        except Exception as e:
            logger.error(f"Error scraping statistics for {symbol}: {e}")
            return {}

    def scrape_company_data(self, asx_code: str, company_name: str) -> Dict:
        """Scrape statistics data for a company."""
        symbol = self.get_yahoo_symbol(asx_code)

        logger.info(f"Scraping data for {company_name} ({symbol})")

        statistics = self.scrape_statistics_page(symbol)

        return {
            'asx_code': asx_code,
            'company_name': company_name,
            'yahoo_symbol': symbol,
            'statistics': statistics,
            'scrape_timestamp': pd.Timestamp.now().isoformat()
        }

def main():
    """Main function to orchestrate the scraping process."""
    logger.info("Starting ASX Materials Companies scraper")

    # Step 1: Get all ASX companies
    all_companies = get_asx_companies()
    if not all_companies:
        logger.error("Failed to fetch ASX companies list")
        return

    # Step 2: Filter for materials companies
    materials_companies = [
        company for company in all_companies
        if is_materials_company(company['gics_industry_group'])
    ]

    logger.info(f"Found {len(materials_companies)} materials companies out of {len(all_companies)} total companies")

    if not materials_companies:
        logger.warning("No materials companies found")
        return

    # Print materials companies found
    print("\nMaterials companies to scrape:")
    for company in materials_companies:
        print(f"- {company['name']} ({company['code']})")

    # Step 3: Scrape Yahoo Finance data for each materials company
    scraper = YahooFinanceScraper()
    scraped_data = []

    for i, company in enumerate(materials_companies):
        try:
            logger.info(f"Processing {i + 1}/{len(materials_companies)}: {company['name']}")

            company_data = scraper.scrape_company_data(
                company['code'],
                company['name']
            )
            scraped_data.append(company_data)

            # Save progress after each company
            with open('asx_materials_data_progress.json', 'w') as f:
                json.dump(scraped_data, f, indent=2)

        except Exception as e:
            logger.error(f"Failed to scrape {company['name']}: {e}")
            continue

    # Step 4: Save final results
    if scraped_data:
        # Save as JSON
        with open('asx_materials_companies_data.json', 'w') as f:
            json.dump(scraped_data, f, indent=2)

        # Save as CSV (flattened)
        flattened_data = []
        for company in scraped_data:
            flat_record = {
                'asx_code': company['asx_code'],
                'company_name': company['company_name'],
                'yahoo_symbol': company['yahoo_symbol'],
                'scrape_timestamp': company['scrape_timestamp']
            }

            # Add statistics with prefix
            for key, value in company['statistics'].items():
                flat_record[f'stats_{key}'] = value

            flattened_data.append(flat_record)

        df = pd.DataFrame(flattened_data)
        df.to_csv('asx_materials_companies_data.csv', index=False)

        logger.info(f"Successfully scraped data for {len(scraped_data)} companies")
        logger.info("Data saved to 'asx_materials_companies_data.json' and 'asx_materials_companies_data.csv'")

        # Print summary
        print(f"\n📊 Scraping Summary:")
        print(f"Total materials companies found: {len(materials_companies)}")
        print(f"Successfully scraped: {len(scraped_data)}")
        print(f"Success rate: {len(scraped_data) / len(materials_companies) * 100:.1f}%")

        # Print sample of captured data
        if scraped_data:
            print(f"\n📈 Sample data for {scraped_data[0]['company_name']}:")
            stats = scraped_data[0]['statistics']
            if 'Shares Outstanding' in stats:
                print(f"  • Shares Outstanding: {stats['Shares Outstanding']}")
            if 'Market Cap' in stats:
                print(f"  • Market Cap: {stats['Market Cap']}")
            if 'Revenue' in stats:
                print(f"  • Revenue: {stats['Revenue']}")
            print(f"  • Total metrics captured: {len(stats)}")

    else:
        logger.warning("No data was successfully scraped")

if __name__ == "__main__":
    main()