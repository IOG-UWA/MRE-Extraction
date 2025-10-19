"""
ASX Target Stocks Fallback Scraper
----------------------------------
This script is intended to be used **only for companies that failed** in the main ASX Materials Companies scraping run.
It fetches data for a predefined set of ASX codes (TARGET_STOCKS) that were logged as failures.
The script scrapes Yahoo Finance statistics for these target companies and saves results as JSON and CSV.
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

# List of target ASX codes
TARGET_STOCKS = {
    'BM1', 'VTM', 'AJX', 'AA2', 'AR1', 'DMM', 'EV8', 'LLL', 'MNS', 'MC2',
    'MQR', 'ORE', 'OZZ', 'STA', 'TGH', 'TI1', 'XTC'
}


def get_asx_companies() -> List[Dict]:
    """
    Scrape ASX company list from ASX website and filter for target stocks.
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

        # Try reading with different header positions
        for header_row in [0, 1, 2, 3]:
            try:
                df = pd.read_csv('temp_asx_companies.csv', header=header_row)

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
                    # Rename columns for consistency
                    df = df.rename(columns={
                        company_col: 'company_name',
                        code_col: 'asx_code',
                        gics_col: 'gics_industry_group'
                    })
                    break

            except Exception as e:
                continue

        if df is None:
            raise Exception("Could not parse CSV with any header configuration")

        # Clean up the dataframe and filter for target stocks
        df = df.dropna(subset=['company_name', 'asx_code'])
        df = df[df['asx_code'].isin(TARGET_STOCKS)]

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
                continue

        logger.info(f"Found {len(companies)} matching target companies")

        # Clean up temp file
        try:
            os.remove('temp_asx_companies.csv')
        except Exception as e:
            logger.warning(f"Failed to remove temp file: {e}")

        return companies

    except Exception as e:
        logger.error(f"Error fetching ASX companies: {e}")
        return []

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
        self.request_times = [t for t in self.request_times if current_time - t < 60]
        if len(self.request_times) >= self.max_requests_per_minute:
            wait_time = 60 - (current_time - self.request_times[0]) + 1
            logger.info(f"Rate limit reached. Waiting {wait_time:.1f} seconds...")
            time.sleep(wait_time)
        self.request_times.append(current_time)
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
    logger.info("Starting ASX Target Stocks scraper")

    # Step 1: Get target ASX companies
    target_companies = get_asx_companies()
    if not target_companies:
        logger.error("Failed to fetch target companies list")
        return

    logger.info(f"Found {len(target_companies)} target companies")

    if not target_companies:
        logger.warning("No target companies found")
        return

    # Print target companies found
    print("\nTarget companies to scrape:")
    for company in target_companies:
        print(f"- {company['name']} ({company['code']})")

    # Step 2: Scrape Yahoo Finance data for each target company
    scraper = YahooFinanceScraper()
    scraped_data = []

    for i, company in enumerate(target_companies):
        try:
            logger.info(f"Processing {i + 1}/{len(target_companies)}: {company['name']}")
            company_data = scraper.scrape_company_data(
                company['code'],
                company['name']
            )
            scraped_data.append(company_data)
            with open('asx_target_data_progress.json', 'w') as f:
                json.dump(scraped_data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to scrape {company['name']}: {e}")
            continue

    # Step 3: Save final results
    if scraped_data:
        # Save as JSON
        with open('asx_target_companies_data.json', 'w') as f:
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
            for key, value in company['statistics'].items():
                flat_record[f'stats_{key}'] = value
            flattened_data.append(flat_record)

        df = pd.DataFrame(flattened_data)
        df.to_csv('asx_target_companies_data.csv', index=False)

        logger.info(f"Successfully scraped data for {len(scraped_data)} companies")
        logger.info("Data saved to 'asx_target_companies_data.json' and 'asx_target_companies_data.csv'")

        # Print summary
        print(f"\n📊 Scraping Summary:")
        print(f"Total target companies found: {len(target_companies)}")
        print(f"Successfully scraped: {len(scraped_data)}")
        print(f"Success rate: {len(scraped_data) / len(target_companies) * 100:.1f}%")

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