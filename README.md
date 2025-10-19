# ASX Mining & Materials Data Tools

This repository contains Python scripts and Jupyter notebooks for scraping, extracting, and enriching data on ASX-listed mining and materials companies. The tools include web scraping, PDF downloading, MRE (Mineral Resource Estimate) extraction, and data enrichment using LLMs.

---

## File Overview

1. **`pull_ASX_mining_companies.ipynb`**  
   Jupyter notebook to fetch a list of ASX-listed mining companies, including filtering for materials/gold companies.

2. **`yfinance_webscraper.py`**  
   Python script to scrape financial statistics from Yahoo Finance for ASX materials companies, with logging and rate limiting.

3. **`yfinance_webscraper(fallback).py`**  
   Fallback scraper for specific ASX companies. Use this script if the main scraper fails for certain companies (e.g., based on a log of failed companies).

4. **`datmorning_downloader.py`**  
   Selenium-based script to download ASX announcement PDFs from Morningstar DataAnalysis.  
   **Important:**  
   - Start Chrome manually with remote debugging before running:  
     ```
     "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="C:\ChromeDebug"
     ```
   - Navigate to the Morningstar DataAnalysis site, go to the ASX Announcement search, and find the list of PDFs.  
   - Then run the script to download PDFs automatically to `./pdf_downloads`.

5. **`mre_extractor.ipynb`**  
   Notebook to process extracted PDF data into wide-format Mineral Resource Estimate (MRE) tables, including computed gold values.

6. **`mre_gemini.ipynb`**  
   Notebook to enhance MRE extraction using a Gemini-based LLM for text/data extraction from PDFs.

7. **`LLM-Prompt-for-Data-Enrichment.txt`**  
   Contains the prompt template used for the LLM-based data enrichment in `mre_gemini.ipynb`.

---

## Usage

1. Start by fetching ASX mining companies using `pull_ASX_mining_companies.ipynb`.
2. Scrape financial data with `yfinance_webscraper.py`. Use the fallback script for any failed companies.
3. Download ASX announcement PDFs using `datmorning_downloader.py`.
4. Extract and process MRE data using `mre_extractor.ipynb`.
5. Optionally enrich MRE data using the LLM with `mre_gemini.ipynb` and `LLM-Prompt-for-Data-Enrichment.txt`.

---

## Requirements

- Python 3.9+  
- Libraries: `pandas`, `numpy`, `selenium`, `beautifulsoup4`, `requests`, `yfinance`, etc.  
- Chrome for Selenium scripts.  

---

## Notes

- PDF downloads will go into `./pdf_downloads`. Make sure the folder exists or will be created automatically.
- Rate limiting and logging are implemented to prevent API blocking when scraping Yahoo Finance.
- Some scripts require manual steps (like opening Chrome with remote debugging) for Selenium automation.

---

## License

This repository is licensed under the **GNU General Public License (GPL)**. You are free to modify, distribute, and use the code under the terms of the GNU GPL.
