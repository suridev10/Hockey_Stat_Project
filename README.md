# Hockey Stats ETL Pipeline

#Overview
This Python project implements an ETL (Extract, Transform, Load) pipeline for scraping and analyzing hockey team statistics. It uses aiohttp for asynchronous web scraping and BeautifulSoup for HTML parsing.

#Features
Data Scraping: Collects hockey team stats from multiple pages.
Data Transformation: Extracts and processes key statistics.

#Output:
ZIP file with original HTML pages.
Excel workbook with detailed stats and yearly summaries.

#Requirements
aiohttp
BeautifulSoup4
openpyxl
zipfile36
Installation

#Clone the repository and install the required dependencies:

git clone https://github.com/suridev10/Hockey_Stat_Project
cd your-repo
pip install -r requirements.txt

#Usage
Run the following script to start the ETL process:

python your_script.py
This will create a ZIP file containing the HTML pages and an Excel workbook with the processed data.

How to Contribute
Feel free to open issues or submit pull requests. Contributions are welcome!

Contact
For any questions or suggestions, reach out to me at suri.verma10@gmail.com
