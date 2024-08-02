{
 "cells": [
  {
   "cell_type": "markdown",
   "id": "a67cc56d-53ed-4ad2-ae0f-9c5343b42f5b",
   "metadata": {},
   "source": [
    "# Hockey Stats ETL Pipeline"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "75d4c6b7-520e-48f5-a0a7-e1dca5d970f9",
   "metadata": {},
   "source": [
    "About the Task : \n",
    "\n",
    "This project is a Python-based ETL pipeline for scraping and analyzing hockey team statistics. It uses aiohttp for asynchronous web scraping and BeautifulSoup for HTML parsing. The pipeline fetches data from multiple pages, extracts key stats, and transforms them into a structured format. The results are saved in a ZIP file containing the original HTML pages and an Excel workbook with detailed stats and yearly summaries. Designed for efficiency and modularity, this project includes error handling and type annotations. Ideal for exploring data processing and analysis in Python."
   ]
  },
  {
   "cell_type": "markdown",
   "id": "d9773c95-0d95-4536-b442-d6e9cde1dd1b",
   "metadata": {},
   "source": [
    "### Install the necessary Libraries"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "55f01b0e-52eb-4d77-913e-f39fefa7c586",
   "metadata": {},
   "outputs": [],
   "source": [
    "!pip install aiohttp beautifulsoup4 openpyxl pytest\n"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "b24eb6bb-db9e-419b-bb9a-10022dd16ca7",
   "metadata": {},
   "source": [
    "### I have divided the task into 4 valuable function and lastly run the scraper and saved the Data."
   ]
  },
  {
   "cell_type": "markdown",
   "id": "07b213e6-e161-4066-9c0e-073e90f90dad",
   "metadata": {},
   "source": [
    "### 1. Scraper Functions"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "582cee5c-75dc-4120-aa4e-ef6f1841af9c",
   "metadata": {},
   "outputs": [],
   "source": [
    "import aiohttp\n",
    "import asyncio\n",
    "from bs4 import BeautifulSoup\n",
    "from typing import List, Tuple\n",
    "\n",
    "BASE_URL = \"https://www.scrapethissite.com/pages/forms/\"\n",
    "\n",
    "async def fetch_html(session: aiohttp.ClientSession, url: str) -> str:\n",
    "    async with session.get(url) as response:\n",
    "        return await response.text()\n",
    "\n",
    "async def fetch_all_pages() -> List[str]:\n",
    "    async with aiohttp.ClientSession() as session:\n",
    "        tasks = []\n",
    "        for i in range(1, 25):\n",
    "            url = f\"{BASE_URL}?page_num={i}\"\n",
    "            tasks.append(fetch_html(session, url))\n",
    "        return await asyncio.gather(*tasks)\n",
    "\n",
    "def parse_html(html: str) -> List[Tuple[str, int, int, int, int, float, int, int]]:\n",
    "    soup = BeautifulSoup(html, 'html.parser')\n",
    "    rows = soup.select(\"tr.team\")\n",
    "    data = []\n",
    "    for row in rows:\n",
    "        cols = row.find_all(\"td\")\n",
    "        try:\n",
    "            team_data = (\n",
    "                cols[0].text.strip(),  # Team name\n",
    "                int(cols[1].text.strip()),  # Year\n",
    "                int(cols[2].text.strip()),  # Wins\n",
    "                int(cols[3].text.strip()),  # Losses\n",
    "                int(cols[4].text.strip()),  # OT Losses\n",
    "                float(cols[5].text.strip().strip('%')) / 100,  # Win %\n",
    "                int(cols[6].text.strip()),  # Goals For\n",
    "                int(cols[7].text.strip())   # Goals Against\n",
    "            )\n",
    "        except ValueError:\n",
    "            # If any value is empty or cannot be converted, skip this row\n",
    "            continue\n",
    "        data.append(team_data)\n",
    "    return data\n",
    "\n",
    "async def scrape_data() -> List[str]:\n",
    "    html_pages = await fetch_all_pages()\n",
    "    return html_pages\n"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "324366fb-b0de-472d-a2f4-64ca8b8219a0",
   "metadata": {},
   "source": [
    "### 2. Transformer Functions"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "51359c42-3296-4883-8aa7-cc805ba6ce85",
   "metadata": {},
   "outputs": [],
   "source": [
    "from typing import List, Tuple, Dict\n",
    "\n",
    "def transform_data(data: List[Tuple[str, int, int, int, int, float, int, int]]) -> Tuple[List[List], List[List]]:\n",
    "    all_rows = []\n",
    "    summary = {}\n",
    "    \n",
    "    for row in data:\n",
    "        all_rows.append(row)\n",
    "        year = row[1]\n",
    "        team = row[0]\n",
    "        wins = row[2]\n",
    "        \n",
    "        if year not in summary:\n",
    "            summary[year] = {\"winner\": (team, wins), \"loser\": (team, wins)}\n",
    "        else:\n",
    "            if wins > summary[year][\"winner\"][1]:\n",
    "                summary[year][\"winner\"] = (team, wins)\n",
    "            if wins < summary[year][\"loser\"][1]:\n",
    "                summary[year][\"loser\"] = (team, wins)\n",
    "    \n",
    "    summary_rows = []\n",
    "    for year, result in summary.items():\n",
    "        summary_rows.append([\n",
    "            year,\n",
    "            result[\"winner\"][0], result[\"winner\"][1],\n",
    "            result[\"loser\"][0], result[\"loser\"][1]\n",
    "        ])\n",
    "    \n",
    "    return all_rows, summary_rows\n"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "fc8fdc3c-d5b9-4480-98cc-9097e80a280d",
   "metadata": {},
   "source": [
    "### 3. Excel Generator Functions"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "de70ce44-3792-4168-bd72-ad3e7e0ad666",
   "metadata": {},
   "outputs": [],
   "source": [
    "from openpyxl import Workbook\n",
    "from typing import List\n",
    "\n",
    "def generate_excel(all_rows: List[List], summary_rows: List[List], filename: str) -> None:\n",
    "    wb = Workbook()\n",
    "    ws1 = wb.active\n",
    "    ws1.title = \"NHL Stats 1990-2011\"\n",
    "    \n",
    "    headers = [\"Team\", \"Year\", \"Wins\", \"Losses\", \"OT Losses\", \"Win %\", \"Goals For\", \"Goals Against\"]\n",
    "    ws1.append(headers)\n",
    "    for row in all_rows:\n",
    "        ws1.append(row)\n",
    "    \n",
    "    ws2 = wb.create_sheet(title=\"Winner and Loser per Year\")\n",
    "    summary_headers = [\"Year\", \"Winner\", \"Winner Num. of Wins\", \"Loser\", \"Loser Num. of Wins\"]\n",
    "    ws2.append(summary_headers)\n",
    "    for row in summary_rows:\n",
    "        ws2.append(row)\n",
    "    \n",
    "    wb.save(filename)\n"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "c1aa42b2-5d5b-4392-83f0-a3cf9e1695cd",
   "metadata": {},
   "source": [
    "### 4. Utility Functions"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "d352d396-3746-41b5-81c7-192c587760de",
   "metadata": {},
   "outputs": [],
   "source": [
    "import zipfile\n",
    "import os\n",
    "from typing import List\n",
    "\n",
    "def save_html_files(html_pages: List[str], directory: str) -> None:\n",
    "    if not os.path.exists(directory):\n",
    "        os.makedirs(directory)\n",
    "    for i, html in enumerate(html_pages, start=1):\n",
    "        with open(os.path.join(directory, f\"{i}.html\"), \"w\", encoding=\"utf-8\") as file:\n",
    "            file.write(html)\n",
    "\n",
    "def create_zip_file(directory: str, zip_filename: str) -> None:\n",
    "    with zipfile.ZipFile(zip_filename, 'w') as zipf:\n",
    "        for root, _, files in os.walk(directory):\n",
    "            for file in files:\n",
    "                zipf.write(os.path.join(root, file), arcname=file)\n"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "e31e7996-4461-4212-83bf-a15dc6e95257",
   "metadata": {},
   "source": [
    "### Final Output : Run the Scraper and Save the Data"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "113816e5-beba-4128-8178-4ac96a32faad",
   "metadata": {},
   "outputs": [],
   "source": [
    "import asyncio\n",
    "\n",
    "# Ensure this cell runs in an asynchronous environment\n",
    "from IPython.display import display, HTML\n",
    "display(HTML(\"<script>Jupyter.notebook.kernel.execute('import nest_asyncio; nest_asyncio.apply()')</script>\"))\n",
    "\n",
    "async def main():\n",
    "    html_pages = await scrape_data()\n",
    "    save_html_files(html_pages, \"html_pages\")\n",
    "    create_zip_file(\"html_pages\", \"hockey_stats.zip\")\n",
    "\n",
    "    all_data = []\n",
    "    for html in html_pages:\n",
    "        all_data.extend(parse_html(html))\n",
    "        \n",
    "    all_rows, summary_rows = transform_data(all_data)\n",
    "    generate_excel(all_rows, summary_rows, \"hockey_stats.xlsx\")\n",
    "\n",
    "# Run the async main function\n",
    "await main()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "d5a9e535-033e-4576-8599-add16a1e79a9",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.4"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
