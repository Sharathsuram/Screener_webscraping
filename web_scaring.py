import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
import re
from urllib.parse import urljoin
import json

class ScreenerFetcher:
    def __init__(self):
        self.base_url = "https://www.screener.in"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.sectors = {
            'Cement & Cement Products': '/market/IN01/IN0102/IN010203/',
            'Chemicals & Petrochemicals': '/market/IN01/IN0101/IN010101/',
            'Gas': '/market/IN03/IN0301/IN030101/',
            'Finance': '/market/IN05/IN0501/IN050101/',
            'Pharmaceuticals & Biotechnology': '/market/IN06/IN0601/IN060101/',
            'Paper, Forest & Jute Products': '/market/IN01/IN0104/IN010401/',
            'Auto Components': '/market/IN02/IN0201/IN020102/',
            'Non - Ferrous Metals': '/market/IN01/IN0103/IN010302/',
            'Commercial Services & Supplies': '/market/IN09/IN0901/IN090104/',
            'Leisure Services': '/market/IN02/IN0206/IN020601/',
            'Textiles & Apparels': '/market/IN02/IN0203/IN020301/',
            'Agricultural, Commercial & Construction Vehicles': '/market/IN07/IN0702/IN070202/',
            'Consumer Durables': '/market/IN02/IN0202/IN020201/',
            'Industrial Products': '/market/IN07/IN0702/IN070205/',
            'Minerals & Mining': '/market/IN01/IN0103/IN010304/',
            'Automobiles': '/market/IN02/IN0201/IN020101/',
            'Realty': '/market/IN02/IN0205/IN020501/',
            'Agricultural Food & other Products': '/market/IN04/IN0401/IN040101/',
            'Industrial Manufacturing': '/market/IN07/IN0702/IN070204/',
            'Fertilizers & Agrochemicals': '/market/IN01/IN0101/IN010102/',
            'Electrical Equipment': '/market/IN07/IN0702/IN070203/',
            'Food Products': '/market/IN04/IN0401/IN040104/',
            'Power': '/market/IN11/IN1101/IN110101/',
            'Construction': '/market/IN07/IN0701/IN070101/',
            'Transport Services': '/market/IN09/IN0901/IN090102/',
            'Personal Products': '/market/IN04/IN0401/IN040105/',
            'IT - Hardware': '/market/IN08/IN0801/IN080103/',
            'IT - Services': '/market/IN08/IN0801/IN080102/',
            'Other Construction Materials': '/market/IN01/IN0102/IN010204/',
            'Entertainment': '/market/IN02/IN0204/IN020402/',
            'Diversified': '/market/IN12/IN1201/IN120101/',
            'Diversified FMCG': '/market/IN04/IN0401/IN040107/',
            'IT - Software': '/market/IN08/IN0801/IN080101/',
            'Petroleum Products': '/market/IN03/IN0301/IN030103/',
            'Other Utilities': '/market/IN11/IN1102/IN110201/',
            'Aerospace & Defense': '/market/IN07/IN0702/IN070201/',
            'Beverages': '/market/IN04/IN0401/IN040102/',
            'Household Products': '/market/IN04/IN0401/IN040106/',
            'Retailing': '/market/IN02/IN0206/IN020603/',
            'Insurance': '/market/IN05/IN0501/IN050104/',
            'Ferrous Metals': '/market/IN01/IN0103/IN010301/',
            'Diversified Metals': '/market/IN01/IN0103/IN010303/',
            'Printing & Publication': '/market/IN02/IN0204/IN020403/',
            'Cigarettes & Tobacco Products': '/market/IN04/IN0401/IN040103/',
            'Healthcare Services': '/market/IN06/IN0601/IN060103/',
            'Telecom - Services': '/market/IN10/IN1001/IN100101/',
            'Telecom -  Equipment & Accessories': '/market/IN10/IN1001/IN100102/',
            'Oil': '/market/IN03/IN0301/IN030102/',
            'Metals & Minerals Trading': '/market/IN01/IN0103/IN010305/',
            'Banks': '/market/IN05/IN0501/IN050102/',
            'Financial Technology (Fintech)': '/market/IN05/IN0501/IN050105/',
            'Other Consumer Services': '/market/IN02/IN0206/IN020602/',
            'Capital Markets': '/market/IN05/IN0501/IN050103/',
            'Consumable Fuels': '/market/IN03/IN0301/IN030104/',
            'Engineering Services': '/market/IN09/IN0901/IN090101/',
            'Healthcare Equipment & Supplies': '/market/IN06/IN0601/IN060102/',
            'Media': '/market/IN02/IN0204/IN020401/',
            'Transport Infrastructure': '/market/IN09/IN0901/IN090103/'
        }

    def _clean_number(self, text):
        if not text or text.strip() == '' or text.strip() == '-':
            return None
        cleaned = re.sub(r'[,\s]', '', text.strip())
        try:
            return float(cleaned)
        except ValueError:
            return None

    def fetch_sector_data(self, sector_name, sector_url, top_n=10):
        full_url = urljoin(self.base_url, sector_url)
        try:
            print(f"Fetching data for {sector_name}...")
            response = self.session.get(full_url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table', class_='data-table')
            if not table:
                print(f"No data table found for {sector_name}")
                return []

            companies = []
            rows = table.find('tbody').find_all('tr')

            for i, row in enumerate(rows[:top_n]):
                cells = row.find_all('td')
                if len(cells) < 10:
                    continue
                company_link = cells[1].find('a')
                if not company_link:
                    continue
                company_data = {
                    'sector': sector_name,
                    'rank': i + 1,
                    'name': company_link.text.strip(),
                    'url': urljoin(self.base_url, company_link.get('href', '')),
                    'cmp': self._clean_number(cells[2].text.strip()),
                    'pe': self._clean_number(cells[3].text.strip()),
                    'market_cap': self._clean_number(cells[4].text.strip()),
                    'div_yield': self._clean_number(cells[5].text.strip()),
                    'net_profit_qtr': self._clean_number(cells[6].text.strip()),
                    'qtr_profit_var': self._clean_number(cells[7].text.strip()),
                    'sales_qtr': self._clean_number(cells[8].text.strip()),
                    'qtr_sales_var': self._clean_number(cells[9].text.strip()),
                    'roce': self._clean_number(cells[10].text.strip()) if len(cells) > 10 else None
                }
                companies.append(company_data)
            print(f"Successfully fetched {len(companies)} companies from {sector_name}")
            return companies
        except Exception as e:
            print(f"Error fetching {sector_name}: {e}")
            return []

    def fetch_all_sectors(self, top_n=10, delay=2):
        all_companies = []
        for sector_name, sector_url in self.sectors.items():
            companies = self.fetch_sector_data(sector_name, sector_url, top_n)
            all_companies.extend(companies)
            time.sleep(delay)
        return all_companies

    def save_to_csv(self, companies, filename='screener_top_companies.csv'):
        if not companies:
            print("No data to save")
            return
        df = pd.DataFrame(companies)
        df.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
        return df

    def get_sector_summary(self, companies):
        if not companies:
            return None
        df = pd.DataFrame(companies)
        summary = df.groupby('sector').agg({
            'name': 'count',
            'market_cap': ['mean', 'median', 'max'],
            'pe': ['mean', 'median'],
            'roce': ['mean', 'median']
        }).round(2)
        return summary

    def fetch_quarterly_results(self, company_name, url):
        try:
            response = self.session.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            section = soup.find('section', id='quarters')
            if not section:
                print(f"[!] Quarterly section not found for {company_name}")
                return None
            table = section.find('table')
            if not table:
                print(f"[!] Table not found for {company_name}")
                return None
            headers = [th.text.strip() for th in table.find('thead').find_all('th')]
            rows = []
            for tr in table.find('tbody').find_all('tr'):
                row = [td.text.strip() for td in tr.find_all(['th', 'td'])]
                if row:
                    rows.append(row)
            df = pd.DataFrame(rows, columns=headers)
            df.insert(0, 'Company', company_name)
            return df
        except Exception as e:
            print(f"[!] Failed to fetch quarterly results for {company_name}: {e}")
            return None

    def fetch_quarterly_for_all(self, companies, output_csv='quarterly_results.csv'):
        all_quarters = []
        for company in companies:
            name = company['name']
            url = company['url']
            print(f"Fetching quarterly results for: {name}")
            df = self.fetch_quarterly_results(name, url)
            if df is not None:
                all_quarters.append(df)
            time.sleep(1.5)
        if all_quarters:
            final_df = pd.concat(all_quarters, ignore_index=True)
            final_df.to_csv(output_csv, index=False)
            print(f"[✓] Quarterly data saved to {output_csv}")
        else:
            print("[!] No quarterly data fetched.")

# Usage example
def main():
    fetcher = ScreenerFetcher()
    print("Fetching sector companies...")
    companies = fetcher.fetch_all_sectors(top_n=5)
    fetcher.save_to_csv(companies, 'top_companies.csv')

    print("\nFetching quarterly performance...")
    fetcher.fetch_quarterly_for_all(companies, output_csv='quarterly_results.csv')

if __name__ == "__main__":
    main()
