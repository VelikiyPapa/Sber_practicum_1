import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
                  " Chrome/114.0.0.0 Safari/537.36"
}
MAX_WORKERS = 20

def get_session():
    session = requests.Session()
    session.headers.update(headers)
    return session

def extract_table_data(table, required_columns):
    table_dict = {col: "N/A" for col in required_columns}
    rows = table.find_all('tr')
    for row in rows:
        cells = row.find_all('td')
        for i in range(len(cells) - 1):
            key = cells[i].get_text(strip=True)
            if key in required_columns:
                value = cells[i + 1].get_text(strip=True)
                table_dict[key] = value
    return pd.DataFrame([table_dict])

def process_url(url, session):
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        title_text = soup.title.get_text(strip=True) if soup.title else ""
        title_parts = title_text.split("|")
        title_part1 = title_parts[0].strip() if len(title_parts) > 0 else "N/A"
        title_part2 = "N/A"
        if "ОЭЗ" in title_text:
            title_part2_parts = title_text.split("ОЭЗ")
            title_part2 = title_part2_parts[1].strip() if len(title_part2_parts) > 1 else "N/A"

        industry_text = next((a_tag.get_text(strip=True) for a_tag in soup.find_all('a', href=True)
                              if "industry" in a_tag['href'].lower()), "N/A")

        tables = soup.find_all('table')
        if len(tables) >= 2:
            table1_checked = extract_table_data(tables[0], ["Адрес", "Телефоны", "E-mail", "Веб-сайт"])
            table2_checked = extract_table_data(tables[1], ["Полное наименование", "ИНН", "КПП", "ОГРН", "Руководитель"])
            combined = pd.concat([table1_checked, table2_checked], axis=1)
            combined['Сокращенное наименование'] = title_part1
            combined['ОЭЗ'] = title_part2
            combined['Отрасль'] = industry_text
            return combined
        else:
            return pd.DataFrame()
    except requests.RequestException as e:
        print(f"Ошибка при обработке {url}: {e}")
        return pd.DataFrame()

def process_urls(urls):
    session = get_session()
    all_data = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        progress_bar = tqdm(total=len(urls), desc="Обработка URL")
        for url in urls:
            future = executor.submit(process_url, url, session)
            futures.append(future)
        for future in as_completed(futures):
            data = future.result()
            if not data.empty:
                all_data.append(data)
            progress_bar.update(1)
        progress_bar.close()

    if all_data:
        all_data_df = pd.concat(all_data, ignore_index=True)
    else:
        all_data_df = pd.DataFrame()

    return all_data_df

if __name__ == "__main__":
    urls_df = pd.read_excel('extracted_urls.xlsx')
    urls = urls_df['URL'].dropna().tolist()

    all_data_df = process_urls(urls)

    all_data_df.to_excel('url_data.xlsx', index=False)
    print("URL data extraction complete. The file is saved as 'url_data.xlsx'.")

    if 'ОГРН' in all_data_df.columns:
        ogrn_df = all_data_df[['ОГРН']].drop_duplicates()
        ogrn_df.to_excel('ogrn_list.xlsx', index=False)
        print("OGRN data saved to 'ogrn_list.xlsx'.")
    else:
        print("OGRN column not found in the extracted data.")
