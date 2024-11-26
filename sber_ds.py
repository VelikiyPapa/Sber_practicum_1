import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
import random
from tqdm import tqdm

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
    ' AppleWebKit/537.36 (KHTML, like Gecko)'
    ' Chrome/58.0.3029.110 Safari/537.3',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6)'
    ' AppleWebKit/605.1.15 (KHTML, like Gecko)'
    ' Version/12.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0)'
    ' Gecko/20100101 Firefox/52.0',
]

url_error_count = 0
ogrn_error_count = 0

def get_random_headers():
    return {
        'User-Agent': random.choice(user_agents)
    }

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

# Асинхронная функция для обработки каждого URL
async def process_url(semaphore, session, url):
    global url_error_count
    async with semaphore:
        headers = get_random_headers()
        try:
            async with session.get(url, headers=headers, timeout=10) as response:
                if response.status != 200:
                    url_error_count += 1
                    return pd.DataFrame()
                content = await response.text()
                soup = BeautifulSoup(content, 'html.parser')

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
        except Exception:
            url_error_count += 1
            return pd.DataFrame()

# Асинхронная функция для получения информации по ОГРН
async def get_company_info_async(semaphore, session, ogrn):
    global ogrn_error_count
    async with semaphore:
        headers = get_random_headers()
        await asyncio.sleep(random.uniform(0.5, 1.5))

        url = f"https://checko.ru/company/{ogrn}"
        try:
            async with session.get(url, headers=headers, timeout=10) as response:
                if response.status != 200:
                    ogrn_error_count += 1
                    return [ogrn, "N/A", "N/A", "N/A"]
                content = await response.text()
                soup = BeautifulSoup(content, 'html.parser')

                status_div = soup.find('div', class_='text-success fw-700')
                if status_div:
                    status_text = status_div.get_text(strip=True)
                    is_active = "да" if status_text == "Действующая компания" else "нет"
                else:
                    is_active = "нет"

                main_div = soup.find('div', class_='row gy-2 gx-4 mt-0 mb-4')
                revenue = "N/A"
                profit = "N/A"
                if main_div:
                    labels = main_div.find_all('em', class_='d-block mb-1 fw-700')
                    for label_em in labels:
                        label = label_em.get_text(strip=True)
                        if label in ["Выручка", "Чистая прибыль"]:
                            value_div = label_em.find_next_sibling('div', class_='pt-1 pb-1 text-huge')
                            if value_div:
                                link = value_div.find('a')
                                if link:
                                    value_text = link.get_text(strip=True)
                                else:
                                    value_text = value_div.get_text(strip=True)
                                if label == "Выручка":
                                    revenue = value_text
                                elif label == "Чистая прибыль":
                                    profit = value_text
                return [ogrn, is_active, revenue, profit]
        except Exception:
            ogrn_error_count += 1
            return [ogrn, "N/A", "N/A", "N/A"]

# Главная асинхронная функция
async def main():
    urls_df = pd.read_excel('extracted_urls.xlsx')
    urls = urls_df['URL'].dropna().tolist()

    semaphore = asyncio.Semaphore(10)

    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(process_url(semaphore, session, url)) for url in urls]

        results = []
        progress_bar = tqdm(total=len(tasks), desc="Обработка URL")
        for f in asyncio.as_completed(tasks):
            result = await f
            if result is not None and not result.empty:
                results.append(result)
            progress_bar.update(1)
            progress_bar.set_postfix(Ошибки=url_error_count)
        progress_bar.close()

        if results:
            all_data_df = pd.concat(results, ignore_index=True)
        else:
            all_data_df = pd.DataFrame()

    # Извлечение уникальных ОГРН
    if 'ОГРН' in all_data_df.columns:
        ogrns = all_data_df['ОГРН'].dropna().astype(str).unique().tolist()
    else:
        ogrns = []

    # Обработка ОГРН
    if ogrns:
        ogrn_semaphore = asyncio.Semaphore(5)
        async with aiohttp.ClientSession() as session:
            ogrn_tasks = [asyncio.create_task(get_company_info_async(ogrn_semaphore, session, ogrn)) for ogrn in ogrns]

            ogrn_results = []
            progress_bar_ogrn = tqdm(total=len(ogrn_tasks), desc="Обработка ОГРН")
            for f in asyncio.as_completed(ogrn_tasks):
                result = await f
                if result is not None:
                    ogrn_results.append(result)
                progress_bar_ogrn.update(1)
                progress_bar_ogrn.set_postfix(Ошибки=ogrn_error_count)
            progress_bar_ogrn.close()

            ogrn_columns = ['ОГРН', 'Действующая', 'Выручка', 'Чистая прибыль']
            ogrn_df = pd.DataFrame(ogrn_results, columns=ogrn_columns)
    else:
        ogrn_df = pd.DataFrame()

    # Объединение данных по ОГРН с основным DF
    if not ogrn_df.empty:
        final_df = pd.merge(all_data_df, ogrn_df, on='ОГРН', how='left')
    else:
        final_df = all_data_df

    final_df.to_excel('sber_ds.xlsx', index=False)
    print(f"Сбор данных завершен. Файл сохранен как 'sber_ds.xlsx'.")
    print(f"Всего ошибок при обработке URL: {url_error_count}")
    print(f"Всего ошибок при обработке ОГРН: {ogrn_error_count}")

if __name__ == "__main__":
    asyncio.run(main())
