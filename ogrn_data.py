import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import threading
import random
import time
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

class RateLimiter:
    def __init__(self, max_calls, period=1.0):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = threading.Lock()

    def acquire(self):
        with self.lock:
            now = time.time()
            while self.calls and self.calls[0] <= now - self.period:
                self.calls.pop(0)
            if len(self.calls) >= self.max_calls:
                sleep_time = self.calls[0] + self.period - now
                time.sleep(sleep_time)
            self.calls.append(time.time())

rate_limiter = RateLimiter(max_calls=1, period=1)

def get_company_info(ogrn):
    rate_limiter.acquire()
    url = f"https://checko.ru/company/{ogrn}"
    headers = {
        'User-Agent': random.choice(user_agents)
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return [ogrn, "N/A", "N/A", "N/A"]
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
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
                            value_text = "N/A"
                        if label == "Выручка":
                            revenue = value_text
                        elif label == "Чистая прибыль":
                            profit = value_text
        return [ogrn, is_active, revenue, profit]
    except requests.RequestException as e:
        print(f"Ошибка при обработке ОГРН {ogrn}: {e}")
        return [ogrn, "N/A", "N/A", "N/A"]

def process_ogrns(ogrns):
    ogrn_data = []
    max_workers = 10

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(tqdm(executor.map(get_company_info, ogrns), total=len(ogrns), desc="Обработка"))

    ogrn_data.extend(results)
    return ogrn_data

if __name__ == "__main__":
    file_path = 'ogrn_list.xlsx'
    df_ogrn = pd.read_excel(file_path)
    ogrns = df_ogrn['ОГРН'].dropna().astype(str).tolist()
    
    ogrn_data = process_ogrns(ogrns)
    
    ogrn_columns = ['ОГРН', 'Действующая', 'Выручка', 'Чистая прибыль']
    ogrn_df = pd.DataFrame(ogrn_data, columns=ogrn_columns)
    
    ogrn_df.to_excel('ogrn_data.xlsx', index=False)
    print("\nСбор данных по ОГРН завершен. Файл сохранен как 'ogrn_data.xlsx'.")
