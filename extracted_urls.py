import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random

def extract_links():
    base_url = 'https://оэз.рф/residents/'
    page_number = 1
    all_links = []

    while True:
        if page_number == 1:
            url = base_url
        else:
            url = f'https://оэз.рф/residents/page/{page_number}/'

        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            }
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 404:
                print(f"Страница {page_number} не найдена. Завершаем извлечение.")
                break
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            links = soup.find_all('a', class_='filter__comp')
            if not links and page_number != 1:
                print(f"На странице {page_number} больше нет ссылок. Завершаем извлечение.")
                break

            for link in links:
                href = link.get('href')
                if href:
                    href = href.replace('xn--g1an9b.xn--p1ai', 'оэз.рф')
                    all_links.append(href)

            print(f"Извлечено {len(links)} ссылок со страницы {page_number}.")
            page_number += 1

            time.sleep(random.uniform(1, 2))

        except requests.RequestException as e:
            print(f"Ошибка при доступе к {url}: {e}")
            break

    df = pd.DataFrame({'URL': all_links})
    df.to_excel('extracted_urls.xlsx', index=False)
    print("Извлечение завершено. Ссылки сохранены в 'extracted_urls.xlsx'.")

if __name__ == "__main__":
    extract_links()
