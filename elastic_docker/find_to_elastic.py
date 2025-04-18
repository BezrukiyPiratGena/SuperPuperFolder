import os
import shutil
import requests
import json
import urllib3

# Отключаем предупреждения SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === Настройки ===
ELASTIC_URL = "https://kibana.vnigma.ru:30006/pdf_docs/_search"
ELASTIC_USER = "kosadmin_user"
ELASTIC_PASSWORD = "Cir73SPb+"
HEADERS = {"Content-Type": "application/json"}

# === Папки ===
pdf_folder = r"C:\Project1\GITProjects\elastic_docker\Доки\ready\3к мануалов 2"
ready_folder = os.path.join(pdf_folder, "ready")

# Создаём папку ready, если её нет
if not os.path.exists(ready_folder):
    os.makedirs(ready_folder)
    print(f"📂 Создана папка: {ready_folder}")


def search_document_by_filename(filename_text):
    """
    Проверяет, содержится ли указанный текст в поле filename в Elasticsearch.
    Возвращает True, если найден, иначе False.
    """
    query = {
        "size": 1,  # Достаточно 1 совпадения
        "_source": ["filename"],
        "query": {
            "bool": {
                "should": [
                    {"term": {"filename.keyword": filename_text}},  # Полное совпадение
                    {"match": {"filename": filename_text}},  # Гибкий поиск
                    {
                        "wildcard": {"filename.keyword": f"*{filename_text}*"}
                    },  # Подстрока
                    {
                        "wildcard": {"filename": f"*{filename_text}*"}
                    },  # Еще один вариант
                ],
                "minimum_should_match": 1,  # Достаточно одного совпадения
            }
        },
    }

    try:
        response = requests.get(
            ELASTIC_URL,
            headers=HEADERS,
            auth=(ELASTIC_USER, ELASTIC_PASSWORD),
            data=json.dumps(query),
            verify=False,
        )

        if response.status_code == 200:
            result = response.json()
            hits = result.get("hits", {}).get("hits", [])
            return len(hits) > 0  # Возвращает True, если файл найден

        else:
            print(f"⚠ Ошибка запроса: {response.status_code} - {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"🚨 Ошибка сети: {e}")
        return False


# === Обход всех PDF-файлов в папке ===
for filename in os.listdir(pdf_folder):
    if filename.lower().endswith(".pdf"):  # Проверяем, что это PDF
        file_path = os.path.join(pdf_folder, filename)
        ready_path = os.path.join(ready_folder, filename)

        print(f"\n🔍 Проверяем файл: {filename} в Elasticsearch...")

        if search_document_by_filename(filename):  # Если файл найден в Elasticsearch
            print(f"✅ Файл найден! Перемещаем в {ready_folder}")
            shutil.move(file_path, ready_path)
        else:
            print(f"❌ Файл НЕ найден в Elasticsearch. Оставляем на месте.")

print("\n🚀 Обработка завершена!")
