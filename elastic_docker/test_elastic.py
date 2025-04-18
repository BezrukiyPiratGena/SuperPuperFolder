import requests
import json

# === Настройки ===
ELASTIC_URL = "http://kibana.vnigma.ru:9200"  # URL Elasticsearch
INDEX_NAME = "pdf_docs"  # Название индекса
PIPELINE_NAME = "pdf_pipeline"  # Название пайплайна (если используется)

# 🔐 Данные для авторизации (обычный пользователь)
ELASTIC_USER = "kosadmin_user"
ELASTIC_PASSWORD = "Cir73SPb+"

# Заголовки запроса
HEADERS = {"Content-Type": "application/json"}


# === Функция для проверки подключения ===
def check_elastic_connection():
    try:
        response = requests.get(
            f"{ELASTIC_URL}",
            auth=(ELASTIC_USER, ELASTIC_PASSWORD),
            headers=HEADERS,
            verify=False,  # Если HTTPS с самоподписанным сертификатом
            timeout=5,
        )
        if response.status_code == 200:
            print("✅ Elasticsearch доступен!")
            return True
        else:
            print(f"⚠ Ошибка доступа: {response.status_code} - {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Не удалось подключиться к Elasticsearch: {e}")
        return False


# === Функция для отправки запроса ===
def send_data_to_elastic(document):
    if not check_elastic_connection():
        return

    url = f"{ELASTIC_URL}/{INDEX_NAME}/_doc?pipeline={PIPELINE_NAME}"

    try:
        response = requests.post(
            url,
            headers=HEADERS,
            auth=(ELASTIC_USER, ELASTIC_PASSWORD),
            data=json.dumps(document),
            verify=False,  # Если HTTPS с самоподписанным сертификатом
            timeout=10,
        )

        if response.status_code in [200, 201]:
            print(f"✅ Данные успешно загружены: {response.json()}")
        else:
            print(f"❌ Ошибка при загрузке: {response.status_code} - {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"🚨 Ошибка сети при отправке данных: {e}")


# === Тестовые данные ===
test_document = {
    "title": "Тестовый документ",
    "content": "Это тестовая запись в Elasticsearch",
}

# Отправка данных
send_data_to_elastic(test_document)
