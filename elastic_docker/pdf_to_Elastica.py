import base64
import requests
import json

# === Настройки ===
pdf_path = "Эпик_ Разработка AI-бота.pdf"  # Укажи путь к PDF
elastic_url = (
    "http://localhost:9200/pdf_docs/_doc?pipeline=pdf_pipeline"  # URL Elasticsearch
)
headers = {"Content-Type": "application/json"}  # Заголовки для JSON


# === Функция конвертации PDF в Base64 ===
def pdf_to_base64(pdf_path):
    with open(pdf_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


# === Кодируем PDF ===
base64_data = pdf_to_base64(pdf_path)

# === Формируем JSON-документ ===
document = {"data": base64_data, "filename": pdf_path}

# === Отправляем в Elasticsearch ===
response = requests.post(elastic_url, headers=headers, data=json.dumps(document))

# === Выводим результат ===
print(response.json())
