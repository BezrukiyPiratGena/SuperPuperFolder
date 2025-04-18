import requests
import json
import warnings

warnings.simplefilter("ignore")  # Игнорируем предупреждения SSL

# === Настройки ===
elastic_url = "https://kibana.vnigma.ru:30006/pdf_docs/_search"  # URL Elasticsearch
headers = {"Content-Type": "application/json"}  # Заголовки

# 🔐 Вводим логин и пароль
elastic_user = "kosadmin_user"
elastic_password = "Cir73SPb+"
# === Вводим поисковую фразу ===
search_text = input("Введите текст для поиска: ")

# === Формируем JSON-запрос с подсветкой ===
query = {
    "query": {
        "match_phrase": {"attachment.content": search_text}  # Поиск по тексту PDF
    },
    "highlight": {  # Подсветка найденного текста
        "fields": {
            "attachment.content": {
                "fragment_size": 50,  # Длина одного фрагмента
                "number_of_fragments": 5,  # Сколько фрагментов выводить
            }
        }
    },
}

# === Отправляем запрос в Elasticsearch с авторизацией ===
response = requests.get(
    elastic_url,
    headers=headers,
    data=json.dumps(query),
    auth=(elastic_user, elastic_password),  # Авторизация
    verify=False,  # Отключаем проверку SSL (небезопасно, но помогает в тестах)
)

# === Обрабатываем ответ ===
if response.status_code == 200:
    result = response.json()
    hits = result["hits"]["hits"]

    if hits:
        print("\n🔎 Найденные документы:")
        for hit in hits:
            filename = hit["_source"]["filename"]
            highlights = hit.get("highlight", {}).get(
                "attachment.content", ["Фрагменты не найдены"]
            )

            print(f"\n📄 Файл: {filename}")
            for i, fragment in enumerate(highlights, start=1):
                print(f"  🔹 Фрагмент {i}: {fragment}")
    else:
        print("\n❌ Ничего не найдено.")
else:
    print("\n⚠ Ошибка запроса:", response.status_code, response.text)
