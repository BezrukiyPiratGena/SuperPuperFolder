import requests
import json
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# Настройки Elasticsearch
ELASTIC_URL = "https://kibana.vnigma.ru:30006/pdf_docs/_search"
ELASTIC_USER = "kosadmin_user"
ELASTIC_PASSWORD = "Cir73SPb+"
HEADERS = {"Content-Type": "application/json"}


def search_document_by_filename(filename_text):
    """
    Проверяет, содержится ли указанный текст в поле filename в Elasticsearch.
    Возвращает список найденных документов.
    """
    query = {
        "size": 10,
        "_source": ["filename"],
        "query": {
            "bool": {
                "should": [
                    {
                        "term": {"filename.keyword": filename_text}
                    },  # Ищем полное совпадение
                    {"match": {"filename": filename_text}},  # Гибкий поиск
                    {
                        "wildcard": {"filename.keyword": f"*{filename_text}*"}
                    },  # Поиск по подстроке
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
            filenames = [hit["_source"]["filename"] for hit in hits]

            if filenames:
                print(f"✅ Найдены файлы, содержащие '{filename_text}' в имени:")
                for file in filenames:
                    print(f" - {file}")
            else:
                print(f"❌ Файлы, содержащие '{filename_text}', не найдены.")

            return filenames

        else:
            print(f"⚠ Ошибка запроса: {response.status_code} - {response.text}")
            return []

    except requests.exceptions.RequestException as e:
        print(f"🚨 Ошибка сети: {e}")
        return []


# 🔍 Пример использования
filename_text = "ПИКВ-468332-011 Э3_9.pdf"  # Искать этот текст в поле filename
search_document_by_filename(filename_text)
