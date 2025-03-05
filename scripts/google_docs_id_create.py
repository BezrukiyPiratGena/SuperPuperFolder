import os
import random
import string
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv("keys_google_sheet.env")
# Папка, где лежат документы
LOCAL_MANUALS_FOLDER = (
    "C:\Project1\GITProjects\myproject2"  # или './manuals', укажите свой путь
)

# ID вашей Google-таблицы
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")  # ID Google Таблицы MODEL_GPT_INT

# Название листа, где хранятся ID и имена
WORKSHEET_NAME = "ID Мануалов"


def generate_unique_id(existing_ids, length=8):
    """
    Генерирует уникальный ID длиной length из латиницы и цифр.
    Проверяет, чтобы не было совпадения с уже имеющимися existing_ids.
    """
    allowed_chars = string.ascii_letters + string.digits
    while True:
        candidate = "".join(random.choices(allowed_chars, k=length))
        if candidate not in existing_ids:
            return candidate


# === Настройки подключения к GoogleSheets ===
private_key = os.getenv("GOOGLE_PRIVATE_KEY")
if not private_key:
    raise ValueError("GOOGLE_PRIVATE_KEY is not set")
private_key = private_key.replace("\\n", "\n")


def main():
    """
    1. Авторизуемся в Google Sheets.
    2. Открываем лист 'ID Мануалов'.
    3. Считываем уже существующие ID (столбец A) и имена (столбец B).
    4. Сканируем локальную папку с документами.
    5. Для каждого нового файла генерируем уникальный ID и записываем
       в Google Sheets (A - ID, B - название файла).
    """
    # Загрузите свои креденшалы (пример, если у вас уже есть словарь google_credentials)
    google_credentials = {  # Тут все ключи для работы API от гугл щит
        "type": os.getenv("GOOGLE_TYPE"),
        "project_id": os.getenv("GOOGLE_PROJECT_ID"),
        "private_key_id": os.getenv("GOOGLE_PRIVATE_KEY_ID"),
        "private_key": private_key,  # Экранирование переносов строк
        "client_email": os.getenv("GOOGLE_CLIENT_EMAIL"),
        "client_id": os.getenv("GOOGLE_CLIENT_ID"),
        "auth_uri": os.getenv("GOOGLE_AUTH_URI"),
        "token_uri": os.getenv("GOOGLE_TOKEN_URI"),
        "auth_provider_x509_cert_url": os.getenv("GOOGLE_AUTH_PROVIDER_CERT_URL"),
        "client_x509_cert_url": os.getenv("GOOGLE_CLIENT_CERT_URL"),
    }

    # Авторизация в Google Sheets
    credentials = Credentials.from_service_account_info(
        google_credentials, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(credentials)

    # Открываем таблицу и лист "ID Мануалов"
    worksheet = client.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)

    # Считываем все данные из листа (двумерный список)
    all_data = worksheet.get_all_values()

    # Собираем уже имеющиеся ID (столбец A) в множество,
    # чтобы проверять уникальность при генерации
    existing_ids = set()
    for row_index, row in enumerate(all_data):
        if row_index == 0:
            # пропустим строку с заголовками (если есть)
            continue
        if row and len(row) >= 1:
            existing_ids.add(row[0].strip())

    # Узнаем, сколько строк уже занято (чтобы дописывать дальше)
    start_row = len(all_data) + 1  # первая свободная строка

    # Получаем список файлов из локальной папки
    local_files = os.listdir(LOCAL_MANUALS_FOLDER)

    # Перебираем все файлы и добавляем их в таблицу
    row_counter = start_row
    for filename in local_files:
        full_path = os.path.join(LOCAL_MANUALS_FOLDER, filename)
        if os.path.isdir(full_path):
            # Пропускаем папки
            continue

        # Сгенерировать уникальный ID
        unique_id = generate_unique_id(existing_ids, length=8)
        existing_ids.add(unique_id)

        # Записываем ID (A) и название файла (B)
        worksheet.update(f"A{row_counter}", [[unique_id]])
        worksheet.update(f"B{row_counter}", [[filename]])
        row_counter += 1

    print("Готово! Все файлы добавлены в Google Sheets.")


if __name__ == "__main__":
    main()
