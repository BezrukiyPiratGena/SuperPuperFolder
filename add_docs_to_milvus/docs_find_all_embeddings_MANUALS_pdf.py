import re
import fitz  # PyMuPDF

import logging
from ast import Index
import threading
import time
from venv import logger
import openai
import os
import numpy as np
import boto3
from dotenv import load_dotenv
from pymilvus import (
    connections,
    FieldSchema,
    CollectionSchema,
    DataType,
    Collection,
    utility,
)
from docx import Document
from io import BytesIO
from PIL import Image
from openpyxl import Workbook
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from threading import Lock
import shutil
import chardet  # Для автоматического определения кодировки


# Загрузка переменных среды
load_dotenv("all_tockens.env")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # API токен OpenAI

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")  # Логин для подключенияMiniO
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")  # Пароль для подключения MiniO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")  # IP и порт MiniO
MINIO_REGION_NAME = os.getenv("MINIO_REGION_NAME")  # Регион MiniO
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")  # Название Бакета MiniO
MINIO_FOLDER_DOCS_NAME_SPRAVOCHNIK = os.getenv(
    "MINIO_FOLDER_DOCS_NAME_SPRAVOCHNIK"
)  # Название Папки хранения Таблиц/Изображений Справочника инженеров
MINIO_FOLDER_DOCS_NAME_MANUAL = os.getenv(
    "MINIO_FOLDER_DOCS_NAME_MANUAL"
)  # Название Папки хранения Таблиц/Изображений Мануала
MILVUS_DB_NAME_FIRST = os.getenv(
    "MILVUS_DB_NAME_FIRST"
)  # БД коллекций Милвуса(БД) с справочником

MILVUS_COLLECTION = os.getenv("MILVUS_COLLECTION")  # Коллекция Милвуса(БД)
MILVUS_HOST = os.getenv("MILVUS_HOST")  # IP Милвуса(БД)
MILVUS_PORT = os.getenv("MILVUS_PORT")  # Порт Милвуса(БД)

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)

# =======================================================================================================

DOCX_DIRECTORY = r"C:\Users\CIR\Desktop\jopa\mANUalS\ready_all\1"  # <================= Путь к файлам docx

end_name_docs = ".pdf"  # <============ Конец имени исходного файла, названия коллекции

milvus_collection = "Manuals2"

# =======================================================================================================

docx_files = [file for file in os.listdir(DOCX_DIRECTORY) if file.endswith(".pdf")]
docx_count = len(docx_files)
print(f"Количество релевантных документов: {docx_count}")

# Настройка важных переменных
change_db_of_milvus = MILVUS_DB_NAME_FIRST  # <================================= Выбери бд, в которую будет записываться инфа (Справочник)
if not docx_files:
    raise ValueError("Нет файлов .pdf в указанной директории.")

minio_folder_docs_name = MINIO_FOLDER_DOCS_NAME_MANUAL  # <================================= Выбери папку, в которую будет записываться инфа (Справочник)

name_of_bucket_minio = MINIO_BUCKET_NAME

# path_of_doc_for_convert = r"C:\Project1\GITProjects\myproject2\add_docs_to_milvus\Simrad Autopilot System AP70, AP80 Installation Manual.docx"  # <============== Путь к файлу для добавления его в БД
# description_milvus_collection = name_documents + ".pdf"


openai.api_key = OPENAI_API_KEY

# Подключение к MinIO
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name=MINIO_REGION_NAME,
)
print(f'Логин "{MINIO_ACCESS_KEY}" для БД MiniO')  # Проверка LOG
print(f'Пароль "{MINIO_SECRET_KEY}" для БД MiniO')  # Проверка PSWD

# Подключение к Milvus
connections.connect(
    alias="default", host=MILVUS_HOST, port=MILVUS_PORT, db_name=change_db_of_milvus
)


def process_content_from_pdf(
    pdf_path, bucket_name, description_milvus_collection, collection
):
    """Обрабатывает текст из PDF, создает эмбеддинги и сохраняет в Milvus."""
    text_blocks_with_refs, full_text = extract_content_from_pdf(pdf_path)
    text_blocks = split_text_logically(full_text)

    # Добавление данных в Milvus
    for block in text_blocks:
        if block.strip():
            embedding = create_embeddings(block, description_milvus_collection)
            if embedding:
                data = [
                    [embedding],
                    [block],
                    [description_milvus_collection],
                ]
                collection.insert(data)

    collection.flush()


def fix_text_paragraphs(text):
    """
    Исправляет текст:
    - Добавляет точки между предложениями, если они отсутствуют.
    - Объединяет строки, которые относятся к одному предложению.
    Учитывает знаки `:`, `;`, `.`, `!`, `?` и большие буквы на новой строке.
    """
    lines = text.split("\n")  # Разделяем текст на строки
    fixed_lines = []  # Список для исправленных строк
    buffer = ""  # Буфер для объединения строк, которые относятся к одному предложению

    for i, line in enumerate(lines):
        line = line.strip()  # Убираем лишние пробелы
        if not line:  # Пропускаем пустые строки
            continue

        # Если буфер уже есть, добавляем текущую строку
        if buffer:
            buffer += " " + line
        else:
            buffer = line

        # Проверяем, заканчивается ли строка знаком конца предложения
        if re.search(r"[.!?]$", buffer):  # Конец предложения
            fixed_lines.append(buffer.strip())  # Добавляем завершённое предложение
            buffer = ""  # Очищаем буфер
        elif re.search(r"[:;]$", buffer):  # Если строка заканчивается на `:` или `;`
            continue  # Оставляем в буфере для обработки
        elif (
            i + 1 < len(lines)  # Проверяем, есть ли следующая строка
            and lines[i + 1].strip()  # Следующая строка не пустая
            and lines[i + 1].strip()[0].isupper()  # Начинается с большой буквы
            and not re.search(
                r"[.!?;:]$", buffer
            )  # Текущая строка не заканчивается точкой
        ):
            # Считаем, что предложение завершено, добавляем точку
            fixed_lines.append(buffer.strip())
            buffer = ""  # Очищаем буфер

    # Добавляем остатки буфера как последнюю строку
    if buffer:
        fixed_lines.append(buffer.strip())

    # Склеиваем строки с добавлением точки между абзацами, если нужно
    final_text = ""
    for i, line in enumerate(fixed_lines):
        final_text += line
        if i < len(fixed_lines) - 1:  # Если это не последняя строка
            if not re.search(r"[.!?;:]$", line):  # Исключаем добавление точки
                final_text += "."
        final_text += "\n"

    return final_text.strip()


def extract_content_from_pdf(pdf_path):
    """Извлекает текст и обрабатывает его с помощью fix_text_paragraphs."""
    doc = fitz.open(pdf_path)
    all_text = ""
    text_blocks_with_refs = []

    for page_num, page in enumerate(doc, start=1):
        raw_text = page.get_text("text")  # Извлекаем текст страницы

        # ✅ Определяем кодировку текста
        detected_encoding = chardet.detect(raw_text.encode())
        encoding = (
            detected_encoding["encoding"] if detected_encoding["encoding"] else "utf-8"
        )

        try:
            # ✅ Принудительно конвертируем в UTF-8
            fixed_text = raw_text.encode(encoding, errors="ignore").decode(
                "utf-8", errors="ignore"
            )
        except UnicodeDecodeError:
            print(f"⚠ Ошибка кодировки в файле {pdf_path} (страница {page_num})")
            continue

        fixed_text = fix_text_paragraphs(raw_text)  # Применяем исправление
        all_text += fixed_text + "\n"  # Добавляем текст страницы
        blocks = page.get_text("blocks")  # Для обработки блоков текста
        for block in blocks:
            x0, y0, x1, y1, text = block[:5]
            if text.strip():
                text_blocks_with_refs.append(
                    {
                        "text": fix_text_paragraphs(text.strip()),
                        "reference": f"Page {page_num}",
                    }
                )

    return text_blocks_with_refs, all_text


# Функция создает эмбеддинги ко всему тексту (описание рисунков, текста таблиц, любого текста)
def create_embeddings(
    text, description_milvus_collection, max_retries=5, retry_delay=5
):
    """
    Создает эмбеддинг текста с помощью OpenAI с повторными попытками в случае ошибки.

    Args:
        text (str): Текст для создания эмбеддинга.
        max_retries (int): Максимальное количество попыток перед завершением.
        retry_delay (int): Задержка между попытками (в секундах).

    Returns:
        list: Эмбеддинг текста или завершает скрипт при неудачных попытках.
    """
    if not text.strip():
        return None

    attempt = 0  # Счетчик попыток

    while attempt < max_retries:
        try:
            response = openai.embeddings.create(
                input=[text], model="text-embedding-ada-002"
            )
            return response.data[0].embedding  # Возврат успешного эмбеддинга

        except Exception as e:
            attempt += 1
            print(
                f"Попытка {attempt}/{max_retries} коллекции {description_milvus_collection}: Ошибка при создании эмбеддинга: {e}"
            )

            # Если ошибка связана с API ограничениями (например, 429 или 500)
            if "rate limit" in str(e).lower() or "server error" in str(e).lower():
                print(f"Пауза {retry_delay} секунд перед повторной попыткой...")
                time.sleep(retry_delay)
                continue

            # Если ошибка связана с неподдерживаемым регионом или другой критической причиной
            if "unsupported_country_region_territory" in str(e):
                print("Критическая ошибка: Неподдерживаемый регион.")
                break  # Завершить попытки

            # Задержка перед следующей попыткой для других ошибок
            time.sleep(retry_delay)

    # Если все попытки не удались, завершить выполнение скрипта
    print(
        f"Не удалось создать эмбеддинг после {max_retries} попыток. Завершение скрипта."
    )


def split_text_logically(text):
    """
    Разделяет текст на логические блоки по 150 символов, соблюдая границы предложений,
    при этом перед добавлением следующей части проверяет, не превышает ли блок 100 символов.
    """
    sentences = re.split(
        r"(?<=[.!?])\s+", text.strip()
    )  # Разделяем текст по предложениям
    logical_blocks = []  # Список для хранения логических блоков
    current_block = ""  # Текущий блок текста
    max_length = 100  # Максимальная длина блока
    safe_limit = 70  # Лимит, после которого начинается проверка

    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue  # Пропускаем пустые строки

        # Если предложение длиннее max_length, разрезаем его
        while len(sentence) > max_length:
            logical_blocks.append(sentence[:max_length])  # Сохраняем часть предложения
            sentence = sentence[max_length:]  # Оставшуюся часть снова проверяем

        # Проверяем, не превысит ли добавление нового предложения safe_limit
        if len(current_block) + len(sentence) + 1 <= safe_limit:
            current_block += sentence + " "
        else:
            logical_blocks.append(
                current_block.strip()
            )  # Добавляем текущий блок в список
            current_block = sentence + " "  # Начинаем новый блок

    # Добавляем последний блок, если он не пустой
    if current_block.strip():
        logical_blocks.append(current_block.strip())

    return logical_blocks


def split_table_text_logically(table_data):
    """
    Разделяет таблицу на логические блоки, обрабатывая каждую строку индивидуально.

    Args:
        table_data (list of list of str): Данные таблицы в виде списка строк, где каждая строка - это список ячеек.

    Returns:
        list of str: Список текстовых строк таблицы.
    """
    logical_blocks = []

    for row in table_data:
        # Объединяем ячейки строки через табуляцию
        row_text = "\t".join(row)
        logical_blocks.append(row_text)  # Добавляем строку как отдельный блок

    return logical_blocks


collection_lock = threading.Lock()


# метод для перемещения отработанных мануалов(оригиналов)
def move_file(file_name, destination_path):
    """
    Перемещает файл из текущего местоположения в указанный путь.

    Args:
        file_name (str): Название файла для перемещения.
        destination_path (str): Путь, куда переместить файл.

    Raises:
        FileNotFoundError: Если файл не найден.
        Exception: Если возникает ошибка при перемещении.
    """
    try:
        # Получаем полный путь к файлу
        current_directory = destination_path  # Текущая директория
        source_path = os.path.join(current_directory, file_name)

        # Проверяем, существует ли файл
        if not os.path.exists(source_path):
            raise FileNotFoundError(
                f"Файл {file_name} не найден в {current_directory}."
            )

        # Проверяем, существует ли целевая папка, и создаем, если нет
        if not os.path.exists(f"{destination_path}\\ready"):
            os.makedirs(f"{destination_path}\\ready")

        # Полный путь к новому местоположению файла
        target_path = os.path.join(f"{destination_path}\\ready", file_name)

        # Перемещаем файл
        shutil.move(source_path, target_path)
        print(f"Файл {file_name} успешно перемещен в {destination_path}\\ready")

    except Exception as e:
        print(f"Ошибка при перемещении файла {file_name}: {e}")


def process_docx_file(docx_file, s3_client, path_to_save_manuals):
    """Асинхронная функция для обработки одного файла."""
    print(f"Метод process_docx_file запустился для {docx_file}")

    # Работа с Milvus

    name_documents = os.path.splitext(docx_file)[0]
    path_of_doc_for_convert = os.path.join(DOCX_DIRECTORY, docx_file)
    description_milvus_collection = name_documents + end_name_docs
    # print(f"description_milvus_collection {description_milvus_collection}")

    # Уникальное имя коллекции

    global milvus_collection

    # Загрузка файла в MinIO

    if not utility.has_collection(milvus_collection):
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=1536),
            FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="manual_id", dtype=DataType.VARCHAR, max_length=256),
        ]
        schema = CollectionSchema(fields, description="Коллекция со всеми мануалами")
        collection = Collection(name=milvus_collection, schema=schema)
    else:
        collection = Collection(name=milvus_collection)

    process_content_from_pdf(
        path_of_doc_for_convert,
        name_of_bucket_minio,
        description_milvus_collection,
        collection,
    )

    print(
        "---------------------------------------------------------------------------------------------------"
    )
    print("Начало загрузки коллекции в Milvus")
    # Создание и загрузка индекса в Milvus
    index_params = {
        "index_type": "IVF_FLAT",
        "metric_type": "L2",
        "params": {"nlist": 4096},
    }
    # if not collection.has_index():
    # print("⚙ Индекс отсутствует. Создаём...")
    # collection.create_index(field_name="embedding", index_params=index_params)
    # else:
    #    print("✅ Индекс уже существует. Пропускаем создание.")
    collection.load()
    print(f"Конец загрузки коллекции в Milvus {description_milvus_collection}")
    move_file(description_milvus_collection, path_to_save_manuals)

    print(
        f"Индекс успешно создан и коллекция '{milvus_collection}''{description_milvus_collection}' загружена в БД '{change_db_of_milvus}'"
    )
    print(
        "---------------------------------------------------------------------------------------------------"
    )


def main():
    # Создаем подключение один раз
    try:
        milvus_collection = connections.connect(
            alias="default",
            host=MILVUS_HOST,
            port=MILVUS_PORT,
            db_name=change_db_of_milvus,
        )
        print("Подключение к Milvus успешно установлено!")
    except Exception as e:
        print(f"Ошибка подключения к Milvus: {e}")
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION_NAME,
    )

    # Передаем подключение в потоки
    with ThreadPoolExecutor(
        max_workers=12
    ) as executor:  # <============= max_workers - количество потоков
        executor.map(
            lambda docx_file: process_docx_file(docx_file, s3_client, DOCX_DIRECTORY),
            docx_files,
        )

    print("🔹 Индекс отсутствует. Создаём...")
    index_params = {
        "index_type": "IVF_FLAT",
        "metric_type": "L2",
        "params": {"nlist": 4096},
    }
    milvus_collection = Collection(name="Manuals2")  # Загрузка коллекции

    milvus_collection.create_index(field_name="embedding", index_params=index_params)

    milvus_collection.load()
    print("🎯 Коллекция загружена в память.")


if __name__ == "__main__":
    main()


print(f"Все коллекции загружены.")
