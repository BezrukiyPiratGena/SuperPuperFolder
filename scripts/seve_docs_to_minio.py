import logging
import asyncio
from ast import Index
import re
import threading
import time
from venv import logger
import spacy
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
import tiktoken
from openpyxl import Workbook
import aioboto3
import aiofiles
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from threading import Lock
import shutil
import sys
import requests
import http.client

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

DOCX_DIRECTORY = r"C:\Project1\GITProjects\elastic_docker\Доки"  # <================= Путь к файлам docx

end_name_docs = ".pdf"  # <============ Конец имени исходного файла, названия коллекции

# =======================================================================================================

docx_files = [file for file in os.listdir(DOCX_DIRECTORY) if file.endswith(".pdf")]
docx_count = len(docx_files)

print(f"Количество релевантных документов: {docx_count}")

# Настройка важных переменных
change_db_of_milvus = MILVUS_DB_NAME_FIRST  # <================================= Выбери бд, в которую будет записываться инфа (Справочник)
if not docx_files:
    raise ValueError("Нет файлов .docx в указанной директории.")

minio_folder_docs_name = MINIO_FOLDER_DOCS_NAME_MANUAL  # <================================= Выбери папку, в которую будет записываться инфа (Справочник)

name_of_bucket_minio = MINIO_BUCKET_NAME

milvus_collection_base = "Docs"  # <============== Название коллекции в Milvus


# name_documents = "Simrad Autopilot System AP70, AP80 Installation Manual"  # <============== Описание коллекции milvus

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


# Функция сохраняет исходный файл в MiniO
def save_table_to_minio(
    bucket_name, description_milvus_collection, path_to_save_manuals
):
    """
    Сохраняет файл, соответствующий значению переменной description_milvus_collection, в MinIO.

    Args:
        bucket_name (str): Название бакета в MinIO.
        description_milvus_collection (str): Имя файла для поиска и сохранения.
    """
    try:
        # Генерируем полный путь к файлу
        file_path = os.path.join(DOCX_DIRECTORY, description_milvus_collection)

        # Проверяем, существует ли файл
        if not os.path.exists(file_path):
            raise FileNotFoundError(
                f"Файл {description_milvus_collection} не найден в {DOCX_DIRECTORY}."
            )

        # Открываем файл и читаем его содержимое
        with open(file_path, "rb") as file:
            file_data = file.read()

        # Генерируем ключ для сохранения в MinIO
        minio_key = f"{minio_folder_docs_name}/{description_milvus_collection}"

        content_type = (
            "application/pdf"
            if description_milvus_collection.lower().endswith(".pdf")
            else "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        )

        # Сохраняем файл в MinIO
        s3_client.put_object(
            Bucket=bucket_name,
            Key=minio_key,
            Body=file_data,
            ContentType=content_type,
            ContentDisposition="inline",  # Указывает браузеру открывать файл, а не скачивать
        )

        logger.info(
            f"Файл {description_milvus_collection} успешно сохранён в MinIO под ключом {minio_key}."
        )
        move_file(description_milvus_collection, path_to_save_manuals)
        print(f"Коллекция '{description_milvus_collection}' загружена в MiniO")

    except Exception as e:
        logger.info(
            f"Ошибка при сохранении файла {description_milvus_collection} в MinIO: {e}"
        )


def send_telegram_complite_message():
    """
    Отправляет сообщение об ошибке в Telegram перед завершением программы.

    Args:
        bot_token (str): Токен Telegram-бота.
        chat_id (str): Идентификатор чата (или пользователя), куда отправить сообщение.
        error_message (str): Сообщение об ошибке для отправки.

    Raises:
        Exception: Если возникла ошибка при отправке сообщения.
    """
    try:
        conn = http.client.HTTPSConnection("api.telegram.org")

        payload = '\n{\n  "chat_id": "5746497552",\n  "text": "Все документы загружены"\n}'.encode(
            "utf-8"
        )

        headers = {"Content-Type": "application/json"}

        conn.request(
            "POST",
            "/bot7219050865:AAFuYYrlMdyNTOd2Ffy83sFY-byESBF7hwQ/sendMessage",
            payload,
            headers,
        )

        res = conn.getresponse()
        data = res.read()

        print(data.decode("utf-8"))

    except Exception as e:
        print(f"Не удалось отправить сообщение в Telegram: {e}")


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
    # print(f"Метод process_docx_file запустился для {docx_file}")

    # Работа с Milvus

    name_documents = os.path.splitext(docx_file)[0]
    path_of_doc_for_convert = os.path.join(DOCX_DIRECTORY, docx_file)
    description_milvus_collection = name_documents + end_name_docs

    # Загрузка файла в MinIO

    print(
        "---------------------------------------------------------------------------------------------------"
    )
    print(f"{description_milvus_collection} начало загрузки в MiniO")
    # print("Список всех мануалов")
    # print(description_milvus_collection)
    save_table_to_minio(
        name_of_bucket_minio, description_milvus_collection, path_to_save_manuals
    )  # <===================== Метод загрузки исходника в MiniO
    # move_file(description_milvus_collection, path_to_save_manuals)
    # print(f"Коллекция '{description_milvus_collection}' загружена в MiniO")


def main():
    # Создаем подключение один раз
    try:

        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name=MINIO_REGION_NAME,
        )
    except Exception as e:
        print(f"Ошибка подключения к MiniO: {e}")

    # Передаем подключение в потоки
    with ThreadPoolExecutor(
        max_workers=12
    ) as executor:  # <============= max_workers - количество потоков
        executor.map(
            lambda docx_file: process_docx_file(docx_file, s3_client, DOCX_DIRECTORY),
            docx_files,
        )
    send_telegram_complite_message()


if __name__ == "__main__":
    main()


print(f"Все коллекции загружены.")
