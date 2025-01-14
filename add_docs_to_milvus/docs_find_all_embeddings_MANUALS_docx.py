import logging
import asyncio
from ast import Index
import re
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

# =================================================

DOCX_DIRECTORY = r"C:\Project1\GITProjects\Мануалы\ИНТЕРНЕТ от МЕГАФОН"  # <================= Путь к файлам docx

# Количество коллекций + 8
count_collection_starts = 227  # <===================== Номер коллекцииы
count_collection_ends = count_collection_starts

end_name_docs = ".pdf"  # <============ Конец имени исходного файла, названия коллекции

# =================================================

docx_files = [file for file in os.listdir(DOCX_DIRECTORY) if file.endswith(".docx")]
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

# Подключение к Milvus
connections.connect(
    alias="default", host=MILVUS_HOST, port=MILVUS_PORT, db_name=change_db_of_milvus
)

"""# Создание бакета MinIO, если он не существует
if s3_client.list_buckets().get("Buckets", None):
    existing_buckets = [
        bucket["Name"] for bucket in s3_client.list_buckets()["Buckets"] # <=================== пока сохранения таблиц и рисунков не требуется
    ]
    if name_of_bucket_minio not in existing_buckets:
        s3_client.create_bucket(Bucket=name_of_bucket_minio)"""

# Создание коллекции Milvus (если её нет)


# Функция обрабатывает данные из Word, создает эмбеддинги и сохраняет все в Milvus
def process_content_from_word(word_path, bucket_name, batch_size=50):
    """Обрабатывает текст, таблицы и изображения из Word файла и сохраняет в Milvus."""
    successful_embeddings_count = 0
    text_blocks_with_refs, full_text = extract_content_from_word(word_path, bucket_name)
    text_blocks = split_text_logically(full_text)

    # Данные для батч-вставки
    embeddings_batch = []
    texts_batch = []
    references_batch = []
    figure_ids_batch = []
    related_tables_batch = []

    # **Добавление имени коллекции в Milvus как первый элемент**
    collection_name_block = description_milvus_collection  # Имя коллекции
    collection_name_embedding = create_embeddings(collection_name_block)
    if collection_name_embedding:
        embeddings_batch.append(
            np.array(collection_name_embedding, dtype=np.float32).tolist()
        )
        texts_batch.append(collection_name_block)
        references_batch.append("")
        figure_ids_batch.append("")
        related_tables_batch.append("")
        successful_embeddings_count += 1

    # Обрабатываем текстовые блоки
    for block in text_blocks:
        if block and block.strip():  # Проверка, чтобы блок текста не был пустым
            embedding = create_embeddings(block)
            if embedding is None:
                continue
            embeddings_batch.append(np.array(embedding, dtype=np.float32).tolist())
            texts_batch.append(block)
            references_batch.append("")
            figure_ids_batch.append("")
            related_tables_batch.append("")
            successful_embeddings_count += 1

            # Если размер батча достиг предела, вставляем его
            if len(embeddings_batch) >= batch_size:
                insert_batch_to_milvus(
                    embeddings_batch,
                    texts_batch,
                    references_batch,
                    figure_ids_batch,
                    related_tables_batch,
                )
                (
                    embeddings_batch,
                    texts_batch,
                    references_batch,
                    figure_ids_batch,
                    related_tables_batch,
                ) = ([], [], [], [], [])

    # Обрабатываем метаинформацию (ссылки, рисунки, таблицы)
    for ref_info in text_blocks_with_refs:
        text = ref_info["text"]
        reference = ref_info["reference"]
        figure_id = ref_info["figure_id"]
        related_table = ref_info["related_table"]
        if text and text.strip():
            embedding = create_embeddings(text)
            if embedding is None:
                continue
            embeddings_batch.append(np.array(embedding, dtype=np.float32).tolist())
            texts_batch.append(text)
            references_batch.append(reference)
            figure_ids_batch.append(figure_id)
            related_tables_batch.append(related_table)
            successful_embeddings_count += 1

            # Если размер батча достиг предела, вставляем его
            if len(embeddings_batch) >= batch_size:
                insert_batch_to_milvus(
                    embeddings_batch,
                    texts_batch,
                    references_batch,
                    figure_ids_batch,
                    related_tables_batch,
                )
                (
                    embeddings_batch,
                    texts_batch,
                    references_batch,
                    figure_ids_batch,
                    related_tables_batch,
                ) = ([], [], [], [], [])

    # Вставляем оставшиеся данные, если батч не пустой
    if embeddings_batch:
        insert_batch_to_milvus(
            embeddings_batch,
            texts_batch,
            references_batch,
            figure_ids_batch,
            related_tables_batch,
        )

    collection.flush()
    print(f"Количество успешно созданных эмбеддингов: {successful_embeddings_count}")


def insert_batch_to_milvus(embeddings, texts, references, figure_ids, related_tables):
    """Вставляет батч данных в Milvus."""
    data = [embeddings, texts, references, figure_ids, related_tables]
    collection.insert(data)
    print(f"Батч из {len(embeddings)} записей успешно добавлен.")


# Функция создает эмбеддинги ко всему тексту (описание рисунков, текста таблиц, любого текста)
def create_embeddings(text, pause_duration=0.5):
    """Создает эмбеддинг текста с помощью OpenAI."""
    if not text.strip():
        return None
    try:
        num_tokens = count_tokens(text)
        print(f"Количество токенов в тексте: {num_tokens}")
        response = openai.embeddings.create(
            input=[text], model="text-embedding-ada-002"
        )
        # time.sleep(pause_duration) # <================================== Тайм Слип для обхода ограничений на кол-во запросов
        return response.data[0].embedding
    except Exception as e:
        print(f"Ошибка при создании эмбеддинга: {e}")
        return None


# Подсчет токенов какого-то отрывка текста
def count_tokens(text, model="text-embedding-ada-002"):
    """
    Подсчитывает количество токенов в тексте для указанной модели OpenAI.

    Args:
        text (str): Текст, для которого нужно посчитать токены.
        model (str): Название модели OpenAI (по умолчанию text-embedding-ada-002).

    Returns:
        int: Количество токенов в тексте.
    """
    encoding = tiktoken.encoding_for_model(model)
    tokens = encoding.encode(text)
    return len(tokens)


# Пример использования
text = "Это пример текста для подсчета токенов."
num_tokens = count_tokens(text)
print(f"Количество токенов: {num_tokens}")


def split_text_logically(text):
    """
    Разделяет текст на логические блоки по 500 символов, соблюдая границы абзацев.
    """
    paragraphs = text.split("\n")  # Разделяем текст на абзацы
    logical_blocks = []  # Список для хранения логических блоков
    current_block = ""  # Текущий блок текста

    for paragraph in paragraphs:
        paragraph = paragraph.strip()  # Убираем лишние пробелы
        if not paragraph:
            continue  # Пропускаем пустые абзацы

        # Если добавление текущего абзаца не превышает 500 символов, добавляем его
        if len(current_block) + len(paragraph) + 1 <= 500:  # +1 для пробела/разделителя
            current_block += paragraph + " "
        else:
            # Если текущий блок превышает 500 символов, сохраняем его и начинаем новый
            logical_blocks.append(current_block.strip())
            current_block = paragraph + " "

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


# Функция сохраняет исходный файл в MiniO
def save_table_to_minio(bucket_name, description_milvus_collection):
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

        # Сохраняем файл в MinIO
        s3_client.put_object(
            Bucket=bucket_name,
            Key=minio_key,
            Body=file_data,
            ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ContentDisposition="inline",  # Указывает браузеру открывать файл, а не скачивать
        )

        logger.info(
            f"Файл {description_milvus_collection} успешно сохранён в MinIO под ключом {minio_key}."
        )
    except Exception as e:
        logger.info(
            f"Ошибка при сохранении файла {description_milvus_collection} в MinIO: {e}"
        )


# Функция сохраняет все рисунки из документа как JPEG
def save_image_to_minio(bucket_name, image_name, image_data):
    """Сохраняет изображение в MinIO как JPEG файл и возвращает имя файла."""
    buffer = BytesIO()
    image_data = image_data.convert(
        "RGB"
    )  # Конвертируем изображение в RGB перед сохранением
    image_data.save(buffer, format="JPEG")
    s3_client.put_object(
        Bucket=bucket_name,
        Key=f"{minio_folder_docs_name}/{image_name}",
        Body=buffer.getvalue(),
        ContentType="image/jpeg",  # MIME-тип изображения
        ContentDisposition="inline",  # Указывает браузеру открывать файл, а не скачивать
    )
    print(f"Изображение загружено в MinIO как {image_name}")
    return image_name  # Возвращаем имя файла вместо ссылки


# Функция для извлечения "Рисунок Х" из текста
def extract_figure_id(text):
    match = re.search(r"(Рисунок \d+)", text)
    if match:
        return match.group(1)
    return ""


# Функция для извлечения "Таблица Х" из названия таблицы
def extract_table_id(text):
    """
    Извлекает идентификатор таблицы в формате 'Таблица X' из текста.
    Если идентификатор не найден, возвращает пустую строку.
    """
    match = re.search(r"(Таблица \d+)", text, re.IGNORECASE)
    return match.group(1) if match else ""


# Функция обрабатывает Word документ, извлекая таблицы, текст, изображения из документа и сохраняя в MiniO
def extract_content_from_word(word_path, bucket_name):
    """Извлекает текст, таблицы и изображения из Word файла, избегая дубликатов."""
    doc = Document(word_path)
    text_blocks_with_refs = []
    current_text_block = []
    current_table_data = []
    table_counter, image_counter = 1, 1
    last_was_table = False
    saved_images = set()  # Набор для отслеживания сохраненных изображений

    # Обработка текста и таблиц
    for idx, block in enumerate(doc.element.body):
        if block.tag.endswith("p"):  # Обработка параграфов
            paragraph = block.text.strip()
            if paragraph:
                if last_was_table and current_table_data:
                    # Сохраняем текущую собранную таблицу в MinIO как одну таблицу
                    table_name = f"table_{table_counter}"
                    table_name_xlsx = f"{table_name}.xlsx"
                    # save_table_to_minio(bucket_name, table_name, current_table_data)                            <=============== Тут сохраняли таблицы в MiniO

                    # Сохраняем описание таблицы
                    explanation = current_text_block[-1] if current_text_block else ""

                    table_id = extract_table_id(explanation)

                    text_blocks_with_refs.append(
                        {
                            "text": explanation,
                            "reference": "",
                            "figure_id": table_id,
                            "related_table": "",
                        }
                    )
                    # Обрабатываем текст из таблицы и сохраняем в Milvus
                    table_text_blocks = split_table_text_logically(current_table_data)
                    for block in table_text_blocks:
                        text_blocks_with_refs.append(
                            {
                                "text": block,
                                "reference": "",
                                "figure_id": "",
                                "related_table": table_name_xlsx,
                            }
                        )
                    current_table_data = []  # Сброс текущих данных таблицы
                    table_counter += 1
                current_text_block.append(paragraph)
                last_was_table = False

        elif block.tag.endswith("tbl"):  # Обработка таблиц
            table = next(t for t in doc.tables if t._tbl == block)
            table_data = [
                [cell.text.strip() for cell in row.cells] for row in table.rows
            ]

            # Объединяем таблицы, если они идут подряд
            if last_was_table:
                current_table_data.extend(table_data)
            else:
                current_table_data = table_data

            # Обработка изображений внутри таблиц
            for row in table.rows:
                for cell in row.cells:
                    for paragraph_index, paragraph in enumerate(cell.paragraphs):
                        for run in paragraph.runs:
                            if run.element.xpath(".//a:blip"):
                                blip = run.element.xpath(".//a:blip")[0]
                                image_part = doc.part.related_parts[
                                    blip.get(
                                        "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}embed"
                                    )
                                ]
                                # Проверка на дубликат
                                if image_part in saved_images:
                                    continue
                                saved_images.add(image_part)

                                image_data = Image.open(BytesIO(image_part.blob))
                                image_name = f"image_{image_counter}.jpeg"
                                # save_image_to_minio(bucket_name, image_name, image_data)              <============================= Тут сохраняли Рисунки в MiniO

                                # Получаем следующий параграф для описания, если он существует
                                if paragraph_index + 1 < len(cell.paragraphs):
                                    text_after_image = cell.paragraphs[
                                        paragraph_index + 1
                                    ].text.strip()
                                else:
                                    text_after_image = "Описание отсутствует"

                                # Извлекаем "Рисунок Х" из описания, если оно есть
                                figure_id = extract_figure_id(text_after_image)

                                # Добавляем запись с `text`, `reference` и `figure_id`
                                text_blocks_with_refs.append(
                                    {
                                        "text": text_after_image,
                                        "reference": "",
                                        "figure_id": figure_id,
                                        "related_table": f"table_{table_counter}.xlsx",  # Смещение на 1 для таблицы
                                    }
                                )
                                print(
                                    f"Изображение {image_name} загружено с описанием: {text_after_image}, figure_id: {figure_id}, related_table: {table_name_xlsx}"
                                )
                                image_counter += 1
            last_was_table = True

    # Обработка изображений вне таблиц
    paragraphs = iter(doc.paragraphs)
    for paragraph in paragraphs:
        for run in paragraph.runs:
            if run.element.xpath(".//a:blip"):
                # Найдено изображение
                blip = run.element.xpath(".//a:blip")[0]
                image_part = doc.part.related_parts[
                    blip.get(
                        "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}embed"
                    )
                ]
                # Проверка на дубликат
                if image_part in saved_images:
                    continue
                saved_images.add(image_part)

                image_data = Image.open(BytesIO(image_part.blob))
                image_name = f"image_{image_counter}.jpeg"
                # save_image_to_minio(bucket_name, image_name, image_data)               <============================= Тут сохраняли Рисунки в MiniO

                # Ищем текст непосредственно после изображения
                try:
                    next_paragraph = next(paragraphs)
                    text_after_image = (
                        next_paragraph.text.strip()
                        if next_paragraph.text.strip()
                        else "Описание отсутствует"
                    )
                except StopIteration:
                    text_after_image = "Описание отсутствует"

                # Извлекаем "Рисунок Х" для сохранения в поле figure_id
                figure_id = extract_figure_id(text_after_image)

                text_blocks_with_refs.append(
                    {
                        "text": text_after_image,
                        "reference": "",
                        "figure_id": figure_id,
                        "related_table": "",  # Поле остаётся пустым
                    }
                )
                print(
                    f"Изображение {image_name} загружено с описанием: {text_after_image} и 'figure_id': {figure_id}"
                )
                image_counter += 1

    return text_blocks_with_refs, " ".join(current_text_block)


# Перебор всех файлов .docx
for docx_file in docx_files:
    # Сохраняем имя файла без расширения
    name_documents = os.path.splitext(docx_file)[0]

    # Генерируем полный путь к файлу
    path_of_doc_for_convert = os.path.join(DOCX_DIRECTORY, docx_file)

    # Описание коллекции
    description_milvus_collection = name_documents + end_name_docs

    # Название коллекции
    milvus_collection = f"{milvus_collection_base}{count_collection_ends}"

    collection_name = milvus_collection
    if not utility.has_collection(collection_name):
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=1536),
            FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="reference", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="figure_id", dtype=DataType.VARCHAR, max_length=100),
            FieldSchema(name="related_table", dtype=DataType.VARCHAR, max_length=65535),
        ]
        schema = CollectionSchema(fields, description=description_milvus_collection)
        collection = Collection(name=collection_name, schema=schema)
    else:
        collection = Collection(name=collection_name)

    # Загрузка модели spaCy
    nlp = spacy.load("ru_core_news_lg")

    # Пример использования
    word_path = path_of_doc_for_convert
    process_content_from_word(word_path, name_of_bucket_minio)

    # Создание и загрузка индекса в Milvus
    index_params = {
        "index_type": "IVF_FLAT",
        "metric_type": "L2",
        "params": {"nlist": 128},
    }
    collection.create_index(field_name="embedding", index_params=index_params)
    collection.load()
    save_table_to_minio(name_of_bucket_minio, description_milvus_collection)

    count_collection_ends += 1
    print(
        "---------------------------------------------------------------------------------------------------"
    )
    print(
        f"Индекс успешно создан и коллекция '{collection_name}' загружена в БД '{change_db_of_milvus}' по счету {count_collection_ends - count_collection_starts}"
    )
    print(
        "---------------------------------------------------------------------------------------------------"
    )


print(f"Все коллекции загружены.")
