from ast import Index
import docx
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
from io import StringIO
import csv

# Загрузка переменных среды
load_dotenv("tokens.env")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # Ключ OpenAI
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")  # Логин Minlo
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")  # Пароль Minlo
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")  # Бакет Minlo
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")  # Адрес и порт для подключения к minlo
MINIO_REGION_NAME = os.getenv("MINIO_REGION_NAME")  # Регион Minlo
MILFUS_HOST = os.getenv("MILFUS_HOST")  # Адрес для подключения к Milfus
MILFUS_PORT = os.getenv("MILFUS_PORT")  # Порт для подключения к Milfus
MILFUS_COLLECTION = os.getenv(
    "MILFUS_COLLECTION"
)  # Коллекция для внесения эмбеддингов в Milfus

# Меняем значения важных переменных
name_of_collection_milfus = MILFUS_COLLECTION
name_of_bucket_miniO = MINIO_BUCKET_NAME
path_of_doc_for_convert = r"C:\Project1\GITProjects\myproject2\example_full.docx"

# Устанавливаем ключ OpenAI API
openai.api_key = OPENAI_API_KEY

# Подключение к Milvus
connections.connect("default", host=MILFUS_HOST, port=MILFUS_PORT)
print(f'Логин "{MINIO_ACCESS_KEY}" для БД MiniO')
print(f'Пароль "{MINIO_SECRET_KEY}" для БД MiniO')

# Подключение к MinIO
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name=MINIO_REGION_NAME,
)

# Создание бакета, если он не существует
bucket_name = name_of_bucket_miniO
if s3_client.list_buckets().get("Buckets", None):
    existing_buckets = [
        bucket["Name"] for bucket in s3_client.list_buckets()["Buckets"]
    ]
    if bucket_name not in existing_buckets:
        s3_client.create_bucket(Bucket=bucket_name)

# Создаем коллекцию Milvus (если её нет)
collection_name = name_of_collection_milfus
if not utility.has_collection(collection_name):
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=1536),
        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="table_reference", dtype=DataType.VARCHAR, max_length=65535),
    ]
    schema = CollectionSchema(
        fields,
        description="Коллекция для хранения инженерского справочника",
    )
    collection = Collection(name=collection_name, schema=schema)
else:
    collection = Collection(name=collection_name)

# Загружаем модель spaCy для разбиения текста на логические блоки
nlp = spacy.load("ru_core_news_lg")


def create_embeddings(text):
    """
    Преобразует текст в эмбеддинг с помощью OpenAI.
    """
    if not text.strip():
        return None
    response = openai.embeddings.create(
        input=[text],
        model="text-embedding-ada-002",
    )
    embedding = response.data[0].embedding
    return embedding


def split_text_logically(text):
    """
    Разбивает текст на логические блоки с использованием spaCy.
    """
    doc = nlp(text)
    logical_blocks = []
    current_block = []

    for sent in doc.sents:
        current_block.append(sent.text)

        if len(" ".join(current_block)) > 500:
            logical_blocks.append(" ".join(current_block))
            current_block = []

    if current_block:
        logical_blocks.append(" ".join(current_block))

    return logical_blocks


def save_table_to_minio(bucket_name, table_name, table_data):
    """
    Сохраняет данные таблицы в MinIO в формате CSV.
    """
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    for row_data in table_data:
        writer.writerow(row_data)

    # Загрузка CSV в MinIO
    s3_client.put_object(Bucket=bucket_name, Key=table_name, Body=csv_buffer.getvalue())


def extract_text_and_tables_from_word(word_path, bucket_name):
    """
    Извлекает текст и таблицы из Word файла. Сохраняет таблицы как CSV файлы в MinIO и
    возвращает список текстовых блоков и ссылок на таблицы.
    """
    doc = Document(word_path)
    text_blocks_with_tables = []
    current_text_block = []
    current_table_data = []
    previous_table_data = []
    table_counter = 1
    last_was_table = False

    for idx, block in enumerate(doc.element.body):
        if block.tag.endswith("p"):
            paragraph = block.text.strip()
            if paragraph:
                if last_was_table and previous_table_data:
                    table_name = f"table_{table_counter}.csv"
                    save_table_to_minio(bucket_name, table_name, previous_table_data)
                    explanation = current_text_block[-1] if current_text_block else ""
                    text_blocks_with_tables.append(
                        {"text": explanation, "table_reference": table_name}
                    )
                    print(f"Таблица {table_counter} загружена в MinIO как {table_name}")
                    previous_table_data = []
                    table_counter += 1

                current_text_block.append(paragraph)
                last_was_table = False

        elif block.tag.endswith("tbl"):
            table = next(t for t in doc.tables if t._tbl == block)
            current_table_data = [
                [cell.text.strip() for cell in row.cells] for row in table.rows
            ]

            if last_was_table:
                previous_table_data.extend(current_table_data)
            else:
                previous_table_data = current_table_data

            last_was_table = True

    if last_was_table and previous_table_data:
        table_name = f"table_{table_counter}.csv"
        save_table_to_minio(bucket_name, table_name, previous_table_data)
        explanation = current_text_block[-1] if current_text_block else ""
        text_blocks_with_tables.append(
            {"text": explanation, "table_reference": table_name}
        )
        print(f"Таблица {table_counter} загружена в MinIO как {table_name}")

    return text_blocks_with_tables, " ".join(current_text_block)


def process_large_text_and_tables_from_word(word_path, bucket_name):
    """
    Обрабатывает Word документ: извлекает текст и таблицы, создает эмбеддинги и сохраняет их в Milvus.
    """
    # Счетчик успешных эмбеддингов
    successful_embeddings_count = 0

    # Извлекаем текст и таблицы из Word файла
    text_blocks_with_tables, full_text = extract_text_and_tables_from_word(
        word_path, bucket_name
    )

    # Сохраняем логические блоки текста
    text_blocks = split_text_logically(full_text)
    for block in text_blocks:
        embedding = create_embeddings(block)
        if embedding is None:
            continue

        embedding_np = np.array(embedding, dtype=np.float32).tolist()
        data = [[embedding_np], [block], [""]]
        collection.insert(data)
        successful_embeddings_count += 1

        print(
            f"Эмбеддинг и текст успешно добавлены для блока {successful_embeddings_count}."
        )

    # Сохраняем таблицы с пояснением
    for i, block_info in enumerate(text_blocks_with_tables, 1):
        text = block_info["text"]
        table_reference = block_info["table_reference"]

        embedding = create_embeddings(text)
        if embedding is None:
            continue

        embedding_np = np.array(embedding, dtype=np.float32).tolist()
        data = [[embedding_np], [text], [table_reference]]
        collection.insert(data)
        successful_embeddings_count += 1
        print(
            f"Эмбеддинг и пояснение успешно добавлены для таблицы. Ссылка на таблицу: {table_reference}"
        )

    collection.flush()
    print("Все эмбеддинги и тексты успешно добавлены в Milvus.")
    print(f"Количество успешно созданных эмбеддингов: {successful_embeddings_count}")


# Пример использования
word_path = path_of_doc_for_convert
process_large_text_and_tables_from_word(word_path, bucket_name)

# Определяем параметры индекса
index_params = {
    "index_type": "IVF_FLAT",
    "metric_type": "L2",
    "params": {"nlist": 128},
}

# Создаем индекс
collection.create_index(field_name="embedding", index_params=index_params)

# Загружаем коллекцию
collection.load()

print(f"Индекс успешно создан и коллекция '{collection_name}' загружена.")
