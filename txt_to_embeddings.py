import spacy
from pymilvus import Index
import openai
import os
import numpy as np
from dotenv import load_dotenv
from pymilvus import (
    connections,
    FieldSchema,
    CollectionSchema,
    DataType,
    Collection,
    utility,
)

# Загружаем переменные из файла tokens.env
load_dotenv("tokens.env")
openai.api_key = os.getenv("OPENAI_API_KEY")

# Подключение к Milvus
connections.connect("default", host="localhost", port="19530")

# Определяем коллекцию, если её ещё нет
collection_name = "text_emb_lg_500"

if not utility.has_collection(collection_name):
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(
            name="embedding", dtype=DataType.FLOAT_VECTOR, dim=1536
        ),  # Размер эмбеддинга для ada-002
        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
    ]
    schema = CollectionSchema(
        fields,
        description="Коллекция для хранения текстов и эмбеддингов всех тестовых частей",
    )
    collection = Collection(name=collection_name, schema=schema)
else:
    collection = Collection(name=collection_name)

# Загружаем модель spaCy для разбиения текста на логические блоки
nlp = spacy.load("ru_core_news_lg")


def create_embeddings(text):
    """
    Преобразует текст в эмбеддинг с помощью OpenAI.

    :param text: Текст, который нужно преобразовать в эмбеддинг.
    :return: Эмбеддинг в виде вектора (списка чисел).
    """
    response = openai.embeddings.create(
        input=[text],
        model="text-embedding-ada-002",
    )
    embedding = response.data[0].embedding
    return embedding


def split_text_logically(text):
    """
    Разбивает текст на логические блоки с использованием spaCy.

    :param text: Полный текст для разбивки.
    :return: Список логических блоков.
    """
    doc = nlp(text)
    logical_blocks = []
    current_block = []

    for sent in doc.sents:  # Разделение на предложения
        current_block.append(sent.text)

        # Условие для завершения блока - можно добавить и другие условия
        if len(" ".join(current_block)) > 500:  # Лимит в символах для одного блока
            logical_blocks.append(" ".join(current_block))
            current_block = []  # Начинаем новый блок

    if current_block:  # Добавляем последний блок, если он не пуст
        logical_blocks.append(" ".join(current_block))

    return logical_blocks


def process_large_text_from_file(file_path):
    """
    Открывает текстовый файл, разбивает его на логические блоки и создает эмбеддинги для каждого блока,
    после чего сохраняет их в Milvus.

    :param file_path: Путь к текстовому файлу.
    """
    # Открываем файл и читаем его содержимое
    with open(file_path, "r", encoding="utf-8") as file:
        large_text = file.read()

    # Разбиваем текст на логические блоки с помощью spaCy
    text_blocks = split_text_logically(large_text)

    # Для каждого логического блока создаем эмбеддинг и сохраняем его в Milvus
    for i, block in enumerate(text_blocks, 1):
        embedding = create_embeddings(block)

        # Преобразуем эмбеддинг в формат numpy и затем в список для добавления в Milvus
        embedding_np = np.array(embedding, dtype=np.float32).tolist()

        # Вставляем эмбеддинг и текст в коллекцию
        data = [[embedding_np], [block]]
        collection.insert(data)

        print(f"Эмбеддинг и текст успешно добавлены для блока {i}.")
    collection.flush()
    print("Все эмбеддинги и тексты успешно добавлены в Milvus.")


# Пример использования
file_path = r"C:\Project1\GITProjects\myproject2\extracted_text.txt"
process_large_text_from_file(file_path)

# Подключаемся к Milvus
connections.connect("default", host="localhost", port="19530")

# Указываем название коллекции
collection_name = "text_emb_lg_500"
collection = Collection(name=collection_name)

# Определяем параметры индекса
index_params = {
    "index_type": "IVF_FLAT",  # Или "IVF_SQ8", или другой подходящий тип
    "metric_type": "L2",  # Метрика расстояния, например, L2 или IP (Inner Product)
    "params": {
        "nlist": 128
    },  # Количество списков для индекса (подбирается экспериментально)
}

# Создаем индекс
index = Index(collection, field_name="embedding", index_params=index_params)

# После создания индекса, загружаем коллекцию
collection.load()

print(f"Индекс успешно создан и коллекция '{collection_name}' загружена.")
