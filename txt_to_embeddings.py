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
collection_name = "text_embeddings_all"

if not utility.has_collection(collection_name):
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(
            name="embedding", dtype=DataType.FLOAT_VECTOR, dim=1536
        ),  # Размер эмбеддинга для ada-002
        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
    ]
    schema = CollectionSchema(
        fields, description="Коллекция для хранения текстов и эмбеддингов все части"
    )
    collection = Collection(name=collection_name, schema=schema)
else:
    collection = Collection(name=collection_name)


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


def process_large_text_from_file(file_path, max_chunk_size=1000):
    """
    Открывает текстовый файл, разделяет его на части и создает эмбеддинги для каждой части,
    после чего сохраняет их в Milvus.

    :param file_path: Путь к текстовому файлу.
    :param max_chunk_size: Максимальный размер части текста.
    """
    # Открываем файл и читаем его содержимое
    with open(file_path, "r", encoding="utf-8") as file:
        large_text = file.read()

    # Разделяем текст на части
    text_parts = [
        large_text[i : i + max_chunk_size]
        for i in range(0, len(large_text), max_chunk_size)
    ]

    # Для каждой части текста создаем эмбеддинг и сохраняем его в Milvus
    for i, part in enumerate(text_parts, 1):
        embedding = create_embeddings(part)

        # Преобразуем эмбеддинг в формат numpy и затем в список для добавления в Milvus
        embedding_np = np.array(embedding, dtype=np.float32).tolist()

        # Вставляем эмбеддинг и текст в коллекцию
        data = [[embedding_np], [part]]
        collection.insert(data)

        print(f"Эмбеддинг и текст успешно добавлены для части {i}.")
    collection.flush()
    print("Все эмбеддинги и тексты успешно добавлены в Milvus.")


# Пример использования
file_path = r"C:\Project1\GITProjects\myproject2\extracted_text.txt"
process_large_text_from_file(file_path)

# Подключаемся к Milvus
connections.connect("default", host="localhost", port="19530")

# Указываем название коллекции
collection_name = "text_embeddings_all"
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
