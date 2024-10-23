import docx
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
from docx import Document  # Библиотека для работы с Word документами
import csv

# Загрузка переменных среды
load_dotenv("tokens.env")
openai.api_key = os.getenv("OPENAI_API_KEY")

# Подключение к Milvus
connections.connect("default", host="localhost", port="19530")

# Создаем коллекцию Milvus (если её нет)
collection_name = "Eng_lg_500_word_tables"
if not utility.has_collection(collection_name):
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(
            name="embedding", dtype=DataType.FLOAT_VECTOR, dim=1536
        ),  # Размер эмбеддинга для ada-002
        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(
            name="table_reference", dtype=DataType.VARCHAR, max_length=65535
        ),  # Ссылка на таблицу
    ]
    schema = CollectionSchema(
        fields,
        description="Коллекция для хранения текстов, эмбеддингов и ссылок на таблицы",
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
    if not text.strip():  # Проверка на пустой текст
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

    for sent in doc.sents:  # Разделение на предложения
        current_block.append(sent.text)

        if len(" ".join(current_block)) > 500:  # Лимит в символах для одного блока
            logical_blocks.append(" ".join(current_block))
            current_block = []  # Начинаем новый блок

    if current_block:  # Добавляем последний блок, если он не пуст
        logical_blocks.append(" ".join(current_block))

    return logical_blocks


def extract_text_and_tables_from_word(word_path, output_dir):
    """
    Извлекает текст и таблицы из Word файла. Сохраняет таблицы как CSV файлы и
    возвращает список текстовых блоков и ссылок на таблицы.
    """
    doc = Document(word_path)
    text_blocks_with_tables = []
    current_text_block = []
    current_table_data = []
    previous_table_data = []
    table_counter = 1
    last_was_table = False  # Флаг для проверки, была ли предыдущая структура таблицей

    for idx, block in enumerate(doc.element.body):
        if block.tag.endswith("p"):  # Если это параграф (текст)
            paragraph = block.text.strip()
            if paragraph:
                if last_was_table and previous_table_data:
                    # Сохраняем предыдущую таблицу, так как после нее идет текст
                    csv_file_path = f"{output_dir}/table_{table_counter}.csv"
                    save_table_to_csv(csv_file_path, previous_table_data)
                    explanation = current_text_block[-1] if current_text_block else ""
                    text_blocks_with_tables.append(
                        {"text": explanation, "table_reference": csv_file_path}
                    )
                    print(f"Таблица {table_counter} сохранена в {csv_file_path}")
                    previous_table_data = []  # Очищаем предыдущие данные
                    table_counter += 1

                current_text_block.append(paragraph)
                last_was_table = False  # Это не таблица

        elif block.tag.endswith("tbl"):  # Если это таблица
            # Добавляем текущую таблицу к предыдущим, если это необходимо
            table = next(t for t in doc.tables if t._tbl == block)
            current_table_data = [
                [cell.text.strip() for cell in row.cells] for row in table.rows
            ]

            if last_was_table:
                # Если предыдущая структура тоже была таблицей, объединяем текущую с предыдущей
                previous_table_data.extend(current_table_data)
            else:
                previous_table_data = (
                    current_table_data  # Запоминаем таблицу для объединения
                )

            last_was_table = True  # Устанавливаем флаг

    # Сохраняем последнюю таблицу, если после нее нет текста
    if last_was_table and previous_table_data:
        csv_file_path = f"{output_dir}/table_{table_counter}.csv"
        save_table_to_csv(csv_file_path, previous_table_data)
        explanation = current_text_block[-1] if current_text_block else ""
        text_blocks_with_tables.append(
            {"text": explanation, "table_reference": csv_file_path}
        )
        print(f"Таблица {table_counter} сохранена в {csv_file_path}")

    return text_blocks_with_tables, " ".join(
        current_text_block
    )  # Возвращаем также весь текст


def save_table_to_csv(csv_file_path, table_data):
    """
    Сохраняет данные таблицы в CSV файл.
    """
    with open(csv_file_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        for row_data in table_data:
            writer.writerow(row_data)


def process_large_text_and_tables_from_word(word_path, output_dir):
    """
    Обрабатывает Word документ: извлекает текст и таблицы, создает эмбеддинги и сохраняет их в Milvus.
    """
    # Извлекаем текст и таблицы из Word файла
    text_blocks_with_tables, full_text = extract_text_and_tables_from_word(
        word_path, output_dir
    )

    # Сохраняем логические блоки текста как раньше
    text_blocks = split_text_logically(full_text)
    for block in text_blocks:
        embedding = create_embeddings(block)
        if embedding is None:
            continue  # Пропускаем пустые блоки

        embedding_np = np.array(embedding, dtype=np.float32).tolist()
        # Сохраняем текст и эмбеддинг в Milvus без ссылок на таблицы
        data = [[embedding_np], [block], [""]]
        collection.insert(data)
        print(f"Эмбеддинг и текст успешно добавлены для блока.")

    # Сохраняем таблицы с пояснением (одно предложение)
    for i, block_info in enumerate(text_blocks_with_tables, 1):
        text = block_info["text"]
        table_reference = block_info["table_reference"]

        # Создаем эмбеддинг для пояснения к таблице
        embedding = create_embeddings(text)
        if embedding is None:
            continue  # Пропускаем пустые блоки

        embedding_np = np.array(embedding, dtype=np.float32).tolist()
        # Сохраняем пояснение и ссылку на таблицу в Milvus
        data = [[embedding_np], [text], [table_reference]]
        collection.insert(data)
        print(
            f"Эмбеддинг и пояснение успешно добавлены для таблицы. Ссылка на таблицу: {table_reference}"
        )

    collection.flush()
    print("Все эмбеддинги и тексты успешно добавлены в Milvus.")


# Пример использования
word_path = r"C:\Project1\GITProjects\myproject2\example_full.docx"
output_dir = r"C:\Project1\Документы для обучения GPT\Тестовые данные"
process_large_text_and_tables_from_word(word_path, output_dir)

# Определяем параметры индекса
index_params = {
    "index_type": "IVF_FLAT",
    "metric_type": "L2",
    "params": {"nlist": 128},
}

# Создаем индекс
index = Index(collection, field_name="embedding", index_params=index_params)

# Загружаем коллекцию
collection.load()

print(f"Индекс успешно создан и коллекция '{collection_name}' загружена.")
