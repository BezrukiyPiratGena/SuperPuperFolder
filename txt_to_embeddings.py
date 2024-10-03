import openai
import os
import json
from dotenv import load_dotenv

# Загружаем переменные из файла tokens.env
load_dotenv("tokens.env")

# Берем ключ OpenAI API из переменной окружения
openai.api_key = os.getenv("OPENAI_API_KEY")


def create_embeddings(text):
    """
    Преобразует текст в эмбеддинг с помощью OpenAI и сохраняет его в файл.

    :param text: Текст, который нужно преобразовать в эмбеддинг.
    :return: Эмбеддинг в виде вектора (списка чисел).
    """
    response = openai.embeddings.create(
        input=[text],
        model="text-embedding-ada-002",
    )
    embeddings = response.data[0].embedding
    return embeddings


def save_embeddings_to_file(text, embeddings, part_number):
    """
    Сохраняет часть текста и его эмбеддинг в JSON файл.

    :param text: Исходный текст.
    :param embeddings: Эмбеддинг текста.
    :param part_number: Номер части текста, если он был разделен.
    """
    data = {"text_part": f"Part {part_number}", "text": text, "embedding": embeddings}

    # Сохраняем в файл JSON
    with open("embeddings.json", "a", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        f.write("\n")


def process_large_text_from_file(file_path, max_chunk_size=2000):
    """
    Открывает текстовый файл, разделяет его на части и создает эмбеддинги для каждой части.

    :param file_path: Путь к текстовому файлу.
    :param max_chunk_size: Максимальный размер части (в символах).
    """
    # Открываем файл и читаем его содержимое
    with open(file_path, "r", encoding="utf-8") as file:
        large_text = file.read()

    # Разделяем текст на части, если он слишком большой
    text_parts = [
        large_text[i : i + max_chunk_size]
        for i in range(0, len(large_text), max_chunk_size)
    ]

    # Для каждой части текста создаем эмбеддинг и сохраняем его
    for i, part in enumerate(text_parts, 1):
        embedding = create_embeddings(part)
        save_embeddings_to_file(part, embedding, i)


# Пример использования
file_path = r"C:\Project1\GITProjects\myproject2\extracted_text.txt"
process_large_text_from_file(file_path)
