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


def process_large_text_from_file(file_path, output_path, max_chunk_size=1000):
    """
    Открывает текстовый файл, разделяет его на части и создает эмбеддинги для каждой части,
    после чего сохраняет все части в JSON файл.

    :param file_path: Путь к текстовому файлу.
    :param output_path: Путь, по которому нужно сохранить итоговый JSON файл.
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

    embeddings_data = []  # Список для хранения всех эмбеддингов и частей текста

    # Для каждой части текста создаем эмбеддинг и сохраняем его в список
    for i, part in enumerate(text_parts, 1):
        embedding = create_embeddings(part)
        data = {
            "text_part": f"Part {i}",
            "text": part,
            "embedding": embedding,
        }
        embeddings_data.append(data)  # Добавляем объект в список

    # Записываем весь список данных в JSON файл
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(embeddings_data, f, ensure_ascii=False, indent=4)

    print(f"Все эмбеддинги успешно сохранены в файл: {output_path}")


# Пример использования
file_path = r"C:\Project1\GITProjects\myproject2\extracted_text.txt"
output_path = r"C:\Project1\GITProjects\myproject2\docs\ready\embeddings.json"
process_large_text_from_file(file_path, output_path)
