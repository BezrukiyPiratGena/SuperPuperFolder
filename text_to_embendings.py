import openai
import numpy as np
import faiss
import os
import tiktoken  # Импортируем библиотеку для работы с токенами

# Путь к папке с текстовыми файлами
directory = r"C:\Project1\GITProjects\myproject"

# Инициализация токенизатора для модели text-embedding-ada-002
tokenizer = tiktoken.get_encoding("cl100k_base")

# Лимит токенов для модели text-embedding-ada-002
MAX_TOKENS = 8192


# Функция для разбиения текста на части по токенам
def split_text_by_tokens(text, tokenizer, max_tokens=MAX_TOKENS):
    tokens = tokenizer.encode(text)  # Токенизируем текст
    chunks = [
        tokens[i : i + max_tokens] for i in range(0, len(tokens), max_tokens)
    ]  # Разбиваем на части
    print(f"Текст разбит на {len(chunks)} чанков.")  # Логируем количество чанков
    return [
        tokenizer.decode(chunk) for chunk in chunks
    ]  # Декодируем токены обратно в текст


# Функция для создания эмбеддингов
def get_embeddings(text):
    response = openai.embeddings.create(input=text, model="text-embedding-ada-002")
    # Логирование длины текста и длины эмбеддингов
    print(f"Создание эмбеддинга для чанка длиной {len(text)} символов.")
    embedding = response.data[0].embedding
    return np.array(embedding, dtype="float32")


# Инициализация FAISS индекса
dimension = 1536  # Размер эмбеддингов OpenAI
index = faiss.IndexFlatL2(dimension)  # Индекс для поиска по L2-норме


# Обработка текстовых файлов и создание эмбеддингов
def create_embeddings_for_text_files(directory):
    for filename in os.listdir(directory):
        if filename.endswith("_output.txt"):
            file_path = os.path.join(directory, filename)
            with open(file_path, "r", encoding="utf-8") as file:
                text = file.read()

            # Разбиение текста на части по токенам
            text_chunks = split_text_by_tokens(text, tokenizer, max_tokens=MAX_TOKENS)

            # Создание эмбеддингов для каждой части текста
            for chunk_num, chunk in enumerate(text_chunks, start=1):
                embedding = get_embeddings(chunk)
                index.add(np.array([embedding]))  # Добавляем эмбеддинг в FAISS индекс
                print(f"Эмбеддинг для чанка {chunk_num} добавлен в индекс.")

            print(f"Эмбеддинг(и) созданы для файла: {filename}")

    # Сохранение индекса FAISS
    faiss.write_index(index, "index.faiss")
    print("Индекс FAISS сохранён.")


# Запуск создания эмбеддингов для всех текстовых файлов
create_embeddings_for_text_files(directory)
