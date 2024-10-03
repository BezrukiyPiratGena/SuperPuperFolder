import os
import docx
import pandas as pd
import PyPDF2

# Путь к папке с файлами
directory = r"C:\Project1\GITProjects\myproject"


# Функция для извлечения текста из PDF
def extract_text_from_pdf(file_path):
    with open(file_path, "rb") as file:
        reader = PyPDF2.PdfReader(file)
        text = []
        for page_num in range(len(reader.pages)):
            page = reader.pages[page_num]
            text.append(page.extract_text())
    return " ".join(text)


# Функция для извлечения текста из Word
def extract_text_from_docx(file_path):
    doc = docx.Document(file_path)
    return " ".join([paragraph.text for paragraph in doc.paragraphs])


# Функция для извлечения данных из Excel
def extract_text_from_excel(file_path):
    df = pd.read_excel(file_path)
    return df.to_string()


# Функция для сохранения текста в файл
def save_text_to_file(text, output_file):
    with open(output_file, "w", encoding="utf-8") as file:
        file.write(text)
    print(f"Текст сохранён в файл: {output_file}")


# Функция для обработки всех файлов в папке
def process_files_in_directory(directory):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        output_file_path = os.path.join(
            directory, filename + "_output.txt"
        )  # Путь для сохранения текста

        if filename.endswith(".pdf"):
            print(f"Извлечение текста из PDF файла: {filename}")
            text = extract_text_from_pdf(file_path)
            save_text_to_file(text, output_file_path)

        elif filename.endswith(".docx"):
            print(f"Извлечение текста из Word файла: {filename}")
            text = extract_text_from_docx(file_path)
            save_text_to_file(text, output_file_path)

        elif filename.endswith(".xlsx"):
            print(f"Извлечение текста из Excel файла: {filename}")
            text = extract_text_from_excel(file_path)
            save_text_to_file(text, output_file_path)

        else:
            print(f"Файл {filename} не поддерживается.")


# Запуск обработки файлов в папке
process_files_in_directory(directory)
