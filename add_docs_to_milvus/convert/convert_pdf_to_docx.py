import os
from pdf2docx import Converter


def find_pdf_file():
    for file in os.listdir(os.getcwd()):
        if file.lower().endswith(".pdf"):
            return file
    return None


def convert_pdf_to_word(pdf_filename):
    current_directory = os.getcwd()
    pdf_path = os.path.join(current_directory, pdf_filename)
    word_filename = os.path.splitext(pdf_filename)[0] + ".docx"
    word_path = os.path.join(current_directory, word_filename)

    try:
        print(f"Начало конвертации: '{pdf_filename}'...")
        cv = Converter(pdf_path)

        # Конвертация с логированием прогресса
        print("Конвертация в процессе...")
        cv.convert(word_path, start=0, end=None)
        print(f"Файл успешно конвертирован: '{word_filename}'")
    except Exception as e:
        print(f"Ошибка при конвертации: {e}")
    finally:
        cv.close()  # Всегда закрываем ресурс


if __name__ == "__main__":
    pdf_file = find_pdf_file()
    if pdf_file:
        convert_pdf_to_word(pdf_file)
    else:
        print("PDF-файл не найден в текущей директории.")
