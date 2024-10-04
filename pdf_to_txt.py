import PyPDF2


def extract_and_save_pdf_text(pdf_path, output_path):
    # Открываем PDF файл
    with open(pdf_path, "rb") as file:
        reader = PyPDF2.PdfReader(file)
        text = ""
        # Извлекаем текст из каждой страницы
        for page in range(len(reader.pages)):
            text += reader.pages[page].extract_text()

    # Сохраняем извлеченный текст в указанный файл
    with open(output_path, "w", encoding="utf-8") as output_file:
        output_file.write(text)

    print(f"Текст извлечен из {pdf_path} и сохранен в {output_path}")


pdf_path = "C:\Project1\GITProjects\myproject2\НАЗВАНИЕ!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!.pdf"  # Путь к PDF файлу
output_path = "C:\Project1\GITProjects\myproject2\extracted_text.txt"  # Путь для сохранения текста

extract_and_save_pdf_text(pdf_path, output_path)
