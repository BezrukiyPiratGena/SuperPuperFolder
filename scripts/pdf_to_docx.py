import os
from PyPDF2 import PdfReader
from docx import Document


def sanitize_text(text):
    """
    Удаляет или заменяет некорректные символы из текста.
    """
    if text is None:
        return ""
    # Удаляем управляющие символы и заменяем некорректные символы
    return "".join(c if c.isprintable() else " " for c in text)


def convert_pdf_to_docx(pdf_path, output_dir):
    """
    Преобразует файл .pdf в .docx.

    Args:
        pdf_path (str): Путь к исходному файлу .pdf.
        output_dir (str): Директория для сохранения файлов .docx.

    Returns:
        str: Путь к созданному файлу .docx.
    """
    try:
        # Открываем PDF
        reader = PdfReader(pdf_path)

        # Создаем новый Word-документ
        doc = Document()

        # Читаем каждую страницу PDF
        for page in reader.pages:
            text = page.extract_text()
            sanitized_text = sanitize_text(text)
            if sanitized_text.strip():  # Добавляем только непустой текст
                doc.add_paragraph(sanitized_text)

        # Генерируем имя для файла .docx
        docx_name = os.path.splitext(os.path.basename(pdf_path))[0] + ".docx"
        docx_path = os.path.join(output_dir, docx_name)

        # Сохраняем документ
        doc.save(docx_path)
        print(f"Конвертирован: {pdf_path} -> {docx_path}")
        return docx_path
    except Exception as e:
        print(f"Ошибка при конвертации {pdf_path}: {e}")


all_path = r"C:\Project1\GITProjects\scripts\Шлюхи"
# Директория с файлами .pdf
input_directory = all_path
# Директория для сохранения файлов .docx
output_directory = all_path

# Проверяем, существует ли выходная директория, и создаем ее, если нет
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Проходим по всем файлам в указанной директории
for file in os.listdir(input_directory):
    if file.endswith(".pdf"):
        pdf_path = os.path.join(input_directory, file)
        convert_pdf_to_docx(pdf_path, output_directory)
