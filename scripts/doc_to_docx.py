import os
import comtypes.client


def convert_doc_to_docx(doc_path):
    """
    Преобразует файл .doc в .docx.

    Args:
        doc_path (str): Путь к исходному файлу .doc.

    Returns:
        str: Путь к созданному файлу .docx.
    """
    word = comtypes.client.CreateObject("Word.Application")
    word.Visible = False

    try:
        # Открываем файл .doc
        doc = word.Documents.Open(doc_path)
        # Формируем путь для нового файла .docx
        docx_path = os.path.splitext(doc_path)[0] + ".docx"
        # Сохраняем как .docx
        doc.SaveAs(docx_path, FileFormat=16)  # Формат 16 соответствует .docx
        print(f"Конвертирован: {doc_path} -> {docx_path}")
        return docx_path
    except Exception as e:
        print(f"Ошибка при конвертации {doc_path}: {e}")
    finally:
        # Закрываем документ и приложение Word
        doc.Close()
        word.Quit()


# Указываем директорию с файлами .doc
directory = r"C:\Project1\GITProjects\elastic_docker\Доки\word"
for file in os.listdir(directory):
    if file.endswith(".doc"):
        doc_path = os.path.join(directory, file)
        convert_doc_to_docx(doc_path)
