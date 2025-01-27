import os
import comtypes.client


def convert_docx_to_pdf(docx_path, output_dir):
    """
    Преобразует файл .docx в .pdf.

    Args:
        docx_path (str): Путь к исходному файлу .docx.
        output_dir (str): Директория, куда сохранять файлы .pdf.

    Returns:
        str: Путь к созданному файлу .pdf.
    """
    word = comtypes.client.CreateObject("Word.Application")
    word.Visible = False
    doc = None

    try:
        # Открываем документ
        doc = word.Documents.Open(docx_path)

        # Создаем путь для нового файла .pdf
        pdf_name = os.path.splitext(os.path.basename(docx_path))[0] + ".pdf"
        pdf_path = os.path.join(output_dir, pdf_name)

        # Сохраняем документ как .pdf
        doc.SaveAs(pdf_path, FileFormat=17)  # Формат 17 соответствует PDF
        print(f"Конвертирован: {docx_path} -> {pdf_path}")
        return pdf_path

    except Exception as e:
        print(f"Ошибка при конвертации {docx_path}: {e}")

    finally:
        if doc:
            doc.Close(False)  # Закрыть без сохранения
        word.Quit()  # Закрыть приложение Word


path_all = r"C:\Project1\GITProjects\scripts\Шлюхи"
# Директория с файлами .docx
input_directory = path_all
# Директория для сохранения файлов .pdf
output_directory = path_all

# Проверяем, существует ли выходная директория, и создаем ее, если нет
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Проходим по всем файлам в указанной директории
for file in os.listdir(input_directory):
    if file.endswith(".docx"):
        docx_path = os.path.join(input_directory, file)
        if os.path.getsize(docx_path) > 0:  # Проверяем, что файл не пустой
            convert_docx_to_pdf(docx_path, output_directory)
        else:
            print(f"Файл пустой и пропущен: {docx_path}")
