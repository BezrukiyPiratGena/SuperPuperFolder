import csv
from openpyxl import Workbook


def convert_csv_to_xlsx_openpyxl(csv_file, xlsx_file):
    """
    Конвертирует CSV файл в XLSX формат с использованием openpyxl.

    :param csv_file: Путь к исходному CSV файлу.
    :param xlsx_file: Путь для сохранения XLSX файла.
    """
    # Создаем новый XLSX файл
    workbook = Workbook()
    sheet = workbook.active

    # Открываем CSV файл и копируем данные
    with open(csv_file, mode="r", encoding="utf-8") as file:
        reader = csv.reader(file)
        for row in reader:
            sheet.append(row)  # Добавляем строки в XLSX

    # Сохраняем XLSX файл
    workbook.save(xlsx_file)
    print(f"Файл сохранён как {xlsx_file}")


# Пример использования
csv_file = r"C:\Project1\GITProjects\myproject2\table_5.csv"
xlsx_file = r"C:\Project1\GITProjects\myproject2\table_5.xlsx"
convert_csv_to_xlsx_openpyxl(csv_file, xlsx_file)
