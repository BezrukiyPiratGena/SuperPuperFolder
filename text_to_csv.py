from docx import Document
import csv


def save_table_to_csv(csv_file_path, table_data):
    """
    Сохраняет данные таблицы в CSV файл.
    """
    with open(csv_file_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        for row_data in table_data:
            writer.writerow(row_data)


def extract_text_and_tables_from_word():
    """
    Извлекает текст и таблицы из Word файла C:\Project1\GITProjects\myproject2\example_full.docx,
    сохраняет таблицы как CSV файлы и возвращает список текстовых блоков и ссылок на таблицы.
    """
    word_path = r"C:\Project1\GITProjects\myproject2\example_full.docx"
    output_dir = r"C:\Project1\GITProjects\myproject2\csv_tables"
    doc = Document(word_path)

    text_blocks_with_tables = []
    current_text_block = []
    current_table_data = []
    previous_table_data = []
    table_counter = 1
    last_was_table = False

    for idx, block in enumerate(doc.element.body):
        if block.tag.endswith("p"):  # Если это параграф (текст)
            paragraph = block.text.strip()
            if paragraph:
                if last_was_table and previous_table_data:
                    # Сохраняем предыдущую таблицу как CSV, так как после нее идет текст
                    csv_file_path = f"{output_dir}/table_{table_counter}.csv"
                    save_table_to_csv(csv_file_path, previous_table_data)
                    explanation = current_text_block[-1] if current_text_block else ""
                    text_blocks_with_tables.append(
                        {"text": explanation, "table_reference": csv_file_path}
                    )
                    print(f"Таблица {table_counter} сохранена в {csv_file_path}")
                    previous_table_data = []
                    table_counter += 1

                current_text_block.append(paragraph)
                last_was_table = False

        elif block.tag.endswith("tbl"):  # Если это таблица
            table = next(t for t in doc.tables if t._tbl == block)
            current_table_data = [
                [cell.text.strip() for cell in row.cells] for row in table.rows
            ]

            if last_was_table:
                previous_table_data.extend(current_table_data)
            else:
                previous_table_data = current_table_data

            last_was_table = True

    # Сохраняем последнюю таблицу, если после нее нет текста
    if last_was_table and previous_table_data:
        csv_file_path = f"{output_dir}/table_{table_counter}.csv"
        save_table_to_csv(csv_file_path, previous_table_data)
        explanation = current_text_block[-1] if current_text_block else ""
        text_blocks_with_tables.append(
            {"text": explanation, "table_reference": csv_file_path}
        )
        print(f"Таблица {table_counter} сохранена в {csv_file_path}")

    return text_blocks_with_tables, " ".join(current_text_block)


# Запуск функции для извлечения таблиц и сохранения в CSV
extract_text_and_tables_from_word()
