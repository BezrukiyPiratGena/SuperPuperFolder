import base64
import requests
import json
import os
import shutil
import logging
import warnings
import docx  # Для работы с .docx файлами
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.simplefilter("ignore")

# === Настройки ===
word_folder = r"C:\Users\CIR\Desktop\jopa\Доки\word"
ready_folder = os.path.join(word_folder, "ready")
elastic_url = "https://kibana.vnigma.ru:30006/word_docs/_doc?pipeline=word_pipeline"

# 🔐 Данные для авторизации
elastic_user = "kosadmin_user"
elastic_password = "Cir73SPb+"
headers = {"Content-Type": "application/json"}

# === Настройка логирования ===
log_file = "upload_log_word.txt"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)

# === Создаём папку "ready", если её нет ===
if not os.path.exists(ready_folder):
    os.makedirs(ready_folder)
    print(f"📂 Создана папка: {ready_folder}")


def extract_text_from_docx(docx_path):
    """Извлекает текст из Word документа (.docx)."""
    try:
        document = docx.Document(docx_path)
    except Exception as e:
        logging.error(f"Ошибка при открытии документа {docx_path}: {e}")
        return ""
    text = []
    for para in document.paragraphs:
        if para.text:
            text.append(para.text)
    return "\n".join(text).strip()


def docx_to_base64(docx_path):
    with open(docx_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def process_docx(filename):
    file_path = os.path.join(word_folder, filename)
    ready_path = os.path.join(ready_folder, filename)

    print(f"📄 Обрабатывается файл: {filename}")
    logging.info(f"Начата обработка файла: {filename}")

    try:
        # Извлечение текста из документа
        doc_text = f"{filename}\n\n" + extract_text_from_docx(file_path)
        # Преобразование файла в base64
        base64_data = docx_to_base64(file_path)
        # Формирование документа для отправки
        document = {"data": base64_data, "text": doc_text, "filename": filename}

        response = requests.post(
            elastic_url,
            headers=headers,
            auth=(elastic_user, elastic_password),
            data=json.dumps(document),
            verify=False,
        )

        if response.status_code in [200, 201]:
            print(f"✅ Успешно загружен: {filename}")
            shutil.move(file_path, ready_path)
            print(f"📂 Файл перемещён в {ready_folder}")
            logging.info(
                f"Файл {filename} успешно загружен и перемещён в {ready_folder}"
            )
        else:
            print(
                f"❌ Ошибка при загрузке {filename}: Код {response.status_code} - {response.text}"
            )
            logging.error(
                f"Ошибка загрузки {filename}: Код {response.status_code} - {response.text}"
            )

    except requests.exceptions.RequestException as req_err:
        print(f"🚨 Сетевая ошибка при загрузке {filename}: {req_err}")
        logging.error(f"Сетевая ошибка при загрузке {filename}: {req_err}")
    except json.JSONDecodeError as json_err:
        print(f"⚠ Ошибка обработки JSON при загрузке {filename}: {json_err}")
        logging.error(f"Ошибка обработки JSON при загрузке {filename}: {json_err}")
    except Exception as e:
        print(f"⚠ Неизвестная ошибка при обработке {filename}: {e}")
        logging.exception(f"Неизвестная ошибка при обработке {filename}: {e}")


# === Обрабатываем все файлы в папке с использованием многопоточности ===
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [
        executor.submit(process_docx, filename)
        for filename in os.listdir(word_folder)
        if filename.lower().endswith(".docx")
    ]
    for future in as_completed(futures):
        try:
            future.result()
        except Exception as e:
            print(f"⚠ Ошибка при выполнении задачи: {e}")

print("🚀 Все Word документы обработаны, загружены и перемещены в 'ready'!")
