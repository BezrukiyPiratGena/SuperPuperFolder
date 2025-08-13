import base64
import re
import requests
import json
import os
import shutil
import logging
import warnings
import docx
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.simplefilter("ignore")

# === Настройки ===
word_folder = r"C:\Users\CIR\Desktop\jopa\Доки\All_manuals\trouble word"
ready_folder = os.path.join(word_folder, "ready")
elastic_url = (
    "https://kibana.vnigma.ru:30006/pdf_docs_new_v2/_doc?pipeline=pdf_pipeline"
)

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
logger = logging.getLogger(__name__)

# === Создаём папку "ready", если её нет ===
if not os.path.exists(ready_folder):
    os.makedirs(ready_folder)
    print(f"📂 Создана папка: {ready_folder}")


def extract_text_from_docx(docx_path):
    """Извлекает текст из Word документа (.docx)."""
    try:
        document = docx.Document(docx_path)
        text = []
        for para in document.paragraphs:
            if para.text:
                text.append(para.text)
        return "\n".join(text).strip()
    except Exception as e:
        logger.error(f"⚠️ Ошибка при открытии {docx_path}: {e}")
        return ""


def docx_to_base64(docx_path):
    with open(docx_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def split_text_by_sentences(text, max_length=10000):
    """
    Разбивает текст на части по предложениям, чтобы избежать превышения лимита.
    """
    print("Запустился split_text_by_sentences")
    sentences = re.split(r"(?<=[.!?])\s+", text)
    if len(sentences) == 1:
        return [text[:max_length], text[max_length:]]

    chunks = []
    current_chunk = ""
    for sentence in sentences:
        if not current_chunk:
            current_chunk = sentence
        elif len(current_chunk) + 1 + len(sentence) <= max_length:
            current_chunk += " " + sentence
        else:
            chunks.append(current_chunk)
            current_chunk = sentence
    if current_chunk:
        chunks.append(current_chunk)
    return chunks


def process_docx(filename):
    file_path = os.path.join(word_folder, filename)
    ready_path = os.path.join(ready_folder, filename)

    print(f"📄 Обрабатывается файл: {filename}")
    logger.info(f"Начата обработка файла: {filename}")

    try:
        doc_text = extract_text_from_docx(file_path)
        header_text = f"{filename}\n\n"
        full_text = header_text + doc_text

        print(f"Длина текста - {len(full_text)}")
        chunks = split_text_by_sentences(full_text, max_length=10000)
        base64_data = docx_to_base64(file_path)

        all_parts_success = (
            True  # ✅ Новый флаг: были ли все части загружены без ошибок
        )

        for i, chunk in enumerate(chunks, start=1):
            text_to_send = (
                f"{chunk}\n\n(part {i} из {len(chunks)})" if len(chunks) > 1 else chunk
            )
            document = {
                "data": base64_data,
                "text": text_to_send,
                "filename": filename,
                "attachment": {"content": text_to_send},
            }

            response = requests.post(
                elastic_url,
                headers=headers,
                auth=(elastic_user, elastic_password),
                data=json.dumps(document),
                verify=False,
            )

            if response.status_code in [200, 201]:
                print(f"✅ Успешно загружена часть {i} файла: {filename}")
                logger.info(f"Часть {i} файла {filename} успешно загружена.")
            else:
                print(
                    f"❌ Ошибка при загрузке части {i} файла {filename}: Код {response.status_code} - {response.text}"
                )
                logger.error(
                    f"Ошибка загрузки части {i} файла {filename}: Код {response.status_code} - {response.text}"
                )
                all_parts_success = False  # ❗ Отмечаем ошибку

        # ✅ Перемещаем в ready только если все части успешные
        if all_parts_success:
            try:
                shutil.move(file_path, ready_path)
                print(f"📂 Файл перемещён в {ready_folder}")
                logger.info(
                    f"Файл {filename} успешно загружен и перемещён в {ready_folder}"
                )
            except Exception as move_err:
                logger.error(f"Ошибка при перемещении файла {filename}: {move_err}")
        else:
            print(f"⚠️ Файл {filename} НЕ перемещён из-за ошибок загрузки.")

    except requests.exceptions.RequestException as req_err:
        print(f"🚨 Сетевая ошибка при загрузке {filename}: {req_err}")
        logger.error(f"Сетевая ошибка при загрузке {filename}: {req_err}")
    except json.JSONDecodeError as json_err:
        print(f"⚠️ Ошибка обработки JSON при загрузке {filename}: {json_err}")
        logger.error(f"Ошибка обработки JSON при загрузке {filename}: {json_err}")
    except Exception as e:
        print(f"⚠️ Неизвестная ошибка при обработке {filename}: {e}")
        logger.exception(f"Неизвестная ошибка при обработке {filename}: {e}")


# === Обрабатываем все .docx файлы в папке ===
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
            print(f"⚠️ Ошибка при выполнении задачи: {e}")

print("🚀 Все Word документы обработаны, загружены и перемещены в 'ready'!")
