import base64
import requests
import json
import os
import re
import shutil
import logging
import warnings
import pdfplumber
import pytesseract
import cv2
from pdf2image import convert_from_path
from concurrent.futures import ThreadPoolExecutor, as_completed
import easyocr
import numpy as np

warnings.simplefilter("ignore")

# === Настройки ===
reader = easyocr.Reader(["ru", "en"], gpu=True)
POPPLER_PATH = r"C:\Project1\Poppler\poppler-24.08.0\Library\bin"


pdf_folder = r"C:\Project1\GITProjects\elastic_docker"
ready_folder = os.path.join(pdf_folder, "ready")
elastic_url = (
    "https://kibana.vnigma.ru:30006/pdf_docs_new_v3/_doc?pipeline=pdf_pipeline"
)

# 🔐 Данные для авторизации
elastic_user = "kosadmin_user"
elastic_password = "Cir73SPb+"
headers = {"Content-Type": "application/json"}

# === Настройка логирования ===
log_file = "upload_log.txt"
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


def extract_text_from_pdf(pdf_path):
    """Сначала пытается извлечь текст стандартным способом, затем через OCR, если неудачно."""
    text = ""

    # === 1. Попытка обычного извлечения текста ===
    try:
        with pdfplumber.open(pdf_path) as pdf:
            extracted_pages = [page.extract_text() for page in pdf.pages]
            text = "\n\n".join(filter(None, extracted_pages)).strip()
    except Exception as e:
        print(
            f"⚠ Ошибка при извлечении текста файла {os.path.basename(pdf_path)} через pdfplumber: {e}"
        )

    # === 2. Если текст не найден — запускаем OCR ===
    if not text or len(text) < 50:  # Можно регулировать порог, напр. < 50 символов
        print(f"🔍 PDF '{os.path.basename(pdf_path)}' - текста нет, запускаем OCR...")
        text_blocks = []
        pages = convert_from_path(pdf_path, dpi=200, poppler_path=POPPLER_PATH)
        for img in pages:
            img_np = preprocess_image(np.array(img))
            lines = reader.readtext(img_np, detail=0, paragraph=True)
            lines = [line for line in lines if len(line.strip()) > 2]
            text_blocks.append("\n".join(lines))
        text = "\n\n".join(text_blocks)

    return text


def preprocess_image(img_np):
    gray = cv2.cvtColor(img_np, cv2.COLOR_RGB2GRAY)
    _, thresh = cv2.threshold(gray, 150, 255, cv2.THRESH_BINARY)
    return cv2.cvtColor(thresh, cv2.COLOR_GRAY2RGB)


def pdf_to_base64(pdf_path):
    with open(pdf_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def split_text_by_sentences(text, max_length=10000):
    print("Запустился split_text_by_sentences")
    """
    Разбивает текст по предложениям так, чтобы каждый чанк не превышал max_length символов.
    Предложения определяются по знакам ., !, ? с учетом пробелов после них.
    """
    sentences = re.split(r"(?<=[.!?])\s+", text)
    # Если не удалось разбить текст на предложения (например, нет знаков препинания), просто разделим по длине.
    if len(sentences) == 1:
        return [text[:max_length], text[max_length:]]

    chunks = []
    current_chunk = ""
    for sentence in sentences:
        # Если текущий чанк пуст, начинаем с текущего предложения
        if not current_chunk:
            current_chunk = sentence
        # Если добавление следующего предложения не превышает лимита
        elif len(current_chunk) + 1 + len(sentence) <= max_length:
            current_chunk += " " + sentence
        else:
            # Если текущий чанк уже почти максимальный, сохраняем его и начинаем новый с текущего предложения
            chunks.append(current_chunk)
            current_chunk = sentence
    if current_chunk:
        chunks.append(current_chunk)
    return chunks


def process_pdf(filename):
    file_path = os.path.join(pdf_folder, filename)
    ready_path = os.path.join(ready_folder, filename)

    print(f"📄 Обрабатывается файл: {filename}")
    logging.info(f"Начата обработка файла: {filename}")

    try:
        pdf_text = extract_text_from_pdf(file_path)
        header_text = f"{filename}\n\n"
        full_text = header_text + pdf_text

        print(f"Длина текста - {len(full_text)}")
        chunks = split_text_by_sentences(full_text, max_length=10000)
        base64_data = pdf_to_base64(file_path)

        for i, chunk in enumerate(chunks, start=1):
            text_to_send = (
                f"{chunk}\n\n(part {i} из {len(chunks)})" if len(chunks) > 1 else chunk
            )
            print(f"длина стака - {len(text_to_send)}")
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
                logging.info(f"Часть {i} файла {filename} успешно загружена.")
            else:
                print(
                    f"❌ Ошибка при загрузке части {i} файла {filename}: Код {response.status_code} - {response.text}"
                )
                logging.error(
                    f"Ошибка загрузки части {i} файла {filename}: Код {response.status_code} - {response.text}"
                )

        shutil.move(file_path, ready_path)
        print(f"📂 Файл перемещён в {ready_folder}")
        logging.info(f"Файл {filename} успешно загружен и перемещён в {ready_folder}")

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
with ThreadPoolExecutor(max_workers=4) as executor:  # Максимальное количество потоков
    futures = [
        executor.submit(process_pdf, filename)
        for filename in os.listdir(pdf_folder)
        if filename.lower().endswith(".pdf")
    ]
    for future in as_completed(futures):
        try:
            future.result()
        except Exception as e:
            print(f"⚠ Ошибка при выполнении задачи: {e}")

print("🚀 Все PDF обработаны, загружены и перемещены в 'ready'!")
