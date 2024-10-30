import logging
import openai
import os
import numpy as np
import gspread  # Библиотека для работы с Google Sheets
from google.oauth2.service_account import Credentials
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters
from dotenv import load_dotenv
from pymilvus import connections, Collection, utility
import tiktoken
import boto3  # Библиотека для работы с MinIO (S3 совместимое API)
from botocore.exceptions import NoCredentialsError

# Загрузка переменных окружения из файла .env
load_dotenv("tokens.env")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_REGION_NAME = os.getenv("MINIO_REGION_NAME")
MILFUS_HOST = os.getenv("MILFUS_HOST")
MILFUS_PORT = os.getenv("MILFUS_PORT")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")  # ID Google Таблицы MODEL_GPT_INT
MODEL_GPT_INT = os.getenv("MODEL_GPT_INT")

# Устанавливаем ключ OpenAI API
openai.api_key = OPENAI_API_KEY

# Настройка Google Sheets API
credentials = Credentials.from_service_account_file(
    r"C:\Project1\GITProjects\myproject2\telegramgpt.json",
    scopes=["https://www.googleapis.com/auth/spreadsheets"],
)
client = gspread.authorize(credentials)
sheet = client.open_by_key(SPREADSHEET_ID).sheet1

# Настройка MinIO клиента
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name=MINIO_REGION_NAME,
)
print(f'Логин "{MINIO_ACCESS_KEY}" для БД MiniO')
print(f'Пароль "{MINIO_SECRET_KEY}" для БД MiniO')

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)

# Подключаемся к Milvus
connections.connect("default", host=MILFUS_HOST, port=MILFUS_PORT)

# Получаем список всех коллекций в базе данных
all_collections = utility.list_collections()

# Собираем эмбеддинги из всех активных коллекций
all_texts = []
all_embeddings = []
all_table_references = []

for collection_name in all_collections:
    collection = Collection(name=collection_name)

    try:
        if collection.num_entities > 0:
            entities = collection.query(
                expr="id > 0", output_fields=["embedding", "text", "table_reference"]
            )
            texts = [entity["text"] for entity in entities]
            embeddings = [entity["embedding"] for entity in entities]
            table_references = [entity["table_reference"] for entity in entities]

            all_texts.extend(texts)
            all_embeddings.extend(embeddings)
            all_table_references.extend(table_references)
    except Exception as e:
        print(f"Коллекция {collection_name} не активна или не загружена: {e}")


# Функция для создания эмбеддинга запроса пользователя
def create_embedding_for_query(query):
    response = openai.embeddings.create(
        input=[query],
        model="text-embedding-ada-002",
    )
    return response.data[0].embedding


# Поиск наиболее релевантных эмбеддингов
def find_most_similar(query_embedding, top_n=10):
    query_embedding_np = np.array([query_embedding], dtype=np.float32)
    similarities = np.dot(all_embeddings, query_embedding_np.T)
    most_similar_indices = np.argsort(similarities, axis=0)[::-1][:top_n]
    return [all_texts[i] for i in most_similar_indices.flatten()], [
        all_table_references[i] for i in most_similar_indices.flatten()
    ]


# Чтение содержимого таблицы из MinIO (S3 хранилища)
def read_table_from_minio(table_reference):
    try:
        response = s3_client.get_object(Bucket=MINIO_BUCKET_NAME, Key=table_reference)
        table_content = response["Body"].read().decode("utf-8")
        return table_content
    except NoCredentialsError as e:
        logger.error(f"Ошибка аутентификации в MinIO: {e}")
        return None
    except Exception as e:
        logger.error(f"Не удалось прочитать таблицу из MinIO: {e}")
        return None


# Функция для обработки команды /start
async def start(update: Update, context):
    await update.message.reply_text(
        "Привет! Я асистент для инженеров, можешь задать мне вопрос🌚"
    )


def count_tokens(text):
    encoding = tiktoken.encoding_for_model("text-embedding-ada-002")
    tokens = encoding.encode(text)
    return len(tokens)


# Функция для записи вопроса пользователя в Google Таблицу
def save_user_question_to_sheet(user_message, gpt_response, user_tag):
    next_row = len(sheet.get_all_values()) + 1  # Следующий номер строки
    sheet.update(
        f"A{next_row}:E{next_row}",
        [[next_row - 1, user_message, gpt_response, "", user_tag]],
    )  # Запись номера теста, вопроса, ответа GPT, оценки (пусто), и тега пользователя


# Функция для обработки сообщений
async def handle_message(update: Update, context):
    user_message = update.message.text
    user_tag = (
        update.message.from_user.username or update.message.from_user.full_name
    )  # Получение никнейма или имени пользователя
    logger.info(f"Получено сообщение от {user_tag}: {user_message}")

    try:
        query_embedding = create_embedding_for_query(user_message)
        most_similar_texts, most_similar_table_refs = find_most_similar(query_embedding)
        context_text = "\n\n".join(most_similar_texts)

        table_contexts = []
        for table_ref in most_similar_table_refs:
            if table_ref:
                table_content = read_table_from_minio(table_ref)
                if table_content:
                    table_contexts.append(table_content)
                    logger.info(f"Использована таблица из MinIO: {table_ref}")

        if table_contexts:
            context_text += "\n\nТаблицы:\n" + "\n\n".join(table_contexts)

        token_count = count_tokens(context_text)
        logger.info(f"Контекст содержит {token_count} токенов")
        logger.info(f"Используемый контекст: {context_text}")

        response = openai.chat.completions.create(
            model=MODEL_GPT_INT,
            messages=[
                {
                    "role": "system",
                    "content": 'Я хочу, чтобы ты выступил в роли асистента-помощника по правилам компании "Связь и Радионавигация". Твоя основная задача - отвечать не сжимая текст, не выдумывать информацию.',
                },
                {
                    "role": "system",
                    "content": f"Вот релевантная информация:\n\n{context_text}",
                },
                {"role": "user", "content": user_message},
            ],
            temperature=0.4,
        )

        bot_reply = response.choices[0].message.content
        logger.info(f"Ответ от OpenAI: {bot_reply}")
        await update.message.reply_text(bot_reply)

        # Сохраняем вопрос пользователя, ответ GPT и никнейм в Google Таблицу
        save_user_question_to_sheet(user_message, bot_reply, user_tag)

        # Добавление кнопок для оценки качества
        reply_keyboard = [
            ["Хорошо"],
            ["Удовлетворительно"],
            ["Плохо"],
        ]
        markup = ReplyKeyboardMarkup(
            reply_keyboard, one_time_keyboard=True, resize_keyboard=True
        )
        await update.message.reply_text("Оцените качество ответа:", reply_markup=markup)

    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")
        await update.message.reply_text(
            f"Произошла ошибка при получении ответа: {str(e)}"
        )


# Функция для обработки оценок
async def handle_feedback(update: Update, context):
    quality_score = update.message.text  # Получение оценки пользователя
    next_row = len(sheet.get_all_values())  # Нахождение строки для записи оценки
    sheet.update(f"D{next_row}", [[quality_score]])  # Запись оценки в 4-й столбик
    await update.message.reply_text("Спасибо за вашу оценку!")


# Основная функция для запуска бота
def main():
    application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.Regex("^(Хорошо|Удовлетворительно|Плохо)$"),
            handle_message,
        )
    )
    application.add_handler(
        MessageHandler(
            filters.Regex("^(Хорошо|Удовлетворительно|Плохо)$"),
            handle_feedback,
        )
    )
    logger.info("Бот запущен.")
    application.run_polling()


if __name__ == "__main__":
    main()
