import logging
import openai
import os
import json
import numpy as np
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters
from dotenv import load_dotenv
from sklearn.metrics.pairwise import cosine_similarity
from openai import OpenAIError, APIError, BadRequestError

# Загружаем переменные окружения из файла .env
load_dotenv()

# Устанавливаем ключи API из переменных окружения
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Устанавливаем ключ OpenAI API
openai.api_key = OPENAI_API_KEY

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


def load_embeddings_from_json(file_path):
    """
    Загружает эмбеддинги и текстовые данные из файла JSON, который является массивом объектов.

    :param file_path: Путь к файлу с эмбеддингами.
    :return: Список текстов и их эмбеддингов.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)  # Загружаем весь JSON файл целиком как массив объектов

    texts = [item["text"] for item in data]
    embeddings = [item["embedding"] for item in data]
    return texts, embeddings


# Создание эмбеддинга для запроса пользователя
def create_embedding_for_query(query):
    response = openai.embeddings.create(
        input=[query],
        model="text-embedding-ada-002",
    )
    return response.data[0].embedding


# Поиск наиболее релевантных эмбеддингов на основе запроса
def find_most_similar(query_embedding, embeddings, top_n=3):
    similarities = cosine_similarity([query_embedding], embeddings)
    most_similar_indices = np.argsort(similarities[0])[::-1][:top_n]
    return most_similar_indices, similarities[0][most_similar_indices]


# Загрузка эмбеддингов и текстов при старте бота
texts, embeddings = load_embeddings_from_json(
    "C:/Project1/GITProjects/myproject2/docs/ready/embeddings_ready.json"
)


# Функция для обработки команды /start
async def start(update: Update, context):
    await update.message.reply_text("Привет! Задай мне любой вопрос.")


# Функция для обработки сообщений
async def handle_message(update: Update, context):
    user_message = update.message.text
    logger.info(f"Получено сообщение: {user_message}")

    try:
        # 1. Создаем эмбеддинг для запроса пользователя
        query_embedding = create_embedding_for_query(user_message)

        # 2. Ищем наиболее релевантные тексты на основе эмбеддингов
        most_similar_indices, similarities = find_most_similar(
            query_embedding, embeddings
        )

        # 3. Собираем контекст из наиболее релевантных текстов
        context_text = "\n\n".join([texts[i] for i in most_similar_indices])

        # 4. Формируем запрос к GPT с контекстом
        response = openai.chat.completions.create(
            model="gpt-4",
            messages=[
                {
                    "role": "system",
                    "content": 'Ты асистент компании "Связь и Радионавигация". Твоя основная задача - это помогать сотрудникам, которые хотят узнать что-то из правил компании "Связь и Радионавигация". Также ты должен уложить ответ в 50 слов',
                },
                {
                    "role": "system",
                    "content": f"Вот релевантная информация:\n\n{context_text}",
                },
                {"role": "user", "content": user_message},
            ],
            max_tokens=200,
        )

        # Получаем ответ от OpenAI
        bot_reply = response.choices[0].message.content
        logger.info(f"Ответ от OpenAI: {bot_reply}")

        await update.message.reply_text(bot_reply)

    except APIError as e:
        logger.error(f"Ошибка OpenAI API: {e}")
        await update.message.reply_text(f"Ошибка при обращении к OpenAI: {str(e)}")
    except BadRequestError as e:
        logger.error(f"Неверный запрос к OpenAI API: {e}")
        await update.message.reply_text(f"Неверный запрос: {str(e)}")
    except OpenAIError as e:
        logger.error(f"Ошибка OpenAI: {e}")
        await update.message.reply_text(
            f"Произошла ошибка при обработке запроса: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")
        await update.message.reply_text(
            f"Произошла ошибка при получении ответа: {str(e)}"
        )


# Основная функция для запуска бота
def main():
    application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
    )

    logger.info("Бот запущен...")
    application.run_polling()


if __name__ == "__main__":
    main()
