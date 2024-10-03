import logging
import openai
import os
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters
from dotenv import load_dotenv
from openai import (
    OpenAIError,
    APIError,
    BadRequestError,
)  # Используем правильные исключения

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


# Функция для обработки команды /start
async def start(update: Update, context):
    await update.message.reply_text("Привет! Задай мне любой вопрос.")


# Функция для обработки сообщений
async def handle_message(update: Update, context):
    user_message = update.message.text
    logger.info(f"Получено сообщение: {user_message}")

    try:
        # Используем правильный метод для OpenAI версии 1.47.0
        response = openai.chat.completions.create(
            model="gpt-4",  # Модель для работы
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": user_message},  # Сообщение от пользователя
            ],
            max_tokens=150,
        )

        # Получаем текст из ответа, используя атрибуты объекта
        bot_reply = response.choices[0].message.content
        logger.info(f"Ответ от OpenAI: {bot_reply}")

        await update.message.reply_text(bot_reply)

    # Обработка исключений OpenAI
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
