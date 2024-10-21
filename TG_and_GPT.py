import logging
import openai
import os
import numpy as np
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters
from dotenv import load_dotenv
from pymilvus import connections, Collection, utility
import tiktoken

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

# Подключаемся к Milvus
connections.connect("default", host="localhost", port="19530")

# Получаем список всех коллекций в базе данных
all_collections = utility.list_collections()

# Собираем эмбеддинги из всех активных коллекций
all_texts = []
all_embeddings = []

# Собираем эмбеддинги из всех коллекций
for collection_name in all_collections:
    collection = Collection(name=collection_name)

    try:
        # Проверяем, есть ли в коллекции данные (работает, только если коллекция активна)
        if collection.num_entities > 0:
            # Извлекаем эмбеддинги и тексты из коллекции
            entities = collection.query(
                expr="id > 0", output_fields=["embedding", "text"]
            )
            texts = [entity["text"] for entity in entities]
            embeddings = [entity["embedding"] for entity in entities]

            all_texts.extend(texts)
            all_embeddings.extend(embeddings)
    except Exception as e:
        # Если коллекция не активна, она выдаст ошибку, которую мы можем проигнорировать
        print(f"Коллекция {collection_name} не активна или не загружена: {e}")


# Функция для создания эмбеддинга запроса пользователя
def create_embedding_for_query(query):
    response = openai.embeddings.create(
        input=[query],
        model="text-embedding-ada-002",
    )
    return response.data[0].embedding


# Поиск наиболее релевантных эмбеддингов
def find_most_similar(query_embedding, top_n=8):
    query_embedding_np = np.array([query_embedding], dtype=np.float32)
    similarities = np.dot(all_embeddings, query_embedding_np.T)
    most_similar_indices = np.argsort(similarities, axis=0)[::-1][:top_n]
    return [all_texts[i] for i in most_similar_indices.flatten()]


# Функция для обработки команды /start
async def start(update: Update, context):
    await update.message.reply_text(
        "Привет! Я асистент для инженеров, можешь задать мне вопрос🌚"
    )


def count_tokens(text):
    encoding = tiktoken.encoding_for_model("text-embedding-ada-002")
    tokens = encoding.encode(text)
    return len(tokens)


# Функция для обработки сообщений
async def handle_message(update: Update, context):
    user_message = update.message.text
    logger.info(f"Получено сообщение: {user_message}")

    try:
        # 1. Создаем эмбеддинг для запроса пользователя
        query_embedding = create_embedding_for_query(user_message)

        # Логирование эмбеддингов, если нужно
        logger.info(f"Эмбеддинги, отправленные в GPT: {query_embedding}")

        # 2. Ищем наиболее релевантные тексты на основе эмбеддингов
        most_similar_texts = find_most_similar(query_embedding)

        # 3. Собираем контекст из наиболее релевантных текстов
        context_text = "\n\n".join(most_similar_texts)

        # Подсчет токенов для контекста
        token_count = count_tokens(context_text)
        logger.info(f"Контекст содержит {token_count} токенов")

        # Логирование используемых текстов
        logger.info(f"Используемый контекст: {context_text}")

        # 4. Формируем запрос к GPT с контекстом
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": 'Я хочу, чтобы ты выступил в роли асистента-помощника по правилам компании "Связь и Радионавигация", Твоя основная задача - отвечать по развернуто, не сжимая текст, не выдумывать информацию. Твой ответ должен быть не более 600 токенов',
                },
                {
                    "role": "system",
                    "content": f"Вот релевантная информация:\n\n{context_text}",
                },
                {"role": "user", "content": user_message},
            ],
            max_tokens=600,
            temperature=0.6,
        )

        # Получаем ответ от OpenAI
        bot_reply = response.choices[0].message.content
        logger.info(f"Ответ от OpenAI: {bot_reply}")

        await update.message.reply_text(bot_reply)

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

    logger.info("Бот запущен.")
    application.run_polling()


if __name__ == "__main__":
    main()
