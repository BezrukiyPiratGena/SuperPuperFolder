from pymilvus import connections, utility, Collection
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Milvus Scanner")
count_all = 0
count_error = 0


def connect_to_milvus(host, port, db_name, user, password):
    """
    Подключается к Milvus.

    Args:
        host (str): Хост Milvus.
        port (str): Порт Milvus.
        db_name (str): Имя базы данных.
        user (str): Логин Milvus.
        password (str): Пароль Milvus.
    """
    try:
        connections.connect(
            alias="default",
            host=host,
            port=port,
            db_name=db_name,
            user=user,
            password=password,
        )
        logger.info(f"Успешно подключено к Milvus на {host}:{port} в БД {db_name}.")
    except Exception as e:
        logger.error(f"Ошибка подключения к Milvus: {e}")
        raise


def find_inactive_collections():
    global count_all
    global count_error
    """
    Находит неактивированные коллекции (коллекции с num_entities = 0).

    Returns:
        list: Список имен неактивированных коллекций.
    """
    inactive_collections = []
    try:
        all_collections = utility.list_collections()
        logger.info(f"Найдено {len(all_collections)} коллекций в базе данных.")

        for collection_name in all_collections:
            try:
                collection = Collection(name=collection_name)
                if collection.num_entities == 0:

                    logger.info(f"Коллекция '{collection_name}' неактивирована.")
                    inactive_collections.append(collection_name)
                    count_error += 1
                # else:
                # logger.info(
                #    f"Коллекция '{collection_name}' активна (количество сущностей: {collection.num_entities})."
                # )
                count_all += 1
                print(f"count_all - {count_all}, count_error - {count_error}")
            except Exception as e:
                logger.error(f"Ошибка при проверке коллекции '{collection_name}': {e}")

    except Exception as e:
        logger.error(f"Ошибка при получении списка коллекций: {e}")

    return inactive_collections


def main():
    # Параметры подключения
    MILVUS_HOST = "attu.vnigma.ru"  # Замените на ваш хост
    MILVUS_PORT = "30004"  # Замените на ваш порт
    MILVUS_DB_NAME = "engs_bot"  # Замените на имя базы данных
    MILVUS_USER = "KosRoot"  # Укажите логин, если требуется
    MILVUS_PASSWORD = "Cir73SPb+"  # Укажите пароль, если требуется

    # Подключение к Milvus
    connect_to_milvus(
        MILVUS_HOST, MILVUS_PORT, MILVUS_DB_NAME, MILVUS_USER, MILVUS_PASSWORD
    )

    # Поиск неактивированных коллекций
    inactive_collections = find_inactive_collections()

    # Вывод списка неактивированных коллекций
    if inactive_collections:
        logger.info(
            f"Найдены неактивированные коллекции ({len(inactive_collections)}): {inactive_collections}"
        )
    else:
        logger.info("Все коллекции активны.")

    delete_collections(inactive_collections)


def delete_collections(collection_names):
    """Удаляет указанные коллекции."""
    for collection_name in collection_names:
        try:
            if utility.has_collection(collection_name):
                utility.drop_collection(collection_name)
                logger.info(f"Коллекция '{collection_name}' успешно удалена.")
            else:
                logger.warning(
                    f"Коллекция '{collection_name}' не найдена для удаления."
                )
        except Exception as e:
            logger.error(f"Ошибка при удалении коллекции '{collection_name}': {e}")


if __name__ == "__main__":
    main()
