from pymilvus import Collection, Index
from pymilvus import connections

# Подключаемся к Milvus
connections.connect("default", host="localhost", port="19530")

# Указываем название коллекции
collection_name = "text_embeddings_test"
collection = Collection(name=collection_name)

# Определяем параметры индекса
index_params = {
    "index_type": "IVF_FLAT",  # Или "IVF_SQ8", или другой подходящий тип
    "metric_type": "L2",  # Метрика расстояния, например, L2 или IP (Inner Product)
    "params": {
        "nlist": 128
    },  # Количество списков для индекса (подбирается экспериментально)
}

# Создаем индекс
index = Index(collection, field_name="embedding", index_params=index_params)

# После создания индекса, загружаем коллекцию
collection.load()

print(f"Индекс успешно создан и коллекция '{collection_name}' загружена.")
