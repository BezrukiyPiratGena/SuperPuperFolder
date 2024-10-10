from pymilvus import Collection, connections

# Подключение к Milvus
connections.connect("default", host="localhost", port="19530")

# Подключение к коллекции
collection = Collection("milkshake_collection")

# Вставка данных
ids = [1, 2, 3]
vectors = [[0.1] * 128, [0.2] * 128, [0.3] * 128]  # Пример данных для вставки
collection.insert([ids, vectors])

# Выполнение flush для сохранения данных на диск
collection.flush()

# Вывести количество записей после вставки
print(f"Количество записей в коллекции: {collection.num_entities}")
