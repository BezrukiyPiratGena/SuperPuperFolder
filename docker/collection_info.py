from pymilvus import Collection, connections

# Подключение к Milvus
connections.connect("default", host="localhost", port="19530")

# Откройте существующую коллекцию по имени
collection = Collection("milkshake_collection")

# Вывести схему коллекции
print(f"Схема коллекции: {collection.schema}")

# Вывести количество данных в коллекции
print(f"Количество записей в коллекции: {collection.num_entities}")
