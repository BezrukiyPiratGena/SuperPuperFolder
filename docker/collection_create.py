from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection

# Подключение к Milvus
connections.connect("default", host="localhost", port="19530")

# Определение схемы коллекции
fields = [
    FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
    FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=128),
]
schema = CollectionSchema(fields, "тестовая коллекция")

# Создание коллекции
collection = Collection(name="ROL_CIR", schema=schema)

# Проверка, что коллекция создана
print(f"Коллекция {collection.name} успешно создана!")
