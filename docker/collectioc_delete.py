from pymilvus import connections, utility

# Подключение к Milvus
connections.connect("default", host="localhost", port="19530")

# Удаление коллекции
utility.drop_collection("test_collection")
print("Коллекция удалена")
