from pymilvus import connections

# Подключение к Milvus
connections.connect("default", host="localhost", port="19530")

# Проверка соединения
print("Connected to Milvus successfully!")
