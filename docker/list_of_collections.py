from pymilvus import connections, utility

# Подключение к Milvus
connections.connect("default", host="localhost", port="19530")

# Получить список всех коллекций
collections = utility.list_collections()
print(f"Список коллекций: {collections}")
