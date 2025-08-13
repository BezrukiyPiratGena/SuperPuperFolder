import json
from pymilvus import Collection, connections, utility


connections.connect(
    alias="default",
    host="attu.vnigma.ru",
    port="30004",
    db_name="test_bot",
    user="KosRoot",
    password="Cir73SPb+",
)

# Получаем список всех коллекций в базе данных
collection = utility.list_collections()[0]  # <======== Загрузка всех коллекций

bd = Collection(collection)

bd.load()

filter_cond = "figure_id != ''"

result = bd.query(expr=filter_cond, output_fields=["figure_id", "reference"])

# Преобразуем в нужный формат
formatted_result = [{item["figure_id"]: item["reference"]} for item in result]

with open("all_links.json", "w", encoding="utf-8") as f:
    json.dump(formatted_result, f, ensure_ascii=False, indent=2)
