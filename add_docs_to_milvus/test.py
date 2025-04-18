import uuid

random_suffix = uuid.uuid4().hex[:20]
table_name = f"table_{random_suffix}"
table_name_xlsx = f"{table_name}.xlsx"
