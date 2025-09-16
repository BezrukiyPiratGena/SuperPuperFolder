import os
import json
import logging

logger = logging.getLogger(__name__)


def load_manual_ids():
    """
    Читает файл id_manuals.json (рядом со скриптом) и возвращает словарь:
      { 'id_мануала': 'оригинальное_название_файла', ... }
    Поддерживаемые форматы JSON:
      1) { "ID1": "file1.pdf", "ID2": "file2.pdf", ... }
      2) [ {"id": "ID1", "name":"file1.pdf"}, ... ]  (также: file_id/manual_id, file_name/filename)
      3) [ ["ID1", "file1.pdf"], ["ID2", "file2.pdf"], ... ]
    """
    path = os.path.join(os.path.dirname(__file__), "id_manuals.json")

    if not os.path.exists(path):
        logger.warning(f"Файл с маппингом не найден: {path}")
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Ошибка чтения {path}: {e}")
        return {}

    manual_id_dict = {}

    # Вариант 1: объект-словарь
    if isinstance(data, dict):
        for k, v in data.items():
            if k and v:
                manual_id_dict[str(k).strip()] = str(v).strip()

    # Вариант 2/3: список
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                file_id = str(
                    item.get("id") or item.get("file_id") or item.get("manual_id") or ""
                ).strip()
                file_name = str(
                    item.get("name")
                    or item.get("file_name")
                    or item.get("filename")
                    or ""
                ).strip()
                if file_id and file_name:
                    manual_id_dict[file_id] = file_name
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                file_id = str(item[0]).strip()
                file_name = str(item[1]).strip()
                if file_id and file_name:
                    manual_id_dict[file_id] = file_name
            # остальные варианты игнорируем
    else:
        logger.error(f"Неподдержимый формат JSON в {path}: {type(data)}")
        return {}

    logger.info(f"Успешно загружено {len(manual_id_dict)} записей из {path}.")
    return manual_id_dict
