#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import argparse
import hashlib
import string
import shutil
from typing import List

# Алфавит base62: 0-9, a-z, A-Z
BASE62_ALPHABET = string.digits + string.ascii_lowercase + string.ascii_uppercase


def base62_from_bytes(b: bytes) -> str:
    """Перевод произвольного байтового числа в base62-строку."""
    n = int.from_bytes(b, "big", signed=False)
    if n == 0:
        return BASE62_ALPHABET[0]
    out = []
    base = len(BASE62_ALPHABET)
    while n:
        n, r = divmod(n, base)
        out.append(BASE62_ALPHABET[r])
    return "".join(reversed(out))


def make_deterministic_id(name: str, salt: str = "") -> str:
    """
    Детерминированный ID длиной 16 символов (буквы+цифры)
    на основе SHA-256(salt + original_name) → base62 → [:16].
    """
    h = hashlib.sha256((salt + name).encode("utf-8")).digest()
    b62 = base62_from_bytes(h)
    return b62[:30]


def unique_dest_path(dest_dir: str, filename: str) -> str:
    """Возвращает уникальный путь в dest_dir: name.ext, name (1).ext, name (2).ext, ..."""
    base, ext = os.path.splitext(filename)
    candidate = os.path.join(dest_dir, filename)
    i = 1
    while os.path.exists(candidate):
        candidate = os.path.join(dest_dir, f"{base} ({i}){ext}")
        i += 1
    return candidate


def collect_file_paths(root: str, exts: set[str], ready_dir: str) -> List[str]:
    """
    Рекурсивно собирает ПОЛНЫЕ пути файлов по расширениям.
    Папка ready исключается из обхода.
    """
    paths: List[str] = []
    ready_abs = os.path.abspath(ready_dir)
    root_abs = os.path.abspath(root)

    for dirpath, dirs, filenames in os.walk(root_abs):
        # не заходим в ready
        dirs[:] = [
            d for d in dirs if os.path.abspath(os.path.join(dirpath, d)) != ready_abs
        ]
        for fname in filenames:
            if os.path.splitext(fname)[1].lower() in exts:
                paths.append(os.path.join(dirpath, fname))
    return paths


def build_id_map_from_paths(paths: List[str]) -> dict:
    """
    Строит словарь {ID16: basename}. Коллизии по ID решаются добавлением соли #1, #2, ...
    """
    id_map: dict[str, str] = {}
    for p in paths:
        fname = os.path.basename(p)
        attempt = 0
        while True:
            salt = f"#{attempt}" if attempt else ""
            uid = make_deterministic_id(fname, salt=salt)
            if uid not in id_map or id_map[uid] == fname:
                id_map[uid] = fname
                break
            attempt += 1
    return id_map


def main():
    parser = argparse.ArgumentParser(
        description="Собрать словарь {ID16: filename} и перенести обработанные файлы в папку ready."
    )
    parser.add_argument("root", help="Корневая папка с документами")
    parser.add_argument(
        "--out", default="id_manuals.json", help="Путь к выходному JSON файлу"
    )
    parser.add_argument(
        "--ready-dir",
        default=None,
        help="Папка для переноса (по умолчанию: <root>/ready)",
    )
    parser.add_argument(
        "--ext",
        nargs="*",
        default=[".pdf", ".PDF", ".doc"],  # при желании добавь ".docx"
        help="Список расширений (с точкой). По умолчанию: .pdf .PDF .doc",
    )
    args = parser.parse_args()

    root = args.root
    ready_dir = args.ready_dir or os.path.join(root, "ready")

    # нормализуем расширения
    exts = {e if e.startswith(".") else "." + e for e in args.ext}
    exts = {e.lower() for e in exts}

    # собираем пути файлов
    file_paths = collect_file_paths(root, exts, ready_dir)
    if not file_paths:
        print("Файлов с указанными расширениями не найдено.")
        return

    # строим словарь ID->basename
    id_map = build_id_map_from_paths(file_paths)

    # сохраняем json
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(id_map, f, ensure_ascii=False, indent=2)
    print(f"Словарь записан: {args.out} (всего {len(id_map)} записей)")

    # переносим файлы в ready
    os.makedirs(ready_dir, exist_ok=True)
    moved, skipped = 0, 0
    for src in file_paths:
        fname = os.path.basename(src)
        dst = unique_dest_path(ready_dir, fname)
        try:
            shutil.move(src, dst)
            moved += 1
        except Exception as e:
            skipped += 1
            print(f"Не удалось перенести '{src}': {e}")

    print(f"Готово. Перенесено: {moved}, пропущено: {skipped}, папка: {ready_dir}")


if __name__ == "__main__":
    main()
