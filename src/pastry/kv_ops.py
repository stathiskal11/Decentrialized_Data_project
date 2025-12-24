from __future__ import annotations
from typing import Optional

def insert_at(node, key: str, value: dict) -> None:
    node.kv_store[key] = value

def lookup_at(node, key: str) -> Optional[dict]:
    return node.kv_store.get(key)

def update_at(node, key: str, value: dict) -> None:
    node.kv_store[key] = value

def delete_at(node, key: str) -> None:
    node.kv_store.pop(key, None)
