import uuid
import hashlib


def generate_id():
    return uuid.uuid4()


def hash_cache_key(items):
    h = hashlib.md5()

    for item in items:
        h.update(str(item).encode("utf-8"))

    return h.hexdigest()
