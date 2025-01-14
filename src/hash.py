import hashlib
MAX_HASH_SIZE = 100

def hash(key: str)->int:
    """
    returns the md5 hash of the key
    """
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % MAX_HASH_SIZE