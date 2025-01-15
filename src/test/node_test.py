import pytest
from src.consistent_hash_ring.node import Node
from src.hash.hash import hash

def test_initialization():
    node = Node(0, 10, 1)
    assert node.capacity == 10
    assert node.index == 0
    assert node.replica_count == 1

def test_insert_delete_one_item():
    node = Node(0, 10, 1)
    item = "item"
    key = "key"
    node.insert(key, item)
    assert node.data[key] == item
    assert node.has_key(key)
    assert node.get(key) == item
    assert node.delete(key) == item
    assert key not in node.data
    assert not node.has_key(key)

def test_insert_update_item():
    node = Node(0, 10, 1)
    item = "item"
    new_item = "new_item"
    key = "key"
    node.insert(key, item)
    assert node.get(key) == item
    node.update(key, new_item) == new_item
    assert node.get(key) == new_item
    assert node.has_key(key)

def test_insert_delete_many():
    node = Node(0, 10, 1)
    key1 = "key1"
    item1 = "item1"

    key2 = "key2"
    item2 = "item2"

    key3 = "key3"
    item3 = "item3"

    node.insert(key1, item1)
    node.insert(key2, item2)
    node.insert(key3, item3)

    assert node.has_key(key1)
    assert node.has_key(key2)
    assert node.has_key(key3)

    assert node.get(key1) == item1
    assert node.delete(key1) == item1
    assert not node.has_key(key1)

    assert node.get(key2) == item2
    assert node.delete(key2) == item2
    assert not node.has_key(key2)

    assert node.get(key3) == item3
    assert node.delete(key3) == item3
    assert not node.has_key(key3)


def insert_more_than_capacity():
    ...

def get_unexistent_key():
    ...

def update_unexistent_key():
    ...

def insert_duplicated_key():
    ...


def test_calc_mid_hash():
    node = Node(0, 10, 1)
    key1 = "key1"
    item1 = "item1"
    item1_hash = hash(key1)

    key2 = "key2"
    item2 = "item2"
    item2_hash = hash(key2)

    key3 = "key3"
    item3 = "item3"
    item3_hash = hash(key3)

    mean = (item1_hash +  item2_hash +  item3_hash) // 3

    node.insert(key1, item1)
    node.insert(key2, item2)
    node.insert(key3, item3)

    assert node.calc_mid_hash() == mean
    
def test_export_keys():
    node1 = Node(0, 4, 1)
    node2 = Node(0, 4, 1)
    items = {
        "key1": "item1",
        "key2": "item2",
        "key3": "item3",
        "key4": "item4"
    }
    for key, item in items.items():
        node1.insert(key, item)
    
    # Sorting keys by their hashes
    sorted_keys = list(items.keys())
    sorted_keys.sort(key=lambda x: hash(x))

    first_key = sorted_keys[2]
    last_key = sorted_keys[3]
    node1.export_keys(node2, hash(first_key), hash(last_key))

    assert not node1.has_key(sorted_keys[2])
    assert not node1.has_key(sorted_keys[3])

    assert node2.has_key(sorted_keys[2])
    assert node2.has_key(sorted_keys[3])

def test_export_keys():
    node1 = Node(0, 4, 1)
    node2 = Node(0, 4, 1)
    items = {
        "key1": "item1",
        "key2": "item2",
        "key3": "item3",
        "key4": "item4"
    }
    for key, item in items.items():
        node1.insert(key, item)
    
    # Sorting keys by their hashes
    sorted_keys = list(items.keys())
    sorted_keys.sort(key=lambda x: hash(x))

    first_key = sorted_keys[2]
    last_key = sorted_keys[3]
    node1.export_keys(node2, hash(first_key), hash(last_key))

    assert not node1.has_key(sorted_keys[2])
    assert not node1.has_key(sorted_keys[3])

    assert node2.has_key(sorted_keys[2])
    assert node2.has_key(sorted_keys[3])

# Tests for clean_keys
def test_clean_keys():
    node = Node(0, 4, 1)
    key1 = "key1"
    item1 = "item1"

    key2 = "key2"
    item2 = "item2"

    key3 = "key3"
    item3 = "item3"
    
    node.insert(key1, item1)
    node.insert(key2, item2)
    node.insert(key3, item3)

    node.keys_to_delete.append(key1)
    node.keys_to_delete.append(key2)
    node.clean_keys()
    
    assert not node.has_key(key1)
    assert not node.has_key(key2)
    assert node.has_key(key3)