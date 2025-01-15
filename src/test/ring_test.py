import pytest
from src.consistent_hash_ring.ring import Ring
from src.consistent_hash_ring.node import Node
from src.adt.binary_search_tree import BinarySearchTree
from src.consistent_hash_ring.errors.node_errors import *
from src.consistent_hash_ring.errors.ring_errors import *

# Initialization
## BST ring
def test_ring_initialization():
    bst = BinarySearchTree()
    ring = Ring(2, bst)
    assert ring.node_capacity == 2
    assert ring.ring.root.key == 0
    assert isinstance(ring.ring.root.value, Node)
    assert not ring.ring.root.value.is_full()


def test_insert_one_element():
    bst = BinarySearchTree()
    ring = Ring(2, bst)
    ring.insert('key', 'value')
    assert ring.get('key') == 'value'

def test_insert_few_elements():
    bst = BinarySearchTree()
    ring = Ring(3, bst)
    ring.insert('key1', 'value1')
    ring.insert('key2', 'value2')
    assert ring.get('key1') == 'value1'
    assert ring.get('key2') == 'value2'

def test_insert_many_elements():
    bst = BinarySearchTree()
    ring = Ring(3, bst)
    ring.insert('key1', 'value1')
    ring.insert('key2', 'value2')
    ring.insert('key3', 'value3')
    ring.insert('key4', 'value4')
    assert ring.get('key1') == 'value1'
    assert ring.get('key2') == 'value2'
    assert ring.get('key3') == 'value3'
    assert ring.get('key4') == 'value4'

def insert_a_lot_of_elements():
    bst = BinarySearchTree()
    ring = Ring(3, bst)
    for i in range(1_000_000):
        ring.insert(f'key{i}', f'value{i}')
    for i in range(1_000_000):
        assert ring.get(f'key{i}') == f'value{i}'

def get_non_existent_element():
    bst = BinarySearchTree()
    ring = Ring(2, bst)
    # ring.insert('key', 'value')
    with pytest.raises(KeyNotFoundError):
        ring.get('key')

def update_non_existent_element():
    bst = BinarySearchTree()
    ring = Ring(2, bst)
    # ring.insert('key', 'value')
    with pytest.raises(KeyNotFoundError):
        ring.update('key', 'new_value')

def delete_non_existent_element():
    bst = BinarySearchTree()
    ring = Ring(2, bst)
    # ring.insert('key', 'value')
    with pytest.raises(KeyNotFoundError):
        ring.delete('key')
    
    
