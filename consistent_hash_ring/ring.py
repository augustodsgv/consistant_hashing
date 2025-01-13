from typing import Any
from consistent_hash_ring.node_set import NodeSet
from consistent_hash_ring.binary_search_tree import BinarySearchTree
import hashlib

class Ring:
    def __init__(self, replication_factor: int, max_node_count: int, node_capacity: int)->None:
        self.replication_factor = replication_factor
        self.max_node_count = max_node_count
        self.node_capacity = node_capacity
        
        self.ring = BinarySearchTree()
        new_node = NodeSet(0, replication_factor, node_capacity)
        self.ring.insert(0, new_node)
        
        self.node_count = 1

    def _hash(self, key)->int:
        return int(hashlib.sha1(key.encode()).hexdigest(), 16)

    def _find_node(self, hash: int)->int:
        # TODO: implement missing nodes less or greater then 
        return self.ring.find_max_smaller_than(hash)
    
    def insert_key(self, key: str, value):
        key_hash = self._hash(key)
        node_set_to_insert: NodeSet = self._find_node(key_hash)
        node_set_to_insert.insert(key, value)
        # TODO: Implement auto scale up

    def check_if_not_full(self)->bool:
        # TODO
        ...

    def create_node_set(self):
        # TODO:
        ...

    def delete_node_set(self, index: int):
        # TODO
        ...

    def search_key(self, key: str)->Any | None:
        key_hash = self._hash(key)
        node_set_to_search: NodeSet = self._find_node(key_hash)
        return node_set_to_search.get(key)

    def delete_key(self, key: str)->Any | None:
        key_hash = self._hash(key)
        node_set_to_search: NodeSet = self._find_node(key_hash)
        removed_key = node_set_to_search.delete(key)
        if removed_key is None:
            return None
        return removed_key
    
    def update_key(self, key: str, new_value: Any)->None:
        """
        Updates the value of the key. Returns Olde object if update or None if didn't find
        """
        key_hash = self._hash(key)
        node_set_to_search: NodeSet = self._find_node(key_hash)
        if not node_set_to_search.has_key(key):
            return None
        else:
            return node_set_to_search.update(key, new_value)

if __name__ == "__main__":
    ring = Ring(replication_factor=3, max_node_count=10, node_capacity=1)
    # Insert some keys
    ring.insert_key("key1", "value1")
    ring.insert_key("key2", "value2")
    ring.insert_key("key3", "value3")

    # Search for keys
    print(ring.search_key("key1"))  # Output: value1
    print(ring.search_key("key2"))  # Output: value2
    print(ring.search_key("key4"))  # Output: None
    
    # Update a key
    ring.update_key("key1", "new_value1")
    print(ring.search_key("key1"))  # Output: new_value1
    
    # Delete a key
    ring.delete_key("key2")
    print(ring.search_key("key2"))  # Output: None




    