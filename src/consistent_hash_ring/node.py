from typing import Any
from src.hash import hash

class Node:
    """
    Node class is just an abstraction of a dictionary.
    This can represent an node of a database, machine, etc.
    """
    def __init__(self, node_index: int, capacity: int, replica_count: int = 1) -> None:
        self.replica_count = replica_count
        self.capacity = capacity
        self.index = node_index
        self.ready = False
        self.data = dict()
        self.keys_to_delete = list()

    def is_full(self) -> bool:
        return len(self.data) >= self.capacity
    
    def set_node_ready(self) -> None:
        self.ready = True

    def set_node_not_ready(self) -> None:
        self.ready = False  

    def insert(self, key: str, value: Any) -> None:
        self.data[key] = value

    def delete(self, key: str) -> Any | None:
        if not self.has_key(key):
            return None
        return self.data.pop(key)

    def get(self, key: str) -> Any | None:
        if not self.has_key(key):
            return None
        return self.data[key]
    
    def update(self, key: str, new_value: Any) -> None:
        self.data[key] = new_value

    def has_key(self, key: str) -> bool:
        return key in self.data
    
    def list_items(self)->dict | None:
        return list(self.data.items())

    def clean_keys(self)->None:
        for key in self.keys_to_delete:
            self.delete(key)
        self.keys_to_delete = list()
        
    def export_keys(self, other_node, first_key_hash: int)->None:
        """
        Exports all keys with hash equal or greater than first_key_hash to the other node
        """
        for key in self.data.keys():
            if first_key_hash <= hash(key):
                value = self.data[key]
                other_node.insert(key, value)     # Import and delete the key from the other node
                self.keys_to_delete.append(key)
        self.clean_keys()

    def calc_mid_hash(self)->int:
        """
        Calculates the mean of the hash all keys of the node
        """
        
        return sum([hash(key) for key in self.data.keys()]) // len(self.data.keys())
    
    def __str__(self) -> str:
        base_str = []
        for key, value in self.data.items():
            base_str.append(f'{key}: {value}')

        return ' '.join(base_str)

    @property
    def load(self) -> float:
        return len(self.data) / self.capacity

    