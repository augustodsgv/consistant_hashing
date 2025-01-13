from typing import Any

class Node:
    """
    Node class is just an abstraction of a dictionary.
    This can represent an node of a database, machine, etc.
    """
    def __init__(self, node_index: int, capacity: str) -> None:
        self.capacity = capacity
        self.node_index = node_index
        self.data = dict()

    def is_full(self) -> bool:
        return len(self.data) >= self.capacity
    
    def insert(self, key: str, value: Any) -> None:
        self.data[key] = value

    def delete(self, key: str) -> Any | None:
        if not self.has_key(key):
            return None
        return self.data.pop(key)

    def get(self, key: str) -> Any | None:
        return self.data[key]
    
    def update(self, key: str, value: Any) -> None:
        self.data[key] = value

    def has_key(self, key: str) -> bool:
        return key in self.data
    
    def import_keys(self, other_node)->None:
        """
        Imports data from other dict.
        TODO: Implement error while trying to copy more data than capacity
        """
        if len(self.data) + len(other_node.data) > self.capacity:
            return False
        self.data = self.data | other_node.data
        return True

    
    @property
    def load(self) -> float:
        return len(self.data) / self.capacity

    