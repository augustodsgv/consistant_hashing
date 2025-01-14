from typing import Any
from src.consistent_hash_ring.node import Node
from src.adt.abstract_data_type import AbstractDataType
from src.hash.hash import hash

import logging
logger = logging.getLogger(__name__)

class Ring:
    def __init__(self, node_capacity: int, adt: AbstractDataType)->None:
        self.node_capacity = node_capacity
        self.ring = adt
        new_node = Node(0, node_capacity)
        self.ring.insert(0, new_node)        

    def _find_node(self, hash: int)->Node:
        """
        Finds the nearest node, i.e. the greatest that is smaler than the provided hash
        """
        return self.ring.find_max_smaller_than(hash)
    
    def insert(self, key: str, value):
        """
        Insert an element in the ring.
        If a node is full, it will create a new node and distribute the keys
        """
        key_hash = hash(key)
        node_to_insert: Node = self._find_node(key_hash)
        # print(node_to_insert)

        if node_to_insert.is_full():
            logger.info(f'Node {node_to_insert.index} id full: scaling up the ring')
            node_mid_hash = node_to_insert.calc_mid_hash()        # The new node will get half of the keys of the old node.
            new_node = Node(node_mid_hash, self.node_capacity)
            self.ring.insert(node_mid_hash, new_node)
            logger.info(f'New node created with index {node_mid_hash}')

            node_to_insert.export_keys(new_node, node_mid_hash)
            node_to_insert = self._find_node(key_hash)
            
        node_to_insert.insert(key, value)        
        

    def delete_node(self, index: int):
        # TODO
        ...

    def get(self, key: str)->Any | None:
        key_hash = hash(key)
        node_set_to_search: Node = self._find_node(key_hash)
        return node_set_to_search.get(key)

    def delete(self, key: str)->Any | None:
        key_hash = hash(key)
        node_set_to_search: Node = self._find_node(key_hash)
        removed_key = node_set_to_search.delete(key)
        if removed_key is None:
            return None
        return removed_key
    
    def update(self, key: str, new_value: Any)->None:
        """
        Updates the value of the key. Returns old object if update or None if didn't find
        """
        key_hash = hash(key)
        node_set_to_search: Node = self._find_node(key_hash)
        if not node_set_to_search.has(key):
            return None
        else:
            return node_set_to_search.update(key, new_value)

    def __str__(self)->str:
        base_str: list[str] = []
        for node in self.ring:
            base_str.append(f'node {node.index}: {str(node)}')
        return '\n'.join(base_str)
