from typing import Any
from .node import Node
from src.adt.abstract_data_type import AbstractDataType
from src.hash import hash
from .errors.node_errors import KeyNotFoundError, NodeIsFullError 
from .errors.ring_errors import NodeNotFoundError

import logging
logger = logging.getLogger(__name__)

class Ring:
    def __init__(self, node_capacity: int, node_min_load: int, node_max_load: int, adt: AbstractDataType)->None:
        self.node_capacity = node_capacity
        self.node_min_load = node_min_load
        self.node_max_load = node_max_load
        self.ring = adt
        new_node = Node(0, node_capacity)
        self.ring.insert(0, new_node)        

    def insert(self, key: str, value):
        """
        Insert an element in the ring.
        If a node is full, it will create a new node and distribute the keys
        """
        key_hash = hash(key)
        node_to_insert: Node = self._find_node(key_hash)

        if node_to_insert.load() >= self.node_max_load:
            logger.info(f'Node {node_to_insert.index} id full: scaling up the ring')
            self._split_node(node_to_insert)
            node_to_insert = self._find_node(key_hash)

        node_to_insert.insert(key, value)        

    def get(self, key: str)->Any:
        """
        Returns the value of the key. Raises an exception if not found
        """
        key_hash = hash(key)
        node_set_to_search: Node = self._find_node(key_hash)
        if not node_set_to_search.has(key):
            raise KeyNotFoundError(f'Key {key} not found')
        return node_set_to_search.get(key)

    def update(self, key: str, new_value: Any)->None:
        """
        Updates the value of the key. Returns old object if update or None if didn't find
        """
        key_hash = hash(key)
        node_set_to_search: Node = self._find_node(key_hash)
        if not node_set_to_search.has(key):
            raise KeyNotFoundError(f'Key {key} not found')
        return node_set_to_search.update(key, new_value)
        
    def delete(self, key: str)->Any:
        """
        Deletes a item from the ring.
        If node become empty, it will be removed from the ring
        """
        key_hash = hash(key)
        node_to_search: Node = self._find_node(key_hash)
        if not node_to_search.has_key(key):
            raise KeyNotFoundError(f'Key {key} not found')

        removed_key = node_to_search.delete(key)
        if node_to_search.load() < self.node_min_load:
            logger.info(f'Node {node_to_search.index} is underloaded: scaling down the ring')
            self._delete_node(node_to_search.index)
            
        return removed_key
        
    def _find_node(self, hash: int)->Node:
        """
        Finds the nearest node to a hash, i.e. the greatest that is smaler than the provided hash
        """
        return self.ring.find_max_smaller_than(hash)
        
    def _split_node(self, node: Node)->None:
        """
        Splits the node in two, creating a new node with the new_node_index
        """
        node_mid_hash = node.calc_mid_hash()        # The new node will get half of the keys of the old node.
        new_node = Node(node_mid_hash, self.node_capacity)
        self.ring.insert(node_mid_hash, new_node)
        node.export_keys(new_node, node_mid_hash)

        logger.info(f'New node created with index {node_mid_hash}')

    def _delete_node(self, index: int):
        """
        Deletes a node and sends it's values to the previous node
        """
        # TODO: check if the prior node has capacity to receive the keys
        node_to_delete: Node = self.ring.search(index)
        if node_to_delete is None:
            logger.info(f'Node with index {index} not found to delete')
            raise NodeNotFoundError(f'Node with index {index} not found')
        prior_node: Node = self.ring.find_max_smaller_than(index)
        logger.info(f'Exporting Keys of node {index} to the node {prior_node.index}')
        node_to_delete.export_keys(prior_node, 0)
        self.ring.remove(index)
        logger.info(f'Node {index} removed from the ring successfully')

    def __str__(self)->str:
        base_str: list[str] = []
        for node in self.ring:
            base_str.append(f'node {node.index}: {str(node)}')
        return '\n'.join(base_str)
