from consistent_hash_ring.node import Node
from typing import Any
import math
import hashlib
import logging

logger = logging.getLogger(__name__)

class HashTableDistributor:
    def __init__(self, nodes_count: int, node_capacity: int, node_threshold: int) -> None:
        self.nodes_count = nodes_count
        self.table_nodes = [Node(i, node_capacity) for i in range(nodes_count)]
        self.node_threshold = node_threshold
        self.elements_count = 0
        self.node_capacity = node_capacity

    def insert(self, key: str, value: Any | None = None) -> int:
        key_hash = self._hash(key)
        if value is None:
            value = key
        if self.table_nodes[key_hash].load + 1 >= self.node_threshold:
            logger.info(f'Node {key_hash} reached it\'s threshold and system will scale up') 
            self.add_nodes(1)
        logger.debug(f'Inserting element {key} in node {key_hash}')
        self.table_nodes[key_hash].insert(key, value)                                  # Hash will be used only for finding the node
        self.elements_count += 1
        return key_hash

    def delete(self, key: str) -> bool:
        key_hash = self._hash(key)
        if key in self.table_nodes[key_hash]:
            self.table_nodes[key_hash].delete(key)
            logger.debug(f'Element {key} was deleted from node {key_hash}')
            return True
        self.elements_count -= 1
        logger.debug(f'Element {key} was not found in node {key_hash}')
        return False

    def get(self, key: str) -> Any | None:
        key_hash = self._hash(key)
        if key in self.table_nodes[key_hash]:
            return self.table_nodes[key_hash].get(key)
        return None

    def update(self, key: str, value: Any) -> None:
        key_hash = self._hash(key)
        self.table_nodes[key_hash].update(key, value)

    def _hash(self, key: str) -> int:
        return int(hashlib.sha1(key.encode()).hexdigest(), 16) % self.nodes_count
    
    def add_nodes(self, nodes_to_add: int) -> None:
        """
        Add nodes_to_add to the hash table and rehash the elements
        """
        logger.info(f'Adding {nodes_to_add} nodes to the hash table')
        self.nodes_count += nodes_to_add
        self.table_nodes = self._rehash_table()

    def remove_nodes(self, nodes_to_remove: int) -> None:
        """
        Removes nodes_to_add to the hash table and rehash the elements
        """
        logger.info(f'Removing {nodes_to_remove} nodes to the hash table')
        self.nodes_count -= nodes_to_remove
        self.table_nodes = self._rehash_table()

        

    def _rehash_table(self) -> list[Node]:
        new_list = [Node(self.node_capacity) for _ in range(self.nodes_count)]
        for index, node in enumerate(self.table_nodes):
            node_elements = list(node.items())
            for element in node_elements:
                key, value = element
                new_list[self._hash(key)][key] = value
                logger.debug(f'Element {key} was rehashed to node {index}')
        logger.info(f'{self.elements_count} elements were rehashed')
        return new_list
            
    
    def __str__(self)->str:
        str_list = []
        log_nodes_count = int(math.log10(self.nodes_count))
        hash_colum_width = log_nodes_count if log_nodes_count > 18 else 18
        items_colum_width = 40
        node_column_width = 6
        key_column_width = 40
        line_divider = '+' + '-' * (node_column_width) + '+' + '-' * (hash_colum_width) + '+' + '-' * (key_column_width) + '+' + '-' * (items_colum_width) + '+'
        header = '|' + ' Node '.center(node_column_width) + '|' + ' hash '.center(hash_colum_width) + '|' + ' key '.center(key_column_width) + '|' + ' items '.center(items_colum_width) + '|'
        node_line_divider = '|' + ' ' * (node_column_width) + '+' + '-' * (hash_colum_width) + '+' + '-' * (key_column_width) + '+' + '-' * (items_colum_width) + '+'
        # header =            '| Node | hash             | key                | items                               |'
        str_list.append(line_divider)
        str_list.append(header)

        for index, node in enumerate(self.table_nodes):
            str_list.append(line_divider)
            node_elements = list(node.items())
            index_log = int(math.log10(index)) if index > 0 else 0

            if len(node.keys()) == 0:
                str_list.append('| ' + str(index) + ' ' * (4 - index_log) + '|' + ' ' * hash_colum_width + '|' + ' ' * key_column_width + '|' + ' ' * items_colum_width + '|')
                continue

            first_element_key, first_element_value = node_elements[0]
            str_list.append(f'| {index}' + ' ' * (4 - index_log) + '| ' + str(self._hash(first_element_key)).ljust(hash_colum_width - 1) + '| ' + first_element_key.ljust(key_column_width - 1) + '| ' + first_element_value.ljust(items_colum_width - 1) + '|')
            for element in node_elements[1:]:
                key, value = element
                str_list.append(node_line_divider)
                str_list.append(f'|      | ' + str(self._hash(key)).ljust(hash_colum_width - 1) + '| ' + key.ljust(key_column_width - 1) + '| ' + value.ljust(items_colum_width - 1) + '|')

        str_list.append(line_divider)
        return '\n'.join(str_list)

        

def main():
    items = ['augusto', 'gaby', 'cacau', 'mingau', 'lilit', 'panquenca', 'outrinha', 'jiji', 'gata veia', 'gata nova', 'gata', 'gatão', 'cindy', 'zebra', 'frajola']
    htd = HashTableDistributor()
    for item in items:
        htd.insert(item)

    # for node in htd.table_nodes:
    #     print(node)
    print(str(htd))

if __name__ == '__main__':
    main()