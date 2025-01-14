from faker import Faker
from src.adt.binary_search_tree import BinarySearchTree
from src.consistent_hash_ring.ring import Ring
import logging
fake = Faker()

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def main():
    bst = BinarySearchTree()
    items = ['augusto', 'gaby', 'cacau', 'mingau', 'lilit', 'panquenca', 'outrinha', 'jiji', 'gata veia', 'gata nova', 'gata', 'gatão', 'cindy', 'zebra', 'frajola']
    consistant_hash_ring = Ring(2, bst)
    for item in items:
        consistant_hash_ring.insert(item, item)

    for ring_nodes in consistant_hash_ring.ring.inorder():
        key, node = ring_nodes
        print(key, node)
        for item in node.list_items():
            print(f'node: {node.index}, value: {item}')
    print(consistant_hash_ring.ring.print_tree())

if __name__ == '__main__':
    main()