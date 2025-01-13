from faker import Faker
# from hash_table.hash_table import HashTableDistributor
from consistent_hash_ring.ring import Ring
import logging
import time
fake = Faker()

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def main():
    items = ['augusto', 'gaby', 'cacau', 'mingau', 'lilit', 'panquenca', 'outrinha', 'jiji', 'gata veia', 'gata nova', 'gata', 'gatão', 'cindy', 'zebra', 'frajola']
    consistant_hash_ring = Ring(3, 5, 2)
    for item in items:
        consistant_hash_ring.insert_key(item, item)

    for ring_node_set in consistant_hash_ring.ring.inorder():
        key, node_set = ring_node_set
        print(key, node_set)
        for node in node_set.list_nodes():
            for item in node.list_items():
                print(f'node_set: {node_set.index} node: {node.index}, value: {item}')
        # print(node[1].list_nodes()[0].list())


if __name__ == '__main__':
    main()