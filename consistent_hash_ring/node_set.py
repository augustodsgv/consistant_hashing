from consistent_hash_ring.node import Node
from typing import Any


class NodeSet:
    def __init__(self, node_set_index: int, replication_factor: int, nodes_capacity: int) -> None:
        """
        A node will have replication_factor nodes, that will be
        node_set_index, node_set_index + 1, ..., node_set_index + replication_factor - 1
        """
        self.nodes: list[Node] = []
        self.index = node_set_index
        for i in range(replication_factor):
            node_index = node_set_index * replication_factor + i
            self.nodes.append(Node(node_index, nodes_capacity))
            
    def insert(self, key: str, value: Any) -> None:
        for node in self.nodes:
            node.insert(key, value)

    def get(self, key: str) -> Any | None:
        for node in self.nodes:
            if node.has_key(key):
                return node.get(key)
        return None
    
    def has_key(self, key: str) -> bool:
        for node in self.nodes:
            if node.has_key(key):
                return True
        return False
    
    def delete(self, key: str) -> None:
        if not self.has_key(key):
            return None
        for node in self.nodes:
            node.delete(key)
    
    def list_nodes(self)->list[Node]:
        return self.nodes

    def update(self, key: str, new_value: Any) -> None | Any:
        # TODO: implement not found error
        old_value = self.get(key)
        for node in self.nodes:
            node.update(key, new_value)
        return old_value

    def get_replica(self, node_index: int)->Node:
        return self.nodes[node_index]
    
    def import_keys(self, other_nodeset: 'NodeSet')->bool:
        for index, node in enumerate(other_nodeset):
            self.nodes[index].import_keys(node)
        return True

    @property
    def load(self) -> float:
        return sum([node.load for node in self.nodes]) / len(self.nodes)


def main():
    node_set = NodeSet(0, 3, 10)
    node_set = NodeSet(1, 3, 10)
    node_set = NodeSet(2, 3, 10)
    node_set = NodeSet(3, 3, 10)

if __name__ == '__main__':
    main()