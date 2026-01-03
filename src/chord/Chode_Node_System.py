class ChordNode:
    def __init__(self, node_id, m_bits):
        self.id = node_id
        self.m = m_bits
        self.mod = 2 ** m_bits
        self.successor = self
        self.predecessor = None
        self.finger = [None] * m_bits

    def in_interval(self,x, a, b, modulo):
        if a < b:
            return a < x <= b
        else:  # wrap-around
            return x > a or x <= b

    def closest_preceding_finger(self, key):
        for i in reversed(range(self.m)):
            finger_id = self.finger[i].id if self.finger[i] else None
            if finger_id and self.in_interval(finger_id, self.id, key, self.mod):
                return self.finger[i]
        return self

    def find_predecessor(self, key):
        node = self
        while not self.in_interval(key, node.id, node.successor.id, self.mod):
            node = node.closest_preceding_finger(key)
        return node

    def find_successor(self, key):
        pred = self.find_predecessor(key)
        return pred.successor

def setup_chord_network():
    m = 3  # ID space size 2^m = 8
    # Create 4 nodes with IDs: 1, 3, 5, 7
    node1 = ChordNode(1, m)
    node3 = ChordNode(3, m)
    node5 = ChordNode(5, m)
    node7 = ChordNode(7, m)

    # Manually set successors and predecessors
    node1.successor = node3
    node1.predecessor = node7

    node3.successor = node5
    node3.predecessor = node1

    node5.successor = node7
    node5.predecessor = node3

    node7.successor = node1
    node7.predecessor = node5

    # Optional: finger table initialization (simplified)
    node1.finger = [node3, node5, node7]
    node3.finger = [node5, node7, node1]
    node5.finger = [node7, node1, node3]
    node7.finger = [node1, node3, node5]

    return node1, node3, node5, node7

# --- Test find_successor ---
node1, node3, node5, node7 = setup_chord_network()

test_keys = [0, 1, 2, 3, 4, 5, 6, 7]

for key in test_keys:
    succ = node1.find_successor(key)
    print(f"Key {key} → Successor: Node {succ.id}")
