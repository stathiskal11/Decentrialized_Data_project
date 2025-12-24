from src.chord.network import ChordNetwork

def test_chord_stub_runs():
    net = ChordNetwork()
    net.build(20, seed=0)
    assert net.insert("A", {"popularity": 1}) == 0
    v, h = net.lookup("A")
    assert h == 0
