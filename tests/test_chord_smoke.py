from src.chord.network import ChordNetwork

def test_chord_runs_basic_ops():
    net = ChordNetwork()
    net.build(20, seed=0)

    h_ins = net.insert("A", {"popularity": 1})
    assert isinstance(h_ins, int) and h_ins >= 0

    val, h_lu = net.lookup("A")
    assert val is not None
    assert val.get("popularity") == 1
    assert isinstance(h_lu, int) and h_lu >= 0

    h_upd = net.update("A", {"popularity": 2})
    assert isinstance(h_upd, int) and h_upd >= 0

    val2, h_lu2 = net.lookup("A")
    assert val2 is not None
    assert val2.get("popularity") == 2
    assert isinstance(h_lu2, int) and h_lu2 >= 0

    h_del = net.delete("A")
    assert isinstance(h_del, int) and h_del >= 0

    val3, h_lu3 = net.lookup("A")
    assert val3 is None
    assert isinstance(h_lu3, int) and h_lu3 >= 0
