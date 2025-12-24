def test_imports():
    from src.pastry.network import PastryNetwork
    from src.chord.network import ChordNetwork
    assert PastryNetwork is not None
    assert ChordNetwork is not None
