from src.pastry.network import PastryNetwork

def test_pastry_basic_ops():
    net = PastryNetwork()
    net.build(30, seed=1)

    h1 = net.insert("Inception", {"popularity": 99})
    v, h2 = net.lookup("Inception")
    assert v is not None and v["popularity"] == 99
    assert isinstance(h1, int) and isinstance(h2, int)

    h3 = net.update("Inception", {"popularity": 100})
    v2, _ = net.lookup("Inception")
    assert v2 is not None and v2["popularity"] == 100
    assert isinstance(h3, int)

    h4 = net.delete("Inception")
    v3, _ = net.lookup("Inception")
    assert v3 is None
    assert isinstance(h4, int)
