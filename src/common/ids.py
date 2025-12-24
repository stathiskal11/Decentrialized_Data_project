from __future__ import annotations
import hashlib

ID_BITS = 128
ID_HEX_LEN = ID_BITS // 4  # 32 hex digits
MOD = 1 << ID_BITS

def hash_128(text: str) -> int:
    """
    Stable 128-bit ID using SHA-1 truncated (160 -> 128).
    Used both for node IDs (if desired) and key IDs (movie title).
    """
    h = hashlib.sha1(text.encode("utf-8")).hexdigest()
    return int(h[:ID_HEX_LEN], 16)

def to_hex128(x: int) -> str:
    return f"{x:0{ID_HEX_LEN}x}"

def common_prefix_len_hex(a: int, b: int) -> int:
    ah = to_hex128(a)
    bh = to_hex128(b)
    i = 0
    while i < ID_HEX_LEN and ah[i] == bh[i]:
        i += 1
    return i

def circ_dist(a: int, b: int) -> int:
    """Circular distance on modulo 2^ID_BITS ring."""
    d = (a - b) % MOD
    return min(d, (-d) % MOD)
