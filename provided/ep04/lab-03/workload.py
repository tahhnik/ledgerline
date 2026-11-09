"""Deterministic Ledgerline workload and Kafka's Java-compatible murmur2."""
from __future__ import annotations

from collections.abc import Iterable


def murmur2(data: bytes) -> int:
    length = len(data)
    m, r = 0x5BD1E995, 24
    h = (0x9747B28C ^ length) & 0xFFFFFFFF
    for i in range(length // 4):
        k = int.from_bytes(data[i * 4:i * 4 + 4], "little")
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    tail, rem = length & ~3, length % 4
    if rem == 3:
        h ^= data[tail + 2] << 16
    if rem >= 2:
        h ^= data[tail + 1] << 8
    if rem >= 1:
        h ^= data[tail]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h


def java_partition(key: str, partitions: int) -> int:
    return (murmur2(key.encode()) & 0x7FFFFFFF) % partitions


def transactions(count: int = 1200) -> list[dict]:
    """Half the traffic belongs to one merchant; cards remain high-cardinality."""
    rows = []
    for i in range(count):
        rows.append({
            "event_id": f"evt_{i:08d}",
            "card_id": f"card_{i % 200:04d}",
            "merchant_id": "merchant_hot" if i < count // 2 else f"merchant_{i % 50:03d}",
            "amount": f"{10 + (i % 900) / 100:.2f}",
            "currency": "USD",
            "event_time": f"2026-11-09T10:{(i // 60) % 60:02d}:{i % 60:02d}+00:00",
        })
    return rows


def distribution(events: Iterable[dict], key_field: str, partitions: int = 6) -> dict[int, int]:
    counts = {partition: 0 for partition in range(partitions)}
    for event in events:
        counts[java_partition(event[key_field], partitions)] += 1
    return counts
