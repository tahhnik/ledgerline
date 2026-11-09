"""Port of Kafka's Java partitioner (Utils.murmur2 + toPositive) for verification."""


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


def java_partition(key: str, num_partitions: int) -> int:
    return (murmur2(key.encode()) & 0x7FFFFFFF) % num_partitions
