"""Byte-level read/write buffer used by TCP and HTTP layers."""


class ByteBuffer:
    """Append-only write buffer with consume-from-front reads."""

    def __init__(self) -> None:
        self._data = bytearray()

    def write(self, data: bytes) -> None:
        self._data.extend(data)

    def peek(self, n: int) -> bytes:
        return bytes(self._data[:n])

    def read(self, n: int) -> bytes:
        chunk = bytes(self._data[:n])
        del self._data[:n]
        return chunk

    def read_until(self, delimiter: bytes) -> bytes | None:
        idx = self._data.find(delimiter)
        if idx == -1:
            return None
        end = idx + len(delimiter)
        chunk = bytes(self._data[:end])
        del self._data[:end]
        return chunk

    def __len__(self) -> int:
        return len(self._data)
