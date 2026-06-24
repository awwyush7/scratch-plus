"""Phase 0 smoke tests — verify the package imports and basic utilities work."""
from scratchplus.buffer import ByteBuffer
from scratchplus.helpers import now_ms
from scratchplus.log import get_logger


def test_byte_buffer_write_read():
    buf = ByteBuffer()
    buf.write(b"hello")
    assert buf.read(3) == b"hel"
    assert buf.read(2) == b"lo"
    assert len(buf) == 0


def test_byte_buffer_read_until():
    buf = ByteBuffer()
    buf.write(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
    line = buf.read_until(b"\r\n")
    assert line == b"GET / HTTP/1.1\r\n"
    assert len(buf) == len(b"Host: localhost\r\n")


def test_byte_buffer_peek_does_not_consume():
    buf = ByteBuffer()
    buf.write(b"abc")
    assert buf.peek(2) == b"ab"
    assert len(buf) == 3


def test_now_ms_increases():
    import time
    t1 = now_ms()
    time.sleep(0.01)
    t2 = now_ms()
    assert t2 > t1


def test_get_logger_returns_logger():
    import logging
    logger = get_logger("test")
    assert isinstance(logger, logging.Logger)
