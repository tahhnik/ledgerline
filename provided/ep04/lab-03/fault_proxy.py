#!/usr/bin/env python3
"""Kafka TCP proxy that can discard exactly one broker response.

Normal traffic is copied between public port 9094 and the broker backend on
127.0.0.1:9095. Creating /run/ep04-lab03-drop-next makes the proxy discard the
next backend response and reset that connection. This models a committed write
whose acknowledgement never reaches the producer.
"""
from __future__ import annotations

import socket
import threading
from pathlib import Path

FLAG = Path("/run/ep04-lab03-drop-next")
DROPPED = Path("/run/ep04-lab03-response-dropped")
LISTEN = ("0.0.0.0", 9094)
BACKEND = ("127.0.0.1", 9095)


def close(sock: socket.socket) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass


def handle(front: socket.socket) -> None:
    back = socket.create_connection(BACKEND, timeout=10)
    front.settimeout(None)
    back.settimeout(None)
    stopped = threading.Event()

    def copy(src: socket.socket, dst: socket.socket, downstream: bool) -> None:
        try:
            while not stopped.is_set():
                data = src.recv(65536)
                if not data:
                    break
                if downstream and FLAG.exists():
                    try:
                        FLAG.unlink()
                    except FileNotFoundError:
                        pass
                    DROPPED.write_text("dropped\n")
                    stopped.set()
                    break
                dst.sendall(data)
        except OSError:
            pass
        finally:
            stopped.set()
            close(front)
            close(back)

    threading.Thread(target=copy, args=(front, back, False), daemon=True).start()
    threading.Thread(target=copy, args=(back, front, True), daemon=True).start()


def main() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(LISTEN)
        server.listen(128)
        while True:
            client, _ = server.accept()
            threading.Thread(target=handle, args=(client,), daemon=True).start()


if __name__ == "__main__":
    main()
