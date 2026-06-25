#!/usr/bin/env python3
"""Minimal HTTP CONNECT proxy for the real-world transport test.

Implements just enough of the HTTP CONNECT method to tunnel a WebSocket
(wss://) through to a backend, logging each CONNECT so you can confirm the
NATS client actually traversed the proxy rather than connecting directly.

This is a test aid, not a production proxy: no auth, no allow-list, single
file, blocking threads. Run as:  python3 connect-proxy.py [port]
"""

import select
import socket
import sys
import threading


def pipe(a: socket.socket, b: socket.socket) -> None:
    try:
        while True:
            r, _, _ = select.select([a, b], [], [])
            for s in r:
                data = s.recv(65536)
                if not data:
                    return
                (b if s is a else a).sendall(data)
    except OSError:
        pass
    finally:
        for s in (a, b):
            try:
                s.close()
            except OSError:
                pass


def handle(client: socket.socket) -> None:
    try:
        request = b""
        while b"\r\n\r\n" not in request:
            chunk = client.recv(4096)
            if not chunk:
                client.close()
                return
            request += chunk

        line = request.split(b"\r\n", 1)[0].decode("latin-1")
        method, target, _ = line.split(" ", 2)
        if method.upper() != "CONNECT":
            client.sendall(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
            client.close()
            return

        host, _, port = target.partition(":")
        port = int(port or "443")
        print(f"CONNECT {host}:{port}", flush=True)

        upstream = socket.create_connection((host, port))
        client.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        pipe(client, upstream)
    except Exception as exc:  # noqa: BLE001 - test aid, log and move on
        print(f"error: {exc}", flush=True)
        try:
            client.close()
        except OSError:
            pass


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8888
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", port))
    server.listen(64)
    print(f"CONNECT proxy listening on 127.0.0.1:{port}", flush=True)
    while True:
        client, _ = server.accept()
        threading.Thread(target=handle, args=(client,), daemon=True).start()


if __name__ == "__main__":
    main()
