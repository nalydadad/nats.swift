#!/usr/bin/env python3
"""Minimal HTTP CONNECT proxy for CI verification of WebSocket-over-proxy.

Logs each CONNECT target to stdout so the test can assert that traffic was
actually tunnelled through the proxy (rather than connecting directly).
"""
import socket
import sys
import threading

LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = 8888


def pipe(src, dst):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def handle(client):
    try:
        request = b""
        while b"\r\n\r\n" not in request:
            chunk = client.recv(4096)
            if not chunk:
                client.close()
                return
            request += chunk
        line = request.split(b"\r\n", 1)[0].decode("latin-1")
        parts = line.split()
        if len(parts) < 2 or parts[0].upper() != "CONNECT":
            client.sendall(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
            client.close()
            return
        target = parts[1]
        print(target, flush=True)  # logged for the test assertion
        host, _, port = target.rpartition(":")
        try:
            upstream = socket.create_connection((host, int(port)), timeout=10)
        except OSError as exc:
            sys.stderr.write(f"upstream connect failed: {exc}\n")
            client.sendall(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            client.close()
            return
        client.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
        threading.Thread(target=pipe, args=(client, upstream), daemon=True).start()
        pipe(upstream, client)
    except OSError as exc:
        sys.stderr.write(f"handler error: {exc}\n")
        try:
            client.close()
        except OSError:
            pass


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((LISTEN_HOST, LISTEN_PORT))
    server.listen(64)
    sys.stderr.write(f"connect-proxy listening on {LISTEN_HOST}:{LISTEN_PORT}\n")
    sys.stderr.flush()
    while True:
        client, _ = server.accept()
        threading.Thread(target=handle, args=(client,), daemon=True).start()


if __name__ == "__main__":
    main()
