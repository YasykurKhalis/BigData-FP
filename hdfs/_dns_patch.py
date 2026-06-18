"""LUMBUNG - DNS patch untuk WebHDFS redirect.

NameNode redirect WebHDFS write ke `http://datanode:9864/...` (hostname
container internal). Host Windows tidak bisa resolve. Patch socket
supaya hostname container -> 127.0.0.1 (port sudah di-expose ke host).

Apply via: `from hdfs._dns_patch import patch_dns; patch_dns()`
"""

import socket

CONTAINER_HOSTS = {
    "namenode": "127.0.0.1",
    "datanode": "127.0.0.1",
    "resourcemanager": "127.0.0.1",
    "nodemanager": "127.0.0.1",
    "historyserver": "127.0.0.1",
}

_orig_getaddrinfo = socket.getaddrinfo
_orig_gethostbyname = socket.gethostbyname


def _patched_getaddrinfo(host, *args, **kwargs):
    if isinstance(host, str) and host in CONTAINER_HOSTS:
        host = CONTAINER_HOSTS[host]
    return _orig_getaddrinfo(host, *args, **kwargs)


def _patched_gethostbyname(host):
    if isinstance(host, str) and host in CONTAINER_HOSTS:
        return CONTAINER_HOSTS[host]
    return _orig_gethostbyname(host)


def patch_dns():
    socket.getaddrinfo = _patched_getaddrinfo
    socket.gethostbyname = _patched_gethostbyname
