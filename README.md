# `UDPSocket.receive()` raises `BrokenResourceError` on a Windows ICMP port-unreachable (trio backend), inconsistent with asyncio

### Summary

On Windows, sending a UDP datagram to an address with nothing bound to it causes an
ICMP "port unreachable" to be associated with the sending socket, and the socket's
next `recvfrom()` fails with `WSAECONNRESET` (`WinError 10054`). This is a well-known
Windows UDP quirk: the error is a per-datagram notification and the socket remains
fully usable.

The two backends handle this differently:

- **asyncio backend**: the ICMP error is delivered to `DatagramProtocol.error_received`.
  `receive()` later raises `BrokenResourceError` from the stored exception.
- **trio backend**: `recvfrom()` raises `ConnectionResetError`, which
  `_convert_socket_error` turns into `anyio.BrokenResourceError`, propagating straight
  out of `UDPSocket.receive()`.

So the same underlying socket condition surfaces as `BrokenResourceError` on both
backends. The problem is that by anyio's own contract this signals a permanently
unusable resource, even though the socket is fine and the very next `receive()`
succeeds. A datagram server that serves many peers off one socket has no robust,
portable way to tell "one peer's datagram bounced, keep going" from "the socket is
genuinely broken."

### Environment

- anyio 4.14.2 (code is unchanged on current `main`)
- trio 0.31.0 (also reproduced on trio 0.33.0)
- Python 3.11 / 3.12
- Windows (loopback and real interfaces)

### Observed traceback

This is from a real HTTP/3 (QUIC-over-UDP) server test suite. A client closed its
socket after a request; the server's reply datagram bounced, and the server's next
`receive()` on its single listening socket raised, taking the whole listener down:

```
  File ".../src/udp_server.py", line 64, in run
    data, address = await self.socket.receive()
  File ".../anyio/_backends/_trio.py", line 619, in receive
    data, addr = await self._trio_socket.recvfrom(65536)
    ...
ConnectionResetError: [WinError 10054] An existing connection was forcibly closed by the remote host

The above exception was the direct cause of the following exception:

  File ".../anyio/_backends/_trio.py", line 622, in receive
    self._convert_socket_error(exc)
  File ".../anyio/_backends/_trio.py", line 471, in _convert_socket_error
    raise BrokenResourceError from exc
anyio.BrokenResourceError
```

### Reproduction

> Note: this script is synthesized from the traceback above plus a reading of both
> backends; it needs a Windows host to trigger `WSAECONNRESET` (Linux does not deliver
> ICMP errors to an unconnected UDP socket's `recv`). The behavior divergence itself is
> confirmed from the anyio source, cited below.

```python
import socket

import anyio
from anyio.abc import SocketAttribute


async def main() -> None:
    errors = []
    async with await anyio.create_udp_socket(
        family=socket.AF_INET, local_host="127.0.0.1"
    ) as udp:
        # An address on loopback with nothing bound to it.
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe.bind(("127.0.0.1", 0))
        dead_host, dead_port = probe.getsockname()
        probe.close()

        # Provokes an ICMP port-unreachable back to `udp` on Windows.
        await udp.sendto(b"ping", dead_host, dead_port)

        # Windows + trio:    raises anyio.BrokenResourceError (WinError 10054),
        #                    even though `udp` is still perfectly usable.
        # Windows + asyncio: no error; this times out waiting for real data.
        try:
            with anyio.fail_after(2):
                await udp.receive()
        except TimeoutError:
            print("timeout out correctly 1")
        except anyio.BrokenResourceError as e:
            errors.append(e)
        else:
            errors.append(AssertionError("did not timeout"))

        try:
            with anyio.fail_after(2):
                await udp.receive()
        except TimeoutError:
            print("timeout out correctly 2")
        except anyio.BrokenResourceError as e:
            errors.append(e)
        else:
            errors.append(AssertionError("did not timeout"))

        local_host, local_port = udp.extra(SocketAttribute.local_address)

        server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            server.bind((dead_host, dead_port))
            server.sendto(b"pong", (local_host, local_port))
        finally:
            server.close()

        try:
            data, addr = await udp.receive()
        except Exception as e:
            errors.append(e)
        else:
            print(f"recieved {data}")
            if data != b"pong":
                errors.append(AssertionError(f"unexpected payload: {data!r}"))
            if addr != (dead_host, dead_port):
                errors.append(AssertionError(f"unexpected peer: {addr!r}"))

    if errors:
        raise ExceptionGroup("socket errors", errors)


anyio.run(main, backend="trio")   # vs. backend="asyncio"
```

Run with `backend="trio"` → `BrokenResourceError`.
Run with `backend="asyncio"` → the error is swallowed; `fail_after` times out instead.

### Where the divergence lives

**asyncio** — `_backends/_asyncio.py`:

```python
class DatagramProtocol(asyncio.DatagramProtocol):
    def error_received(self, exc: Exception) -> None:
        self.exception = exc

class UDPSocket(abc.UDPSocket):
    async def receive(self) -> tuple[bytes, IPSockAddrType]:
        with self._receive_guard:
            ...
            if not self._protocol.read_queue and not self._transport.is_closing():
                self._protocol.read_event.clear()
                await self._protocol.read_event.wait()
            try:
                return self._protocol.read_queue.popleft()
            except IndexError:
                if self._protocol.exception is not None:
                    raise BrokenResourceError from self._protocol.exception
                ...
```

So asyncio converts the deferred datagram error into `BrokenResourceError` too.

**trio** — `_backends/_trio.py`:

```python
class UDPSocket(_TrioSocketMixin[IPSockAddrType], abc.UDPSocket):
    async def receive(self) -> tuple[bytes, IPSockAddrType]:
        with self._receive_guard:
            try:
                data, addr = await self._trio_socket.recvfrom(65536)
                return data, convert_ipv6_sockaddr(addr)
            except BaseException as exc:
                self._convert_socket_error(exc)   # OSError -> BrokenResourceError

    def _convert_socket_error(self, exc):
        if isinstance(exc, trio.ClosedResourceError):
            raise ClosedResourceError from exc
        elif self._trio_socket.fileno() < 0 and self._closed:
            raise ClosedResourceError from None
        elif isinstance(exc, OSError):            # ConnectionResetError lands here
            raise BrokenResourceError from exc
        ...
```

`_convert_socket_error` neither closes the socket nor sets `_closed`, so the resource
is not actually broken — confirming this is a spurious `BrokenResourceError` rather
than a genuine one.

### Expected behavior

`UDPSocket.receive()` should not raise `BrokenResourceError` for a transient,
per-datagram `WSAECONNRESET` on an otherwise usable UDP socket.

### Possible fixes

1. **Ignore Windows UDP port-unreachable in `receive()`.** In both backends, when an
   unconnected UDP socket hits Windows `WSAECONNRESET` / `ConnectionResetError`, treat
   it as a one-shot datagram notification and continue waiting for the next real
   datagram instead of converting it to `BrokenResourceError`.

2. **Filter it before it becomes a resource-level error.** In asyncio, that means not
   turning the exception captured by `error_received()` into `BrokenResourceError` for
   this specific case. In trio, that means not routing this `ConnectionResetError`
   through `_convert_socket_error()` as a fatal socket failure.

### Workaround (for reference)

Downstream, this can be caught and treated as "drop this datagram, keep reading",
scoped to Windows and a `ConnectionResetError` cause so genuine `BrokenResourceError`s
still propagate:

```python
async def receive(self):
    while True:
        try:
            return await self._socket.receive()
        except anyio.BrokenResourceError as error:
            if sys.platform != "win32" or not isinstance(
                error.__cause__, ConnectionResetError
            ):
                raise
```

This relies on the trio backend not latching a broken state for the `OSError` path
(verified above) — a portable, backend-guaranteed fix in anyio would be preferable.

### See also

Prior Windows UDP work in anyio (`aclose()` hanging on the proactor loop):
agronholm/anyio#1237.
