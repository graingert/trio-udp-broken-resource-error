# `BrokenResourceError` is the wrong error for a temporary Windows UDP reset

## Summary

On Windows, sending a UDP datagram to an address with nothing bound to it can cause a
later receive on the same socket to observe `WSAECONNRESET` (`WinError 10054`).

The important part is what [demo.py](/home/graingert/projects/trio-udp-broken-resource-error/demo.py:1)
shows after that happens:

- the socket still works
- a later `receive()` can still time out normally
- once a real peer starts sending, the same socket can still receive datagrams

That means this is a temporary per-datagram condition, not a terminal resource
failure. Raising `anyio.BrokenResourceError` is therefore misleading, because
`BrokenResourceError` implies the resource is no longer usable.

## Reproduction

`demo.py` does three things in sequence:

1. Sends `ping` to a loopback UDP port with nothing bound to it.
2. Calls `udp.receive()` twice under `fail_after(2)`.
3. Starts a UDP server on that same port, sends `pong`, and confirms `udp.receive()`
   gets it.

Current script:

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


anyio.run(main, backend="trio")
```

## What This Demonstrates

If the socket were actually broken, the second timeout and final receive would not
work. But the intended successful path is:

```text
timeout out correctly 1
timeout out correctly 2
recieved b'pong'
```

That outcome matters more than the transient Windows socket error itself. The socket
continues to behave correctly after the ICMP port-unreachable condition, so the error
should not be surfaced as `BrokenResourceError`.

## Why Ubuntu Does Not Reproduce It

Linux does not normally report this condition the same way on an unconnected UDP
socket. The later `recvfrom()` typically just keeps waiting for real datagrams instead
of failing with `ConnectionResetError`. So this repository is specifically about the
Windows UDP behavior.

## Why `BrokenResourceError` Is Wrong

`BrokenResourceError` means the resource is no longer usable. That is not true here.

After the temporary Windows UDP reset:

- the socket remains open
- later receives can still block and time out normally
- later receives can still return valid datagrams

So even if AnyIO wants to preserve visibility of the underlying Windows condition, it
should not classify it as a permanently broken resource.

## Expected Behavior

On Windows, an ICMP port-unreachable for an unconnected UDP socket should be treated
as a temporary datagram-level event. `UDPSocket.receive()` should continue waiting for
real traffic instead of raising `BrokenResourceError`.

## Possible Fixes

1. Ignore this specific Windows UDP reset in `receive()` and keep waiting for the next
   datagram.
2. Surface it as a different, non-terminal exception that does not imply the socket is
   permanently unusable.

The key requirement is semantic: a temporary error state must not be reported as a
terminal resource failure.

## Real-World Impact

A UDP server often serves many peers from one listening socket. One peer causing a
Windows ICMP port-unreachable should not make the application think the shared socket
is dead when it can still receive future traffic from other peers.
