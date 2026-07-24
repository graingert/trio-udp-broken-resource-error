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
