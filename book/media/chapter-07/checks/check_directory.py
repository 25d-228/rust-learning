#!/usr/bin/env python3
"""Drive the Rust lock probe on macOS/Unix; no storage logic is implemented here."""
import argparse
import os
from pathlib import Path
import select
import subprocess
import tempfile
import time


def ready_line(process: subprocess.Popen, seconds: float = 5.0) -> bytes:
    deadline = time.monotonic() + seconds
    data = bytearray()
    while b"\n" not in data:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([process.stdout], [], [], remaining)[0]:
            raise TimeoutError("owner did not report readiness")
        chunk = os.read(process.stdout.fileno(), 256)
        if not chunk:
            raise RuntimeError("owner exited before reporting readiness")
        data.extend(chunk)
        if len(data) > 256:
            raise RuntimeError("unexpected readiness output")
    return bytes(data)


def contender(binary: Path, root: Path, expected_code: int, expected: bytes) -> None:
    result = subprocess.run([str(binary), str(root), "try"],
                            capture_output=True, timeout=5, check=False)
    assert result.returncode == expected_code, (result.returncode, result.stderr)
    assert result.stdout == expected, result.stdout


def exercise(binary: Path, terminate: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="broker-lock-") as name:
        root = Path(name)
        lock = root / ".broker.lock"
        lock.write_bytes(b"sentinel")
        owner = subprocess.Popen([str(binary), str(root), "hold"],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, bufsize=0)
        try:
            assert ready_line(owner) == b"locked\n"
            contender(binary, root, 2, b"busy\n")
            if terminate:
                owner.kill()
            else:
                owner.stdin.close()  # EOF releases the gate, then Rust drops the guard.
            status = owner.wait(timeout=5)
            assert (status < 0) if terminate else (status == 0), status
            contender(binary, root, 0, b"locked\n")
            assert lock.read_bytes() == b"sentinel"
        finally:
            if owner.poll() is None:
                owner.kill()
            owner.wait(timeout=5)
            for stream in (owner.stdin, owner.stdout, owner.stderr):
                stream.close()
    print("kill: busy, exited, reacquired" if terminate else "normal: busy, exited, reacquired")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    exercise(binary, terminate=False)
    exercise(binary, terminate=True)


if __name__ == "__main__":
    main()
