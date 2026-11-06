#!/usr/bin/env python3
"""Kill a real broker owner at five acknowledged checkpoint-publication boundaries."""
from __future__ import annotations
import argparse
import os
from pathlib import Path
import selectors
import signal
import subprocess
import tempfile
import time


def read_marker(process: subprocess.Popen[bytes], timeout: float = 10.0) -> str:
    assert process.stdout is not None
    end = time.monotonic() + timeout
    data = bytearray()
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        while b"\n" not in data:
            remaining = end - time.monotonic()
            if remaining <= 0 or not selector.select(remaining):
                raise TimeoutError("child did not reach the publication gate")
            chunk = os.read(process.stdout.fileno(), 4096)
            if not chunk:
                raise RuntimeError(f"child exited before its gate: {process.poll()}")
            data.extend(chunk)
            if len(data) > 4096:
                raise RuntimeError("unexpectedly large marker output")
    return bytes(data).decode("utf-8").strip()


def check(binary: Path, root: Path, phase: str, expected: int) -> None:
    with tempfile.TemporaryFile(mode="w+b") as errors:
        process = subprocess.Popen([str(binary), "cut", str(root), phase],
            stdout=subprocess.PIPE, stderr=errors)
        try:
            marker = read_marker(process)
            if marker != f"READY {phase}":
                raise RuntimeError(f"wrong marker: {marker!r}")
            process.kill()  # SIGKILL on the native macOS/Unix target, not graceful shutdown.
            code = process.wait(timeout=5)
            if code != -signal.SIGKILL:
                raise RuntimeError(f"unexpected child status: {code}")
        except Exception:
            errors.seek(0)
            print(errors.read(16_384).decode("utf-8", errors="replace"))
            raise
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
            if process.stdout is not None:
                process.stdout.close()
    verified = subprocess.run([str(binary), "verify", str(root)],
        check=True, capture_output=True, text=True, timeout=15)
    wanted = f"RECOVERED 2 {expected} {expected}"
    if verified.stdout.strip() != wanted:
        raise AssertionError(f"{phase}: {verified.stdout!r}, expected {wanted!r}")
    print(f"{phase}: killed at gate; persisted={expected}; first_repeat={expected}; epoch=2")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    if os.name != "posix":
        parser.error("this process-kill check targets native macOS/Unix")
    with tempfile.TemporaryDirectory(prefix="progress-crash-") as parent:
        for phase, expected in [("created", 0), ("written", 0), ("synced", 0),
                                ("renamed", 2), ("directory", 2)]:
            check(binary, Path(parent) / phase, phase, expected)


if __name__ == "__main__":
    main()
