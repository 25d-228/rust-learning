#!/usr/bin/env python3
"""Kill the Rust example at a completed-write / not-yet-sync boundary, then reopen."""
from __future__ import annotations

import argparse
from pathlib import Path
import select
import signal
import subprocess
import tempfile


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/debug/examples/recovery_probe"))
    args = parser.parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit("Build first: cargo build -p broker-core --example recovery_probe")
    with tempfile.TemporaryDirectory(prefix="broker-recovery-") as temporary:
        root = Path(temporary) / "broker"  # Initialization requires this child to be absent.
        initialized = subprocess.run(
            [str(binary), "init", str(root)], check=True, capture_output=True, text=True, timeout=15
        )
        if initialized.stdout.strip() != "initialized epoch=1":
            raise RuntimeError(f"unexpected initialization: {initialized.stdout!r}")
        child = subprocess.Popen(
            [str(binary), "hold", str(root)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        try:
            assert child.stdout is not None
            readable, _, _ = select.select([child.stdout], [], [], 15)
            if not readable:
                raise RuntimeError("watchdog: child did not reach its synchronization gate")
            line = child.stdout.readline().strip()
            if line != "CUT epoch=2 base=0 before-data-sync":
                if child.poll() is None:
                    child.kill()
                _, errors = child.communicate(timeout=10)
                raise RuntimeError(f"unexpected child boundary: {line!r}; exit={child.returncode}; {errors}")
            child.kill()
            _, errors = child.communicate(timeout=10)
            if child.returncode != -signal.SIGKILL:
                raise RuntimeError(f"child was not killed as expected: {child.returncode}; {errors}")
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=10)
        recovered = subprocess.run(
            [str(binary), "inspect", str(root)], check=True, capture_output=True, text=True, timeout=15
        )
        expected = ["epoch=3", "partition=0 end=1 first=636174"]
        expected.extend(f"partition={n} end=0 first=-" for n in range(1, 4))
        if recovered.stdout.splitlines() != expected:
            raise RuntimeError(f"unexpected recovered state: {recovered.stdout!r}")
        print("process-crash check: unconfirmed cat retained; epoch 2 -> 3")
        print("This is not a power-loss test.")


if __name__ == "__main__":
    main()
