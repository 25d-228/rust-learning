#!/usr/bin/env python3
"""Run a fixed native measurement matrix; never build, publish, or overwrite a run."""
from __future__ import annotations
import argparse
import csv
import json
import math
import os
from pathlib import Path
import platform
import shutil
import stat
import statistics
import subprocess
import time

PROFILES = ("single", "batched", "dense")


def text_command(args: list[str]) -> str | None:
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=3, check=False)
        return result.stdout.strip() if result.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def rss_kib(pid: int) -> int | None:
    value = text_command(["ps", "-o", "rss=", "-p", str(pid)])
    return int(value) if value and value.strip().isdigit() else None


def disk_sample(root: Path) -> tuple[int, int | None, int]:
    logical, allocated, raced = 0, 0, 0
    supported = True
    # The measurement creates this tree itself. Do not follow symlinks out of it.
    for directory, _, files in os.walk(root, followlinks=False):
        for name in files:
            try:
                info = (Path(directory) / name).lstat()
            except FileNotFoundError:
                raced += 1
                continue
            if not stat.S_ISREG(info.st_mode):
                continue
            logical += info.st_size
            blocks = getattr(info, "st_blocks", None)
            if blocks is None:
                supported = False
            else:
                allocated += blocks * 512
    return logical, allocated if supported else None, raced


def parse_summary(path: Path) -> dict[str, str]:
    rows: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split("\t")
        if len(parts) != 2 or parts[0] in rows:
            raise ValueError(f"invalid or duplicate summary row in {path}")
        rows[parts[0]] = parts[1]
    if rows.get("valid") != "true" or rows.get("scope") != "in_process_bounded_engine_two_phase":
        raise ValueError(f"not a complete engine sample: {path}")
    for name in ("append_seconds", "consumer_seconds", "recovery_seconds"):
        value = float(rows[name])
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"invalid {name} in {path}")
    offered, started, confirmed, refused, ambiguous, missed = (
        int(rows[key]) for key in ("offered", "started", "confirmed", "refused", "ambiguous", "missed"))
    if min(offered, started, confirmed, refused, ambiguous, missed) < 0:
        raise ValueError("negative request counter")
    if started + missed != offered or confirmed + refused + ambiguous != started or ambiguous:
        raise ValueError("unaccounted or ambiguous requests; do not compare")
    if int(rows["useful_group_records"]) != 2 * confirmed:
        raise ValueError("both groups did not cover all confirmed work")
    return rows


def measured_child(command: list[str], run: Path, timeout: float, interval: float,
                   sample: bool) -> dict[str, int | None]:
    # The Rust program owns creation of run/. Its parent directory belongs to this driver.
    if run.exists():
        raise FileExistsError(run)
    log_path = run.parent / (run.name + ".log")
    samples_path = run.parent / (run.name + "-resources.tsv")
    peaks: dict[str, int | None] = {"rss_kib_sampled": None, "logical_bytes_sampled": None,
                                  "allocated_bytes_sampled": None}
    start = time.monotonic()
    with log_path.open("x", encoding="utf-8") as log, samples_path.open("x", newline="", encoding="utf-8") as output:
        writer = csv.writer(output, delimiter="\t")
        writer.writerow(["seconds", "phase", "rss_kib", "logical_bytes", "allocated_bytes", "raced_files"])
        child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
        try:
            while child.poll() is None:
                if time.monotonic() - start >= timeout:
                    child.kill()
                    child.wait()  # A kill request does not replace reaping; kernel I/O can still delay exit.
                    raise TimeoutError(f"measurement timed out; inspect {log_path}")
                if sample:
                    try:
                        phase = (run / "phase.txt").read_text(encoding="utf-8").strip()
                    except FileNotFoundError:
                        phase = "starting"
                    rss = rss_kib(child.pid)
                    logical, allocated, raced = disk_sample(run / "measured" / "broker")
                    writer.writerow([f"{time.monotonic() - start:.6f}", phase,
                                     "NA" if rss is None else rss, logical,
                                     "NA" if allocated is None else allocated, raced])
                    for key, value in zip(peaks, (rss, logical, allocated)):
                        if value is not None:
                            old = peaks[key]
                            peaks[key] = value if old is None else max(old, value)
                # This controls observation overhead; it is not evidence of broker event ordering.
                time.sleep(interval)
            code = child.wait()
            if code != 0:
                raise RuntimeError(f"measurement exited {code}; inspect {log_path}")
        finally:
            if child.poll() is None:
                child.kill()
            child.wait()
    return peaks


def summarize(rows: list[dict[str, str | int | None]]) -> dict:
    report: dict = {"scope": "engine-only; no TCP", "profiles": {},
                    "decision": "not selected automatically; evaluate paired runs and correctness"}
    fields = ("confirmed_per_second", "useful_group_records_per_second", "recovery_seconds")
    for profile in PROFILES:
        values = [row for row in rows if row["profile"] == profile]
        if not values:
            continue
        aggregate = {}
        for field in fields:
            samples = [float(row[field]) for row in values]
            if not all(math.isfinite(value) for value in samples):
                raise ValueError("nonfinite throughput or recovery value")
            aggregate[field] = {"median": statistics.median(samples), "min": min(samples), "max": max(samples)}
        aggregate["confirmed_counts"] = [int(row["confirmed"]) for row in values]
        aggregate["missed_counts"] = [int(row["missed"]) for row in values]
        aggregate["refused_counts"] = [int(row["refused"]) for row in values]
        aggregate["scan_amplification"] = [float(row["scanned_records"]) / int(row["fetched_records"])
            if int(row["fetched_records"]) else None for row in values]
        report["profiles"][profile] = aggregate
    if "dense" in report["profiles"] and "batched" in report["profiles"]:
        dense = report["profiles"]["dense"]["useful_group_records_per_second"]["median"]
        baseline = report["profiles"]["batched"]["useful_group_records_per_second"]["median"]
        report["dense_consumer_ratio_of_medians"] = dense / baseline if baseline else None
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path, help="new output directory, normally under runs/")
    parser.add_argument("--executable", type=Path, default=Path("target/release/broker-measure"))
    parser.add_argument("--offers", type=int, default=1024)
    parser.add_argument("--payload", type=int, default=1024)
    parser.add_argument("--mode", choices=("closed", "paced"), default="closed")
    parser.add_argument("--period-us", type=int, default=2000, help="paced mode only; closed records zero")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--interval", type=float, default=0.2)
    parser.add_argument("--no-sampling", action="store_true")
    args = parser.parse_args()
    if not (1 <= args.offers <= 4096 and 24 <= args.payload <= 4096 and 1 <= args.repeats <= 5
            and 1000 <= args.period_us <= 10_000 and 1 <= args.timeout <= 900 and 0.05 <= args.interval <= 2):
        parser.error("offers 1..4096, payload 24..4096, repeats 1..5, period 1000..10000us, timeout 1..900s, interval .05..2s")
    executable = args.executable.resolve(strict=True)
    args.root.parent.mkdir(parents=True, exist_ok=True)
    args.root.mkdir()  # Never overwrite, clear, or resume an existing run implicitly.
    manifest = {"platform": platform.platform(), "machine": platform.machine(),
                "sw_vers": text_command(["sw_vers"]), "rustc_on_path": text_command(["rustc", "--version", "--verbose"]),
                "cargo_on_path": text_command(["cargo", "--version"]), "python": platform.python_version(),
                "executable": str(executable), "arguments": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                "rustflags": os.environ.get("RUSTFLAGS", ""), "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "build_note": "Build this executable with this toolchain before running; version-on-PATH is not embedded build provenance."}
    (args.root / "environment.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    if Path("Cargo.lock").is_file():
        shutil.copyfile("Cargo.lock", args.root / "Cargo.lock")
    rows = []
    for repeat in range(args.repeats):
        order = PROFILES if repeat % 2 == 0 else tuple(reversed(PROFILES))
        for profile in order:
            run = args.root / f"{repeat + 1:02d}-{profile}"
            command = [str(executable), str(run), profile, args.mode, str(args.offers), str(args.payload),
                       str(args.period_us if args.mode == "paced" else 0)]
            peaks = measured_child(command, run, args.timeout, args.interval, not args.no_sampling)
            row = parse_summary(run / "measured" / "summary.tsv")
            row.update(peaks); row["repeat"] = repeat + 1; rows.append(row)
            # This summary is rewritten only inside the newly created, owned output directory.
            with (args.root / "results.tsv").open("w", newline="", encoding="utf-8") as output:
                writer = csv.DictWriter(output, fieldnames=list(rows[0]), delimiter="\t")
                writer.writeheader(); writer.writerows(rows)
            print(f"completed {run}: confirmed={row['confirmed']} refused={row['refused']} missed={row['missed']}", flush=True)
    (args.root / "comparison.json").write_text(json.dumps(summarize(rows), indent=2, allow_nan=False) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
