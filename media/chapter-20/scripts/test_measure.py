"""Checks for the measurement driver only; they do not execute the Rust broker."""
from pathlib import Path
import sys
import tempfile
import unittest
import measure


class MeasurementDriverTests(unittest.TestCase):
    def test_disk_lengths_ignore_symlinks(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name); (root / "a").write_bytes(b"abc")
            (root / "link").symlink_to(root / "a")
            logical, allocated, raced = measure.disk_sample(root)
            self.assertEqual((logical, raced), (3, 0))
            self.assertTrue(allocated is None or allocated >= 0)

    def test_missing_tree_is_empty_not_a_fake_allocation(self):
        with tempfile.TemporaryDirectory() as name:
            self.assertEqual(measure.disk_sample(Path(name) / "absent"), (0, 0, 0))

    def fixture(self):
        return {"valid": "true", "scope": "in_process_bounded_engine_two_phase", "offered": "8", "started": "6",
                "confirmed": "5", "refused": "1", "ambiguous": "0", "missed": "2", "useful_group_records": "10",
                "append_seconds": "1", "consumer_seconds": "2", "recovery_seconds": "0.1"}

    def parse(self, values):
        with tempfile.TemporaryDirectory() as name:
            path = Path(name) / "summary.tsv"
            path.write_text("".join(f"{key}\t{value}\n" for key, value in values.items()))
            return measure.parse_summary(path)

    def test_summary_keeps_refusals_and_missed_offers(self):
        values = self.parse(self.fixture())
        self.assertEqual((values["refused"], values["missed"]), ("1", "2"))

    def test_ambiguous_or_missing_outcomes_refuse_comparison(self):
        for updates in ({"ambiguous": "1"}, {"missed": "0"}, {"useful_group_records": "9"}):
            values = self.fixture(); values.update(updates)
            with self.assertRaises(ValueError):
                self.parse(values)

    def test_nonfinite_or_zero_time_is_not_a_throughput_measurement(self):
        for value in ("nan", "inf", "0", "-1"):
            values = self.fixture(); values["append_seconds"] = value
            with self.assertRaises(ValueError):
                self.parse(values)

    def test_duplicate_summary_key_is_rejected(self):
        with tempfile.TemporaryDirectory() as name:
            path = Path(name) / "summary.tsv"; path.write_text("valid\ttrue\nvalid\ttrue\n")
            with self.assertRaises(ValueError):
                measure.parse_summary(path)

    def test_successful_child_is_reaped_and_unsampled_values_are_unknown(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name) / "child"
            peaks = measure.measured_child([sys.executable, "-c", "print('fixture only')"], root, 3, .01, False)
            self.assertTrue(all(value is None for value in peaks.values()))
            self.assertIn("fixture only", (root.parent / "child.log").read_text())

    def test_failed_child_and_existing_run_are_not_success(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name) / "child"
            with self.assertRaises(RuntimeError):
                measure.measured_child([sys.executable, "-c", "raise SystemExit(7)"], root, 3, .01, False)
            root.mkdir(); (root / "keep").write_text("unrelated")
            with self.assertRaises(FileExistsError):
                measure.measured_child([sys.executable, "-c", "pass"], root, 3, .01, False)
            self.assertEqual((root / "keep").read_text(), "unrelated")

    def test_timeout_kills_and_reaps_a_fixture_child(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name) / "slow"
            with self.assertRaises(TimeoutError):
                measure.measured_child([sys.executable, "-c", "import time; time.sleep(30)"], root, .08, .01, False)


if __name__ == "__main__":
    unittest.main()
