"""项目入口与首次体验回归；仅用临时合成材料。"""
import csv
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import shutil

from etl_project import init_project, load_settings

BASE = Path(__file__).parent


class OnboardingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_init_preserves_existing_rules_and_is_idempotent(self):
        agent = self.root / "AGENTS.md"
        agent.write_text("# 我的规则\n保留这句话。\n", encoding="utf-8")
        init_project(self.root, "客户手册.md", "artifacts/b01", "rules/checks.json")
        first = agent.read_bytes()
        init_project(self.root, "客户手册.md", "artifacts/b01", "rules/checks.json")
        self.assertEqual(agent.read_bytes(), first)
        self.assertTrue(first.decode().startswith("# 我的规则\n保留这句话。"))
        self.assertEqual(load_settings(self.root)["mode"], "learning")
        self.assertFalse((self.root / "客户手册.md").exists())

    def test_init_refuses_unrequested_config_change(self):
        init_project(self.root, "原手册.md", "artifacts/b01", "checks.json")
        original = (self.root / "etl-project.json").read_bytes()
        with self.assertRaises(ValueError):
            init_project(self.root, "新手册.md", "artifacts/b02", "checks.json")
        self.assertEqual((self.root / "etl-project.json").read_bytes(), original)

    def test_explicit_update_preserves_rules_outside_markers(self):
        init_project(self.root, "原手册.md", "artifacts/b01", "checks.json")
        agent = self.root / "AGENTS.md"
        agent.write_text(agent.read_text() + "\n保留尾部规则。\n", encoding="utf-8")
        init_project(self.root, "原手册.md", "artifacts/b02", "checks.json", "execution", True)
        self.assertIn("保留尾部规则。", agent.read_text())
        self.assertEqual(load_settings(self.root)["mode"], "execution")

    def test_status_uses_recorded_ledger_instead_of_guessing(self):
        init_project(self.root, "记录的手册.md", "artifacts/b01", "checks.json")
        rows = ["| 步 | 名称 | 状态 | 判据 | 产物 | 下一步 |", "| --- | --- | --- | --- | --- | --- |"]
        rows += [f"| {i} | 步骤{i} | ⬜ | 判据 | | C{i+1}-A |" for i in range(10)]
        for name in ("记录的手册.md", "干扰手册.md"):
            (self.root / name).write_text("\n".join(rows), encoding="utf-8")
        result = subprocess.run([sys.executable, str(BASE / "check.py"), "--status"],
                                cwd=self.root, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("记录的手册.md", result.stdout)
        self.assertNotIn("候选:", result.stdout)

    def test_invalid_marker_is_not_ignored(self):
        (self.root / "etl-project.json").write_text('{"mode":"execution"}')
        result = subprocess.run([sys.executable, str(BASE / "check.py"), "--status"],
                                cwd=self.root, capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertNotIn("Traceback", result.stderr)

    def test_missing_recorded_checks_do_not_fall_back_to_another_file(self):
        init_project(self.root, "手册.md", "artifacts/b01", "custom/checks.json")
        metrics = self.root / "artifacts/b01/metrics/metrics_run.json"
        metrics.parent.mkdir(parents=True)
        metrics.write_text(json.dumps({"metrics": [{"id": "m", "value": 10, "unit": "元"}]}))
        (self.root / "check_config.json").write_text(json.dumps({"unit_checks": [{
            "metric": "m", "value": "value", "unit": "unit", "expected_unit": "分"}]}))
        run = subprocess.run([sys.executable, str(BASE / "check.py"), "--stage", "7"],
                             cwd=self.root, capture_output=True, text=True)
        self.assertEqual(run.returncode, 2, run.stdout + run.stderr)
        self.assertNotIn("FAIL", run.stdout)

    def test_standalone_checker_still_works_without_project_marker(self):
        script = self.root / "check.py"
        shutil.copyfile(BASE / "check.py", script)
        result = subprocess.run([sys.executable, str(script), "--stage", "9"],
                                cwd=self.root, capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertNotIn("No module named", result.stdout + result.stderr)

    def test_demo_replays_expected_positive_negative_missing(self):
        output = self.root / "demo"
        result = subprocess.run([sys.executable, str(BASE / "demo.py"), "--output", str(output)],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        report = json.loads((output / "demo-verification.json").read_text())
        self.assertEqual(report["actual_amount_cents"], 8000)
        self.assertEqual([x["actual_exit"] for x in report["checks"]], [0, 0, 0, 0, 1, 2])
        self.assertTrue(all(x["judge"] for x in report["checks"]))
        self.assertTrue(report["repeat_identity_and_values_equal"])
        original = (output / "demo-verification.json").read_bytes()
        repeat = subprocess.run([sys.executable, str(BASE / "demo.py"), "--output", str(output)],
                                capture_output=True, text=True)
        self.assertNotEqual(repeat.returncode, 0)
        self.assertEqual((output / "demo-verification.json").read_bytes(), original)

    def test_practice_starts_without_answers_or_learner_progress(self):
        output = self.root / "practice"
        result = subprocess.run([sys.executable, str(BASE / "practice.py"), "--output", str(output)],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse((output / "artifacts").exists())
        self.assertFalse((output / "澄川-ETL工作手册.md").exists())
        self.assertFalse((output / "check_config.json").exists())
        self.assertTrue((output / "00-开始练习.md").exists())
        self.assertIn("等待本人确认", (output / "AGENTS.md").read_text())
        with (output / "materials/exports/工单导出.csv").open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)
            self.assertEqual(len(rows), 23)
            self.assertTrue(all(None not in row for row in rows))


if __name__ == "__main__":
    unittest.main()
