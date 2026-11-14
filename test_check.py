"""工具包行为回归：全部使用临时合成材料，不读取任何客户数据。"""
import csv
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("check.py")


class CheckBehaviorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.batch = self.root / "artifacts/b01"
        self.batch.mkdir(parents=True)

    def put(self, relative, value):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value)
        return path

    def invoke(self, *args):
        result = subprocess.run([sys.executable, str(SCRIPT), *map(str, args)],
                                cwd=self.root, capture_output=True, text=True)
        self.assertNotIn("Traceback", result.stderr + result.stdout)
        return result.returncode, result.stdout

    def archive(self, files=None):
        raw = self.put("data/raw/input.txt", "synthetic input")
        manifest = {"input.txt": hashlib.sha256(raw.read_bytes()).hexdigest()}
        self.put("artifacts/b01/run.json", {
            "input_files": manifest if files is None else files,
            "rules_version": "rules-v1", "code_version": "code-v1",
            "started_at": "2026-11-14T10:00:00+08:00", "command": "synthetic-run"})

    def ledger(self, name="客户-ETL工作手册.md", completed=False):
        rows = ["| 步 | 名称 | 状态 | 判据 | 产出物 | 下一步 |",
                "| --- | --- | --- | --- | --- | --- |"]
        rows += [f"| {i} | 步骤{i} | {'✅' if completed else '⬜'} | 判据 | "
                 f"{'`artifacts/b01/result.json`' if completed else ''} | C{i+1}-A |"
                 for i in range(10)]
        return self.put(name, "\n".join(rows))

    def profile(self, missing_type=False, breakdown=True):
        info = {"type": "text", "missing": {"total": 0, "by_literal": {}}, "range": {"min": "1", "max": "1"}}
        if missing_type:
            del info["type"]
        if not breakdown:
            del info["missing"]["by_literal"]
        return info

    def metrics(self, unit="元", display=None):
        item = {"id": "sales", "value": 100, "order_count": 1, "unit": unit}
        if display is not None:
            item.update(display=display, display_unit="元")
        self.put("artifacts/b01/metrics/metrics_run.json", {"metrics": [item]})

    def unit_config(self, display=False):
        spec = {"metric": "sales", "value": "value", "unit": "unit", "expected_unit": "元"}
        if display:
            spec.update(expected_unit="分", display="display", display_unit="display_unit",
                        expected_display_unit="元", scale=100)
        self.put("check_config.json", {"unit_checks": [spec]})

    def field_config(self, raw_note="", clean_note="", required=False):
        # 故意重排列，验证按配置映射而不是按列位置。
        for path, columns, row in [
            ("data/raw/source.csv", ["id", "note"], ["1", raw_note]),
            ("artifacts/b01/cleaning/target.csv", ["comment", "key"], [clean_note, "1"])]:
            p = self.root / path
            p.parent.mkdir(parents=True, exist_ok=True)
            with p.open("w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(columns)
                writer.writerow(row)
        self.put("check_config.json", {"field_checks": [{
            "raw": "source.csv", "clean": "cleaning/target.csv",
            "columns": [{"source": "id", "target": "key", "required": True},
                        {"source": "note", "target": "comment", "required": required, "max_drop": 0.3}]}]})

    def test_valid_archive_passes_stage_zero(self):
        self.archive()
        self.assertEqual(self.invoke(self.batch, "--stage", 0)[0], 0)

    def test_empty_input_is_not_green(self):
        self.archive({})
        self.assertEqual(self.invoke(self.batch, "--stage", 0)[0], 1)

    def test_invalid_hash_is_not_skipped(self):
        self.archive({"input.txt": "TODO"})
        self.assertEqual(self.invoke(self.batch, "--stage", 0)[0], 1)

    def test_missing_manifest_does_not_pass(self):
        self.assertEqual(self.invoke(self.batch, "--stage", 0)[0], 2)

    def test_partial_inspection_does_not_claim_complete(self):
        self.archive()
        self.assertEqual(self.invoke(self.batch)[0], 2)

    def test_unrelated_numbers_are_not_unit_failure(self):
        self.metrics()
        code, output = self.invoke(self.batch, "--stage", 7)
        self.assertEqual(code, 2)
        self.assertNotIn("FAIL", output)

    def test_configured_correct_unit_passes(self):
        self.metrics()
        self.unit_config()
        self.assertEqual(self.invoke(self.batch, "--stage", 7)[0], 0)

    def test_configured_wrong_unit_fails(self):
        self.metrics("分")
        self.unit_config()
        self.assertEqual(self.invoke(self.batch, "--stage", 7)[0], 1)

    def test_display_conversion_passes(self):
        self.metrics("分", 1)
        self.unit_config(display=True)
        self.assertEqual(self.invoke(self.batch, "--stage", 7)[0], 0)

    def test_display_conversion_error_fails(self):
        self.metrics("分", 100)
        self.unit_config(display=True)
        self.assertEqual(self.invoke(self.batch, "--stage", 7)[0], 1)

    def test_optional_null_column_not_false_failure(self):
        self.field_config()
        code, output = self.invoke(self.batch, "--stage", 3)
        self.assertNotIn("FAIL", output)
        self.assertIn("2 个映射字段符合", output)
        self.assertIn(code, (0, 2))  # YAML 尚未提供，不据字段单项宣布本步完成。

    def test_reordered_fields_follow_mapping(self):
        self.field_config("memo", "memo")
        self.assertNotIn("FAIL", self.invoke(self.batch, "--stage", 3)[1])

    def test_real_value_loss_fails(self):
        self.field_config("memo", "")
        self.assertEqual(self.invoke(self.batch, "--stage", 3)[0], 1)

    def test_required_null_fails(self):
        self.field_config(required=True)
        self.assertEqual(self.invoke(self.batch, "--stage", 3)[0], 1)

    def test_misspelled_field_constraint_is_incomplete(self):
        self.field_config()
        path = self.root / "check_config.json"
        config = json.loads(path.read_text())
        config["field_checks"][0]["columns"][0]["requird"] = True
        path.write_text(json.dumps(config))
        self.assertEqual(self.invoke(self.batch, "--stage", 3)[0], 2)

    def test_null_values_must_not_be_a_string(self):
        self.field_config()
        path = self.root / "check_config.json"
        config = json.loads(path.read_text())
        config["field_checks"][0]["null_values"] = "NULL"
        path.write_text(json.dumps(config))
        self.assertEqual(self.invoke(self.batch, "--stage", 3)[0], 2)

    def test_duplicate_raw_basename_requires_explicit_path(self):
        self.archive()
        (self.root / "data/raw/input.txt").unlink()
        self.put("data/raw/a/input.txt", "synthetic input")
        self.put("data/raw/b/input.txt", "synthetic input")
        self.assertEqual(self.invoke(self.batch, "--stage", 0)[0], 2)

    def test_missing_raw_file_is_incomplete(self):
        self.archive()
        (self.root / "data/raw/input.txt").unlink()
        self.assertEqual(self.invoke(self.batch, "--stage", 0)[0], 2)

    def test_missing_completed_outputs_do_not_pass(self):
        self.ledger(completed=True)
        self.assertEqual(self.invoke("--status")[0], 2)

    def test_existing_completed_outputs_remain_ledger_claim(self):
        self.ledger(completed=True)
        self.put("artifacts/b01/result.json", {})
        code, output = self.invoke("--status")
        self.assertEqual(code, 0)
        self.assertIn("仍须核对判据和回溯件", output)

    def test_ambiguous_ledgers_require_selection(self):
        first = self.ledger()
        self.ledger("另一个-进度台账.md")
        self.assertEqual(self.invoke("--status")[0], 2)
        self.assertEqual(self.invoke("--status", "--ledger", first)[0], 0)

    def test_examples_in_code_fences_are_ignored(self):
        self.put("通用工作手册.md", "```\n" + self.ledger().read_text() + "\n```\n")
        self.assertEqual(self.invoke("--status")[0], 0)

    def test_explicit_project_root_and_ledger(self):
        ledger = self.ledger()
        self.assertEqual(self.invoke("--status", "--project-root", self.root, "--ledger", ledger.name)[0], 0)

    def test_documented_text_pointer_works(self):
        self.archive()
        self.put("artifacts/current", "b01\n")
        self.assertEqual(self.invoke("--stage", 0)[0], 0)

    def test_symlink_pointer_works(self):
        self.archive()
        (self.root / "artifacts/current").symlink_to("b01", target_is_directory=True)
        self.assertEqual(self.invoke("--stage", 0)[0], 0)

    def test_pointer_cycle_is_reported(self):
        self.put("artifacts/current", "other")
        self.put("artifacts/other", "current")
        self.assertEqual(self.invoke("--stage", 0)[0], 2)

    def test_nested_profile_fields_not_container(self):
        self.put("artifacts/b01/profile/profile_report.json", {"orders.csv": {"fields": {"id": self.profile()}}})
        self.assertEqual(self.invoke(self.batch, "--stage", 1)[0], 0)

    def test_profile_missing_type_fails(self):
        self.put("artifacts/b01/profile/profile_report.json", {"orders.csv": {"fields": {"id": self.profile(missing_type=True)}}})
        self.assertEqual(self.invoke(self.batch, "--stage", 1)[0], 1)

    def test_column_named_fields_not_mistaken_for_container(self):
        self.put("artifacts/b01/profile/profile_report.json", {"orders.csv": {"fields": self.profile()}})
        self.assertEqual(self.invoke(self.batch, "--stage", 1)[0], 0)

    def test_manifest_string_fields_cannot_be_empty_objects(self):
        self.archive()
        path = self.batch / "run.json"
        data = json.loads(path.read_text())
        data["code_version"] = {}
        path.write_text(json.dumps(data))
        self.assertEqual(self.invoke(self.batch, "--stage", 0)[0], 1)

    def test_profile_array_with_inferred_type_and_range(self):
        info = self.profile()
        info["inferred_type"] = info.pop("type")
        self.put("artifacts/b01/profile/profile_report.json", {"fields": [{"source_file": "orders.csv", "field": "id", **info}]})
        self.assertEqual(self.invoke(self.batch, "--stage", 1)[0], 0)

    def test_same_field_in_two_files_is_checked_twice(self):
        self.put("artifacts/b01/profile/profile_report.json", {
            "a.csv": {"fields": [{"field": "id", **self.profile()}]},
            "b.csv": {"fields": [{"field": "id", **self.profile(missing_type=True)}]}})
        self.assertEqual(self.invoke(self.batch, "--stage", 1)[0], 1)

    def test_partial_missing_breakdown_not_green(self):
        self.put("artifacts/b01/profile/profile_report.json", {"orders.csv": {"id": self.profile(), "note": self.profile(breakdown=False)}})
        self.assertEqual(self.invoke(self.batch, "--stage", 1)[0], 2)

    def test_negative_counts_fail_even_when_equation_balances(self):
        self.put("artifacts/b01/reconcile/counts.json", {"read": 1, "accepted": 2, "quarantined": -1, "deduplicated": 0})
        self.put("artifacts/b01/reconcile/second_run_accepted.txt", "2")
        self.assertEqual(self.invoke(self.batch, "--stage", 4)[0], 1)

    def test_missing_second_run_not_green(self):
        self.put("artifacts/b01/reconcile/counts.json", {"read": 1, "accepted": 1, "quarantined": 0, "deduplicated": 0})
        self.assertEqual(self.invoke(self.batch, "--stage", 4)[0], 2)

    def test_unsupported_stage_not_green(self):
        self.assertEqual(self.invoke(self.batch, "--stage", 9)[0], 2)

    def test_invalid_json_report_is_incomplete(self):
        self.put("artifacts/b01/profile/profile_report.json", "{broken")
        self.assertEqual(self.invoke(self.batch, "--stage", 1)[0], 2)

    def test_unknown_config_key_is_not_ignored(self):
        self.put("check_config.json", {"unit_check": []})
        self.assertEqual(self.invoke(self.batch)[0], 2)


if __name__ == "__main__":
    unittest.main()
