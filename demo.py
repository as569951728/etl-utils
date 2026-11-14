#!/usr/bin/env python3
"""首次体验：在新目录重放合成 ETL 和 PASS/FAIL/MISSING；不调用模型或客户系统。"""
import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import subprocess
import sys

BASE = Path(__file__).resolve().parent


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_csv(path, columns, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)


def run_demo(output):
    output = Path(output).resolve()
    # 必须用全新目录；不清理或覆盖任何旧运行。
    output.mkdir(parents=True, exist_ok=False)
    source = json.loads((BASE / "examples/first-run/source.json").read_text(encoding="utf-8"))
    raw = output / "data/raw/receipts.csv"
    write_csv(raw, source["columns"], source["rows"])
    raw.chmod(0o444)
    batch = output / "artifacts/b01"
    write_json(batch / "run.json", {
        "input_files": {"receipts.csv": hashlib.sha256(raw.read_bytes()).hexdigest()},
        "rules_version": "synthetic-rules-v1",
        "code_version": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "started_at": datetime.now(timezone.utc).isoformat(),
        "command": f"python demo.py --output {output}",
    })
    rules = {
        "basis": "示例设计者明示的处理契约，非客户确认或学员成果",
        "deduplicate": "同文件全部字段完全一致时只留一条",
        "amount": "collected_yuan 是元，用 Decimal ×100 存为整数分",
        "memo": "可空，空值不填造内容",
        "coverage": "仅本次合成柜台收款台账，不含其他渠道；不能据此推算全渠道销售额",
    }
    write_json(batch / "cleaning/rules.json", rules)
    fields = []
    for index, column in enumerate(source["columns"]):
        values = [row[index] for row in source["rows"]]
        fields.append({"source_file": "receipts.csv", "field": column, "inferred_type": "text",
                       "missing": {"total": values.count(""), "by_literal": {"": values.count("")}},
                       "distribution": dict(Counter(values))})
    write_json(batch / "profile/profile_report.json", {"fields": fields})
    seen, cleaned, dispositions = set(), [], []
    for number, row in enumerate(source["rows"], 1):
        key = tuple(row)
        disposition = "deduplicated" if key in seen else "accepted"
        dispositions.append({"source_file": "receipts.csv", "source_row": number, "disposition": disposition})
        if disposition == "accepted":
            seen.add(key)
            cleaned.append([row[0], row[1], int(Decimal(row[2]) * 100), row[3]])
    write_csv(batch / "cleaning/receipts.csv", ["id", "counter", "amount_cents", "memo"], cleaned)
    write_json(batch / "reconcile/dispositions.json", dispositions)
    counts = {"read": 4, "accepted": len(cleaned), "quarantined": 0, "deduplicated": 4 - len(cleaned)}
    write_json(batch / "reconcile/counts.json", counts)
    # 第二次独立读取同一原件，再规范化输出，以身份和字段值比较，不只比总数。
    with raw.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))[1:]
    unique_again = list(dict.fromkeys(tuple(row) for row in rows))
    repeated = [[row[0], row[1], int(Decimal(row[2]) * 100), row[3]] for row in unique_again]
    if repeated != cleaned:
        raise ValueError("重复运行的记录身份或字段值发生变化")
    (batch / "reconcile/second_run_accepted.txt").write_text(str(len(repeated)), encoding="utf-8")
    # 独立的示例预期固定在输入契约中，不从实现输出反推。
    expected = {"T001": 1000, "T002": 2000, "T003": 5000}
    actual = {row[0]: row[2] for row in cleaned}
    if actual != expected or len(cleaned) != 3 or len(dispositions) != 4:
        raise ValueError(f"独立预期与实际不符: {actual}")
    if {r["source_row"] for r in dispositions} != {1, 2, 3, 4}:
        raise ValueError("有原始记录缺少去向")
    write_json(batch / "metrics/metrics_run.json", {"metrics": [{
        "id": "receipt_amount", "value": sum(row[2] for row in cleaned), "unit": "分",
        "display": sum(row[2] for row in cleaned) / 100, "display_unit": "元"}]})
    write_json(output / "check_config.json", {"unit_checks": [{
        "metric": "receipt_amount", "value": "value", "unit": "unit", "expected_unit": "分",
        "display": "display", "display_unit": "display_unit", "expected_display_unit": "元", "scale": 100}]})
    (batch / "reports").mkdir()
    (batch / "reports/coverage_notes.md").write_text(
        "# 覆盖说明\n\n数据范围：仅本次四行合成输入。缺口：其他来源未提供；没有日历覆盖完整性的证据。\n"
        "去重后收款 80.00 元；不代表全渠道销售额。已处理的重复行不增加金额。\n", encoding="utf-8")
    # 三个互不覆盖的检查场景；故障是演示者人为注入，不是真实事故。
    broken = output / "artifacts/wrong-unit"
    write_json(broken / "metrics/metrics_run.json", {"metrics": [{
        "id": "receipt_amount", "value": 8000, "unit": "元", "display": 80, "display_unit": "元"}]})
    missing = output / "artifacts/missing-result"
    missing.mkdir(parents=True)
    results = []
    scenarios = [("建档", batch, 0, 0), ("剖析", batch, 1, 0), ("对账数量初筛", batch, 4, 0),
                 ("正常单位", batch, 7, 0), ("人为注入单位错误", broken, 7, 1), ("缺少结果文件", missing, 7, 2)]
    for label, path, stage, expected_exit in scenarios:
        command = [sys.executable, str(BASE / "check.py"), str(path), "--project-root", str(output), "--stage", str(stage)]
        run = subprocess.run(command, capture_output=True, text=True)
        results.append({"scenario": label, "command": command, "expected_exit": expected_exit,
                        "actual_exit": run.returncode, "judge": run.returncode == expected_exit,
                        "stdout": run.stdout, "stderr": run.stderr})
        print(f"{label}：期望 {expected_exit}，实际 {run.returncode}，{'符合预期' if run.returncode == expected_exit else '不符合预期'}")
    write_json(output / "demo-verification.json", {
        "scope": "原创合成文件 ETL 演示；没有调用 Agent，没有客户反馈，不证明十步全部完成",
        "input_rows": 4, "accepted_ids": sorted(expected), "expected_amount_cents": 8000,
        "actual_amount_cents": sum(row[2] for row in cleaned),
        "repeat_identity_and_values_equal": repeated == cleaned, "checks": results,
    })
    if not all(result["judge"] for result in results):
        raise ValueError("演示检查与预期不符，查看 demo-verification.json")
    print(f"演示完成：{output}；原始材料、规则、处理结果、去向和检查原文已保存。")
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, help="必须尚不存在的新目录")
    args = parser.parse_args()
    try:
        run_demo(args.output)
    except (OSError, ValueError) as exc:
        print(f"演示未完成：{exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
