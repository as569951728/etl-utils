#!/usr/bin/env python3
"""记录一次项目位置；只写配置和带标记的入口，不生成手册或业务规则。"""
import argparse
import json
import os
from pathlib import Path

START = "<!-- etl-utils:start -->"
END = "<!-- etl-utils:end -->"
ALLOWED = {"schema_version", "toolkit", "mode", "ledger", "batch", "checks"}


def load_settings(root):
    path = root / "etl-project.json"
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or set(data) != ALLOWED:
        raise ValueError("etl-project.json 的键须为 schema_version/toolkit/mode/ledger/batch/checks")
    if type(data["schema_version"]) is not int or data["schema_version"] != 1:
        raise ValueError("不支持此项目配置版本")
    if data["mode"] not in ("learning", "execution"):
        raise ValueError("mode 只支持 learning 或 execution")
    for key in ("toolkit", "ledger", "batch", "checks"):
        if not isinstance(data[key], str) or not data[key].strip():
            raise ValueError(f"{key} 必须是非空路径")
    return data


def init_project(root, ledger, batch, checks, mode="learning", update=False):
    root = Path(root).resolve()
    if not root.is_dir():
        raise ValueError("项目目录须先存在；不会替你创建或覆盖项目")
    settings = {
        "schema_version": 1,
        "toolkit": os.path.relpath(Path(__file__).parent.resolve(), root),
        "mode": mode, "ledger": ledger, "batch": batch, "checks": checks,
    }
    # 在任何写操作前检查全部输入和已有入口。
    if mode not in ("learning", "execution") or any(not isinstance(x, str) or not x.strip() for x in (ledger, batch, checks)):
        raise ValueError("模式或路径无效")
    current = load_settings(root)
    if current is not None and current != settings and not update:
        raise ValueError("项目配置已存在且不同；确需调整时使用 --update，旧业务产物不会移动")
    agent = root / "AGENTS.md"
    old = agent.read_text(encoding="utf-8") if agent.exists() else ""
    if old.count(START) != old.count(END) or old.count(START) > 1:
        raise ValueError("AGENTS.md 中的 ETL 入口标记损坏，请先核对")
    block = (
        f"{START}\n"
        f"ETL 工具包在 `{settings['toolkit']}/`。先读该目录的 AGENTS.md 与 START-HERE.md。\n"
        "项目位置和模式读 `etl-project.json`；从项目根运行工具包的 check.py --status。\n"
        "台账尚未生成时先阅读客户材料，只起草四格背景并等用户确认，不生成手册。\n"
        "用户说“开始这个客户／继续／验收这一步”时，按工具包的自然语言入口执行。\n"
        "学习模式分步停；执行模式仅在用户明确授权的范围内推进，业务口径、证据缺口和验收仍需确认。\n"
        f"{END}"
    )
    if START in old:
        a, b = old.index(START), old.index(END) + len(END)
        if a >= old.index(END):
            raise ValueError("AGENTS.md 入口标记顺序错误")
        updated = old[:a] + block + old[b:]
    else:
        updated = old.rstrip() + ("\n\n" if old.strip() else "") + block + "\n"
    config = root / "etl-project.json"
    if current != settings:
        config.write_text(json.dumps(settings, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if updated != old:
        agent.write_text(updated, encoding="utf-8")
    return settings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["init"])
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--ledger", default="客户-ETL工作手册.md")
    parser.add_argument("--batch", default="artifacts/b01")
    parser.add_argument("--checks", default="check_config.json")
    parser.add_argument("--mode", choices=["learning", "execution"], default="learning")
    parser.add_argument("--update", action="store_true")
    args = parser.parse_args()
    try:
        init_project(args.project_root, args.ledger, args.batch, args.checks, args.mode, args.update)
    except (OSError, ValueError) as exc:
        print(f"未初始化：{exc}")
        return 2
    print("已记录项目位置，保留原有协作规则；未生成手册、未执行 ETL。")
    print("下一步：对 AI 说“开始这个客户”，先确认四格背景。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
