#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check.py —— ETL 批次断言脚本（配套《W04-ETL个人工作手册》通用件二①）

谁写：AI（第一次做到某一步时，把该步 D 段"先跑断言"那几条补成下面的 @check）
谁跑：AI 每次交活前自动跑，把灯表原文贴回来；你随时可手动重跑——就这一条命令：

    python check.py <批次目录>
    例：python check.py artifacts/b1-20261111
       （不传参数则默认跑 artifacts/current 指向的批次）

看结果：只扫 FAIL 行；SKIP = 那一步的产物还没生成，属正常，不用管。
退出码：无 FAIL → 0；有任一 FAIL → 1（AI 修完才能交活）。

AI 补断言时必须遵守（独立检查三原则）：
  1. 数据只从批次目录里的产物文件读，不照处理代码反推；
  2. 对应产物文件不存在 → return None（记 SKIP），不许硬编通过；
  3. detail 里带两个数："产物说 X、我算出 Y"，不许只写"不一致"。
"""

import sys
import json
import hashlib
from pathlib import Path

# ---------- 定位批次 ----------
BATCH = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("artifacts/current")
BATCH = BATCH.resolve()
ROOT = BATCH.parents[1] if BATCH.parent.name == "artifacts" else BATCH.parent
RAW_DIR = ROOT / "data" / "raw"

# ---------- 断言注册表 ----------
CHECKS = []  # [(stage, name, fn)]

def check(stage, name):
    def deco(fn):
        CHECKS.append((stage, name, fn))
        return fn
    return deco

def sha256_of(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

def load_json(p: Path):
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None

# 断言返回：None=SKIP ｜ (True, "说明")=PASS ｜ (False, "产物说X、我算出Y")=FAIL

# ================= 第 0 步：建档 =================
RUN_JSON = BATCH / "run.json"

@check("0建档", "run.json 五键齐全、无占位")
def _():
    rj = load_json(RUN_JSON)
    if rj is None:
        return None
    need = ["input_files", "rules_version", "code_version", "started_at", "command"]
    bad = [k for k in need if k not in rj or str(rj[k]).strip() in ("", "TODO", "待确认")]
    return (not bad), ("5 键齐全" if not bad else f"缺/占位: {bad}")

@check("0建档", "raw 文件 sha256 与 run.json 记录一致")
def _():
    rj = load_json(RUN_JSON)
    if rj is None:
        return None
    files = rj.get("input_files") or {}
    # 兼容两种写法：{"文件名": "sha256"} 或 [{"file": "…", "sha256": "…"}]
    if isinstance(files, dict):
        pairs = list(files.items())
    else:
        pairs = [(it.get("file"), it.get("sha256")) for it in files if isinstance(it, dict)]

    bad, checked = [], 0
    for name, want in pairs:
        if not name or not isinstance(want, str) or len(want) < 32:
            continue
        p = RAW_DIR / name
        if not p.exists():                      # 原始区常带一层日期目录，递归找
            hits = list(RAW_DIR.rglob(name))
            p = hits[0] if hits else p
        actual = sha256_of(p) if p.exists() else "文件不存在"
        checked += 1
        if actual != want:
            bad.append(f"{name}：记录 {want[:12]}… 实际 {actual[:12]}…")
    if checked == 0:
        return None
    return (not bad), ("全部一致" if not bad else "哈希不符 → " + "；".join(bad[:3]))

# ================= 第 1 步：剖析 =================
# 约定剖析产物为 BATCH/profile/profile_report.json：
#   {"files": {"orders.csv": {"amount": {"type":..., "missing":{"total":N,"by_literal":{...}}, "distribution":...}}}}
PROFILE = BATCH / "profile" / "profile_report.json"

@check("1剖析", "每个字段都有 类型/缺失/分布 三项")
def _():
    rep = load_json(PROFILE)
    if rep is None:
        return None
    bad = [f"{fn_}.{col}" for fn_, cols in rep.get("files", {}).items()
           for col, info in cols.items()
           if not all(k in info for k in ("type", "missing", "distribution"))]
    return (not bad), ("三项齐全" if not bad else f"缺项: {bad[:5]}{'...' if len(bad) > 5 else ''}")

@check("1剖析", "空值分写法之和 = 该列总空值")
def _():
    rep = load_json(PROFILE)
    if rep is None:
        return None
    bad = []
    for fn_, cols in rep.get("files", {}).items():
        for col, info in cols.items():
            miss = info.get("missing") or {}
            total, by = miss.get("total"), miss.get("by_literal")
            if isinstance(total, int) and isinstance(by, dict) and sum(by.values()) != total:
                bad.append(f"{fn_}.{col}: 分写法{sum(by.values())} ≠ 总空值{total}")
    return (not bad), ("全部相等" if not bad else "; ".join(bad[:3]))

# ================= 第 2 步：诊断分类 =================
# TODO（AI 第一次做到第 2 步时补全，依据手册第 2 步 D 段"先跑断言"）：
#   - 每条发现都有 类别＋依据；类别都在枚举内
#   - 待访谈清单条数 = 标"待访谈"的条数

# ================= 第 3 步：清洗规则 =================
RULES = BATCH / "cleaning" / "cleaning_rules.yaml"

@check("3清洗", "cleaning_rules.yaml 可解析")
def _():
    if not RULES.exists():
        return None
    try:
        import yaml
        yaml.safe_load(RULES.read_text(encoding="utf-8"))
        return True, "解析通过"
    except ImportError:
        return None  # 无 PyYAML 时让 AI 用它自己的解析器补这条
    except Exception as e:
        return False, f"解析失败: {e}"

# --- 字段验：行数对只说明没丢记录，不说明没丢值 ---
# 真实踩过的坑：某列映射写错整列变空，四条行数判据全过、灯表全绿，
# 到第 7 步算指标才发现那列没数据。所以这里按列位置比"非空率"。

def _read_csv(p: Path):
    import csv
    with open(p, encoding="utf-8-sig", newline="") as f:
        rd = csv.DictReader(f)
        return (rd.fieldnames or []), list(rd)

@check("3清洗", "清洗后字段非空率不低于原始（行对≠值对）")
def _():
    clean_dir = BATCH / "cleaning"
    if not clean_dir.exists():
        return None
    raw_files = list(RAW_DIR.rglob("*.csv")) if RAW_DIR.exists() else []
    if not raw_files:
        return None

    problems, compared, lowest = [], 0, 1.0
    for clean in sorted(clean_dir.glob("*.csv")):
        stem = clean.stem.lower()
        cands = [p for p in raw_files if stem in p.stem.lower()]
        if len(cands) != 1:
            continue                      # 原始表找不到或不唯一 → 跳过，不硬判
        ccols, crows = _read_csv(clean)
        rcols, rrows = _read_csv(cands[0])
        n = len(crows)
        if n == 0:
            continue
        compared += 1

        # 1) 清洗后自身有没有整列全空
        for c in ccols:
            rate = sum(1 for r in crows if (r.get(c) or "").strip()) / n
            lowest = min(lowest, rate)
            if rate == 0:
                problems.append(f"{clean.name}.{c} 整列全空（{n} 行）")

        # 2) 列数相同则按位置逐列比非空率（列名不同也能比）
        if rrows and len(ccols) == len(rcols):
            m = len(rrows)
            for c, rc in zip(ccols, rcols):
                cr = sum(1 for r in crows if (r.get(c) or "").strip()) / n
                rr = sum(1 for r in rrows if (r.get(rc) or "").strip()) / m
                if rr > 0 and cr == 0:
                    problems.append(f"{c}(原「{rc}」) 原始 {rr:.0%} → 清洗后 0%")
                elif rr - cr > 0.30:
                    problems.append(f"{c}(原「{rc}」) 原始 {rr:.0%} → 清洗后 {cr:.0%}，跌 {rr - cr:.0%}")

    if compared == 0:
        return None
    if problems:
        more = f"（另有 {len(problems) - 3} 处）" if len(problems) > 3 else ""
        return False, "；".join(problems[:3]) + more + "　掉值必须在 clean_report 里写明原因"
    return True, f"{compared} 张表逐列比对：无整列丢失、无骤降（最低非空率 {lowest:.0%}，可空列属正常）"

# ================= 第 4 步：导入对账 =================
# 约定对账产物为 BATCH/reconcile/counts.json：
#   {"read":N, "accepted":N, "quarantined":N, "deduplicated":N}
# AI 重跑一次后把第二次的 accepted 写进 BATCH/reconcile/second_run_accepted.txt
COUNTS = BATCH / "reconcile" / "counts.json"
SECOND = BATCH / "reconcile" / "second_run_accepted.txt"

@check("4对账", "read = accepted + quarantined + deduplicated")
def _():
    c = load_json(COUNTS)
    if c is None:
        return None
    lhs = c.get("read")
    rhs = c.get("accepted", 0) + c.get("quarantined", 0) + c.get("deduplicated", 0)
    return lhs == rhs, f"read={lhs} vs 三类之和={rhs}"

@check("4对账", "两次运行 accepted 不变（幂等）")
def _():
    c = load_json(COUNTS)
    if c is None:
        return None
    if not SECOND.exists():
        return None  # 还没跑第二次，正常 SKIP
    again = int(SECOND.read_text().strip())
    return again == c.get("accepted"), f'第一次 {c.get("accepted")} vs 第二次 {again}'

# ================= 第 5 步：覆盖与回答 =================
# TODO（AI 第一次做到第 5 步时补全，依据手册第 5 步 D 段"先跑断言"）：
#   - 每个缺口都落到指标库的具体指标
#   - time_range 与数据实际 min/max 一致
#   - 覆盖说明里每个数字都能在对账报告里找到

# ---------- 跑灯表 ----------
def main():
    fails = 0
    print(f"批次: {BATCH}")
    print("-" * 64)
    for stage, name, fn in CHECKS:
        try:
            r = fn()
        except Exception as e:
            r = (False, f"断言自身报错: {e}")
        if r is None:
            status, detail = "SKIP", "产物未生成/条件未满足"
        else:
            ok, detail = r
            status = "PASS" if ok else "FAIL"
            if not ok:
                fails += 1
        print(f"{status:4}  {stage}  {name}  {detail}")
    print("-" * 64)
    print("结果:", "全部通过（SKIP 为未到阶段）" if fails == 0 else f"{fails} 条 FAIL，打回重做")
    sys.exit(1 if fails else 0)

if __name__ == "__main__":
    main()
