#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check.py —— ETL 批次断言脚本（配套《W04-ETL个人工作手册》通用件二①）

谁写：AI（第一次做到某一步时，把该步 D 段"先跑断言"那几条补成下面的 @check）
谁跑：AI 每次交活前自动跑，把灯表原文贴回来；你随时可手动重跑——就这一条命令：

    python check.py <批次目录>
    例：python check.py artifacts/b1-20261111
       （不传参数则默认跑 artifacts/current 指向的批次）

看结果：PASS 只代表列出的检查；FAIL 是证据冲突；MISSING 是未完成验证。
使用 --stage N 验当前步骤；无 --stage 时只做巡检，有跳过项返回 2。
退出码：检查范围完整通过 → 0；FAIL → 1；缺件/无法判定 → 2。

AI 补断言时必须遵守（独立检查三原则）：
  1. 数据只从批次目录里的产物文件读，不照处理代码反推；
  2. 产物缺失 → return None 或 ("SKIP", "原因")，入口统一记 MISSING，不许编通过；
  3. detail 里带两个数："产物说 X、我算出 Y"，不许只写"不一致"。
"""

import sys
import re
import json
import hashlib
import argparse
import subprocess
from decimal import Decimal, InvalidOperation
from pathlib import Path

# ---------- 定位批次 ----------
BATCH = Path.cwd() / "artifacts/current"
CONFIG = {}

def _find_project_root(batch: Path) -> Path:
    """向上最多 4 层找含 data/raw 的目录；找不到就按常规布局猜。"""
    for base in [batch.parent, *list(batch.parents)[1:5]]:
        if (base / "data" / "raw").exists():
            return base
    return batch.parents[1] if batch.parent.name == "artifacts" else batch.parent

ROOT = _find_project_root(BATCH)
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
    if not isinstance(rj, dict):
        return False, "run.json 必须是对象"
    placeholders = {"", "todo", "tbd", "待确认", "initial", "none", "null"}
    bad = [k for k in need[1:] if not isinstance(rj.get(k), str)
           or rj[k].strip().lower() in placeholders]
    files = rj.get("input_files")
    if not isinstance(files, (dict, list)) or not files:
        bad.append("input_files 必须是非空清单")
    return (not bad), ("5 键齐全" if not bad else f"缺/占位: {bad}")

@check("0建档", "raw 文件 sha256 与 run.json 记录一致")
def _():
    rj = load_json(RUN_JSON)
    if rj is None:
        return None
    if not isinstance(rj, dict):
        return False, "run.json 必须是对象"
    files = rj.get("input_files")
    # 兼容两种写法：{"文件名": "sha256"} 或 [{"file": "…", "sha256": "…"}]
    if isinstance(files, dict):
        pairs = list(files.items())
    elif isinstance(files, list) and all(isinstance(it, dict) for it in files):
        pairs = [(it.get("file"), it.get("sha256")) for it in files]
    else:
        return False, "input_files 缺失或格式不正确"
    if not pairs:
        return False, "input_files 为空，没有输入可以核验"

    bad, unavailable, checked = [], [], 0
    for name, want in pairs:
        if not isinstance(name, str) or not name or not isinstance(want, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", want):
            bad.append(f"无效文件名或 SHA-256: {name!r}")
            continue
        p = RAW_DIR / name
        if not p.exists():                      # 原始区常带一层日期目录，递归找
            hits = list(RAW_DIR.rglob(name))
            if len(hits) > 1:
                unavailable.append(f"{name} 匹配多个原件，请记录相对 raw 的完整路径")
                continue
            p = hits[0] if hits else p
        if not p.is_file():
            unavailable.append(f"原始文件不存在: {name}")
            continue
        actual = sha256_of(p)
        checked += 1
        if actual != want.lower():
            bad.append(f"{name}：记录 {want[:12]}… 实际 {actual[:12]}…")
    if bad:
        return False, "输入核验失败 → " + "；".join(bad[:3])
    if unavailable:
        return ("SKIP", "；".join(unavailable[:3]))
    return checked == len(pairs), f"{checked} 个输入哈希全部一致"

# ================= 第 1 步：剖析 =================
# 不绑定报告结构与键名：按"形状"识别 file→col→info（三种常见写法都认），认不出就 SKIP。
PROFILE_CANDIDATES = [
    BATCH / "profile" / "profile_report.json",
    BATCH / "reports" / "profile_report.json",
    BATCH / "profile_report.json",
]
TYPE_KEYS = ("type", "dtype", "data_type", "inferred_type", "数据类型")
MISS_KEYS = ("missing", "nulls", "null_stats", "na", "empty",
             "empty_total", "empty_by_style", "null_count", "missing_count")
DIST_KEYS = ("distribution", "dist", "distinct", "top", "top10", "top_values",
             "values", "value_counts", "range", "取值分布")

def _pick(d, *names):
    for n in names:
        if isinstance(d, dict) and n in d:
            return d[n]
    return None

def _is_info(node) -> bool:
    """一个 dict 若同时带类型键和（缺失或分布）键，就当成"某一列的信息"。"""
    if not isinstance(node, dict):
        return False
    return (any(k in node for k in TYPE_KEYS)
            and (any(k in node for k in MISS_KEYS) or any(k in node for k in DIST_KEYS)))

FILE_RE = re.compile(r"\.(csv|xlsx|xls|json|parquet|txt|tsv)$", re.I)

def _iter_profile_fields(rep):
    """递归收集"某一列的信息"。两种定位方式：
      ① 文件名作锚点（path 上一段是 *.csv）：下一层的 dict 全是列，**不管它有没有 type**；
      ② 没有锚点时退回"看起来像列信息"的判断。
    这样某列缺了 type 也会被抓出来，而不是被当成"不存在"跳过。"""
    found, seen = [], set()

    def add(f, c, info):
        k = (str(f), str(c))
        if k in seen:
            return
        seen.add(k)
        found.append((f, c, info))

    def walk(node, path):
        if isinstance(node, dict):
            # 显式 fields/columns 容器优先，避免把整个容器误当成一列。
            for container in ("fields", "columns"):
                cols = node.get(container)
                if isinstance(cols, dict) and not any(k in cols for k in TYPE_KEYS + MISS_KEYS + DIST_KEYS):
                    fname = _pick(node, "source_file", "file", "file_name", "table") or (path[-1] if path else "?")
                    for col, info in cols.items():
                        if isinstance(info, dict):
                            add(fname, col, info)
                    return
                if isinstance(cols, list):
                    walk(cols, path)
                    return
            if _pick(node, "field", "column") is not None:
                add(_pick(node, "source_file", "file", "file_name")
                    or next((x for x in reversed(path) if isinstance(x, str) and FILE_RE.search(x)), "?"),
                    _pick(node, "field", "column"), node)
                return
            # ① 文件锚点：当前层就是「列名 → 列信息」
            if path and isinstance(path[-1], str) and FILE_RE.search(path[-1]):
                cols = {k: v for k, v in node.items() if isinstance(v, dict)}
                if cols:
                    for col, info in cols.items():
                        add(path[-1], col, info)
                    return
                # 文件名只是个标签（列在 fields 之类里）→ 继续往下
            # ② 无锚点：靠"像不像列信息"判断
            if _is_info(node):
                files = [x for x in path if isinstance(x, str) and FILE_RE.search(x)]
                fname = files[-1] if files else (path[-2] if len(path) >= 2 else "?")
                add(fname, path[-1] if path else "?", node)
                return
            for k, v in node.items():
                walk(v, path + [k])
        elif isinstance(node, list):
            for v in node:
                label = None
                if isinstance(v, dict):
                    label = (_pick(v, "file", "table", "file_name")
                             or _pick(v, "name", "column", "field"))
                walk(v, path + ([label] if label else []))

    walk(rep, [])
    return found

def _missing_of(info):
    """缺失统计归一化：返回 (total, by_literal)。两种写法都认。"""
    m = _pick(info, "missing", "nulls", "null_stats", "na", "empty")
    if isinstance(m, dict):
        return (_pick(m, "total", "count", "n"),
                _pick(m, "by_literal", "by_value", "by_token", "breakdown", "styles"))
    return (_pick(info, "empty_total", "null_count", "missing_count", "n_empty"),
            _pick(info, "empty_by_style", "null_styles", "by_literal", "by_value"))

def _load_profile():
    for p in PROFILE_CANDIDATES:
        d = load_json(p)
        if d:
            return d
    return None

@check("1剖析", "每个字段都有 类型/缺失/分布 三项")
def _():
    rep = _load_profile()
    if rep is None:
        return ("SKIP", "没找到剖析报告（试过 profile/、reports/、批次根目录）")
    fields = _iter_profile_fields(rep)
    if not fields:
        return ("SKIP", '报告结构认不出（找「带 type 的 dict」，认不出就跳过，不硬判）')
    bad = []
    for f, c, info in fields:
        has = (any(k in info for k in TYPE_KEYS),
               any(k in info for k in MISS_KEYS),
               any(k in info for k in DIST_KEYS))
        if not all(has):
            lack = [n for n, ok in zip(("类型", "缺失", "分布"), has) if not ok]
            bad.append(f"{f}.{c} 缺{''.join(lack)}")
    return (not bad), (f"{len(fields)} 个字段三项齐全" if not bad
                       else f"{len(bad)}/{len(fields)} 处缺项: " + "；".join(bad[:3]))

@check("1剖析", "空值分写法之和 = 该列总空值")
def _():
    rep = _load_profile()
    if rep is None:
        return ("SKIP", "没找到剖析报告")
    checked, bad, unavailable = 0, [], []
    for f, c, info in _iter_profile_fields(rep):
        total, by = _missing_of(info)
        if type(total) is not int or not isinstance(by, dict):
            unavailable.append(f"{f}.{c}")
            continue
        checked += 1
        if total < 0 or any(type(v) is not int or v < 0 for v in by.values()):
            bad.append(f"{f}.{c}: 空值计数须为非负整数")
        elif sum(by.values()) != total:
            bad.append(f"{f}.{c}: 分写法 {sum(by.values())} ≠ 总空值 {total}")
    if bad:
        return False, "；".join(bad[:3])
    if unavailable:
        return ("SKIP", "缺逐字段空值分写法统计: " + "、".join(unavailable[:5]))
    if checked == 0:
        return ("SKIP", '没有可核验的空值分写法统计')
    return (not bad), (f"{checked} 列相等" if not bad else "；".join(bad[:3]))

# ================= 第 2 步：诊断分类 =================
# 诊断是 markdown，机器只能验结构（有没有发现、有没有依据栏）；内容由检查者看。
DIAG_CANDIDATES = [
    BATCH / "reports" / "diagnosis.md",
    BATCH / "reports" / "diagnosis" / "diagnosis.md",
    BATCH / "diagnosis.md",
]

@check("2诊断", "诊断表有发现条目、且带依据栏（只验结构）")
def _():
    p = next((x for x in DIAG_CANDIDATES if x.exists()), None)
    if p is None:
        return ("SKIP", "没找到诊断文件（试过 reports/diagnosis.md 等 3 处）")
    text = p.read_text(encoding="utf-8", errors="ignore")
    rows = re.findall(r"(?m)^\|\s*(?:F\d+|\d+)\s*\|", text) or \
           re.findall(r"(?m)^#{2,4}\s*(?:F\d+|\d+)[\.、\s]", text)
    if not rows:
        return ("SKIP", '认不出发现编号（表格首列 F01／1、或标题 ###F01 都没匹配到）')
    has_basis = any(k in text for k in ("依据", "证据", "出处", "basis", "evidence"))
    if not has_basis:
        return False, f'有 {len(rows)} 条发现，但全文找不到「依据／证据」栏'
    return True, f'{len(rows)} 条发现，含依据栏（内容由检查者看）'

# ================= 第 3 步：清洗规则 =================
RULES = BATCH / "cleaning" / "cleaning_rules.yaml"

@check("3清洗", "cleaning_rules.yaml 可解析")
def _():
    if not RULES.exists():
        return None
    try:
        import yaml
        parsed = yaml.safe_load(RULES.read_text(encoding="utf-8"))
        return isinstance(parsed, (dict, list)) and bool(parsed), "仅验 YAML 非空结构；规则内容与 schema 仍需项目验收"
    except ImportError:
        return ("SKIP", "缺可选依赖 PyYAML，未验证 YAML；安装后重跑")
    except Exception as e:
        return False, f"解析失败: {e}"

# --- 字段验：行数对只说明没丢记录，不说明没丢值 ---
# 真实踩过的坑：某列映射写错整列变空，四条行数判据全过、灯表全绿，
# 到第 7 步算指标才发现那列没数据。按项目确认的映射核对，不能按列位置猜。

def _read_csv(p: Path):
    import csv
    with open(p, encoding="utf-8-sig", newline="") as f:
        rd = csv.DictReader(f)
        return (rd.fieldnames or []), list(rd)

@check("3清洗", "按已确认字段映射核对非空率与必填约束")
def _():
    specs = CONFIG.get("field_checks")
    if not specs:
        return ("SKIP", "缺 field_checks 映射配置；不按列位置或文件名猜语义")
    if not isinstance(specs, list):
        return ("SKIP", "field_checks 应为列表，请修正配置")
    problems, checked = [], 0
    for spec in specs:
        if not isinstance(spec, dict) or not all(k in spec for k in ("raw", "clean", "columns")):
            return ("SKIP", "field_checks 每项须有 raw/clean/columns")
        raw, clean = RAW_DIR / spec["raw"], BATCH / spec["clean"]
        if not raw.is_file() or not clean.is_file():
            return ("SKIP", f"映射文件缺失: {raw} / {clean}")
        rcols, rrows = _read_csv(raw)
        ccols, crows = _read_csv(clean)
        columns = spec["columns"]
        if not isinstance(columns, list) or not columns:
            return ("SKIP", "columns 必须是非空字段映射列表")
        null_values = spec.get("null_values", [""])
        if not isinstance(null_values, list) or not all(isinstance(v, str) for v in null_values):
            return ("SKIP", "null_values 须为字符串列表")
        nulls = set(null_values)
        for col in columns:
            if not isinstance(col, dict) or not all(k in col for k in ("source", "target")):
                return ("SKIP", "字段映射须有 source/target")
            if set(col) - {"source", "target", "required", "max_drop"}:
                return ("SKIP", "字段映射含未知配置键，请核对拼写")
            source, target = col["source"], col["target"]
            if type(col.get("required", False)) is not bool:
                return ("SKIP", "required 必须是布尔值，不能用字符串代替")
            if source not in rcols or target not in ccols:
                problems.append(f"映射字段缺失: {source} → {target}")
                continue
            if not rrows or not crows:
                return ("SKIP", "空表不能计算非空率；请核对去向、隔离与空表预期")
            rr = sum((r.get(source) or "").strip() not in nulls for r in rrows) / len(rrows)
            cr = sum((r.get(target) or "").strip() not in nulls for r in crows) / len(crows)
            checked += 1
            if col.get("required", False) and cr < 1:
                problems.append(f"{target} 约定必填，实际非空率 {cr:.1%}")
            # 只有项目明确约定容许下降值，才据此作硬判；无阈值只展示统计。
            limit = col.get("max_drop")
            if limit is not None:
                if isinstance(limit, bool) or not isinstance(limit, (int, float)) or not 0 <= limit <= 1:
                    return ("SKIP", "max_drop 必须是 0～1 的数")
                if rr - cr > limit:
                    problems.append(f"{source} → {target}: 原始 {rr:.1%}，清洗后 {cr:.1%}，超过约定下降 {limit:.1%}")
            print(f"INFO  字段 {source} → {target}: 原始 {rr:.1%}，清洗后 {cr:.1%}")
    return (not problems and checked > 0), ("；".join(problems[:5]) if problems else f"{checked} 个映射字段符合所配置约束（未配置的字段未验证）")

# ================= 第 4 步：导入对账 =================
# 约定对账产物为 BATCH/reconcile/counts.json：
#   {"read":N, "accepted":N, "quarantined":N, "deduplicated":N}
# AI 重跑一次后把第二次的 accepted 写进 BATCH/reconcile/second_run_accepted.txt
COUNTS = BATCH / "reconcile" / "counts.json"
SECOND = BATCH / "reconcile" / "second_run_accepted.txt"
RECON = BATCH / "reports" / "reconciliation.json"

def _count_error(counts):
    keys = ("read", "accepted", "quarantined", "deduplicated")
    if not isinstance(counts, dict):
        return "计数必须是对象"
    bad = [k for k in keys if type(counts.get(k)) is not int or counts[k] < 0]
    return "计数缺失或不是非负整数: " + ",".join(bad) if bad else None

@check("4对账", "read = accepted + quarantined + deduplicated")
def _():
    # 形式一：单表单文件 {"read","accepted","quarantined","deduplicated"}
    c = load_json(COUNTS)
    if c is not None:
        error = _count_error(c)
        if error:
            return False, error
        lhs = c.get("read")
        rhs = c.get("accepted", 0) + c.get("quarantined", 0) + c.get("deduplicated", 0)
        return lhs == rhs, f"read={lhs} vs 三类之和={rhs}"

    # 形式二：对账报告里按表分列 {"segment_one":{表:{read,accepted*,quarantined,deduplicated}}}
    d = load_json(RECON)
    if d is None:
        return None
    seg = d.get("segment_one") or d.get("tables") or {}
    if not isinstance(seg, dict) or not seg:
        return None
    bad, checked = [], 0
    for table, v in seg.items():
        if not isinstance(v, dict):
            bad.append(f"{table}: 计数应为对象")
            continue
        v = dict(v)
        if "accepted" not in v and "accepted_derived" in v:
            v["accepted"] = v["accepted_derived"]
        error = _count_error(v)
        if error:
            bad.append(f"{table}: {error}")
            continue
        read = v.get("read")
        acc = v.get("accepted", v.get("accepted_derived"))
        if read is None or acc is None:
            continue
        rhs = acc + v.get("quarantined", 0) + v.get("deduplicated", 0)
        checked += 1
        if read != rhs:
            bad.append(f"{table}: read={read} ≠ {acc}+{v.get('quarantined', 0)}+{v.get('deduplicated', 0)}={rhs}")
    if bad:
        return False, "；".join(bad[:3])
    if checked == 0:
        return None
    return (not bad), (f"{checked} 张表等式全部成立" if not bad else "；".join(bad[:3]))

@check("4对账", "两次接受数一致（仅数量检查，不证明完整幂等）")
def _():
    c = load_json(COUNTS)
    if c is None:
        return None
    if not SECOND.exists():
        return None  # 还没跑第二次，正常 SKIP
    again = int(SECOND.read_text().strip())
    return again == c.get("accepted"), f'第一次 {c.get("accepted")} vs 第二次 {again}'

# ================= 第 5 步：覆盖与回答 =================
# 覆盖说明是 markdown，只验结构要素（时间范围／缺口）；内容由检查者看。
COVER_CANDIDATES = [
    BATCH / "reports" / "coverage_notes.md",
    BATCH / "reports" / "coverage.md",
]

@check("5覆盖", "覆盖说明含时间范围与缺口段（只验结构）")
def _():
    p = next((x for x in COVER_CANDIDATES if x.exists()), None)
    if p is None:
        return ("SKIP", "没找到覆盖说明（reports/coverage_notes.md）")
    text = p.read_text(encoding="utf-8", errors="ignore")
    has_range = any(k in text for k in ("time_range", "时间范围", "覆盖期", "数据范围"))
    has_gap = any(k in text for k in ("缺口", "未提供", "缺失", "未导出", "gap"))
    lack = [n for n, ok in (("时间范围", has_range), ("缺口段", has_gap)) if not ok]
    if lack:
        return False, f'覆盖说明缺「{"／".join(lack)}」——没有缺口的覆盖说明等于没说边界'
    return True, "含时间范围与缺口段（内容由检查者看）"

# ================= 第 7 步：指标 =================
# 不绑定具体文件名/字段名：三个常见落位都试，字段按"数值字段"识别
METRICS_CANDIDATES = [
    BATCH / "reports" / "metrics_run.json",
    BATCH / "metrics" / "metrics_run.json",
    BATCH / "metrics_run.json",
]
@check("7指标", "按已确认单位契约核验数值与展示换算")
def _():
    specs = CONFIG.get("unit_checks")
    if not isinstance(specs, list) or not specs:
        return ("SKIP", "缺 unit_checks 单位契约；不同数值之间的比值不证明单位错位")
    d = next((load_json(p) for p in METRICS_CANDIDATES if p.is_file()), None)
    if d is None:
        return ("SKIP", "缺指标运行结果")
    items = d.get("metrics", d) if isinstance(d, dict) else d
    if isinstance(items, dict):
        items = [{**v, "id": k} for k, v in items.items() if isinstance(v, dict)]
    if not isinstance(items, list):
        return ("SKIP", "无法识别指标结果结构")
    bad = []
    for spec in specs:
        if not isinstance(spec, dict) or not all(k in spec for k in ("metric", "value", "unit", "expected_unit")):
            return ("SKIP", "单位契约须有 metric/value/unit/expected_unit")
        if not all(isinstance(spec[k], str) and spec[k].strip() for k in ("metric", "value", "unit", "expected_unit")):
            return ("SKIP", "单位契约的字段名与预期单位必须是非空字符串")
        matches = [m for m in items if isinstance(m, dict) and m.get("id") == spec["metric"]]
        if len(matches) != 1:
            bad.append(f"{spec['metric']} 预期 1 项，实际 {len(matches)} 项")
            continue
        m = matches[0]
        if m.get(spec["unit"]) != spec["expected_unit"]:
            bad.append(f"{spec['metric']} 预期单位 {spec['expected_unit']}，实际 {m.get(spec['unit'])}")
        try:
            value = Decimal(str(m[spec["value"]]))
            if not value.is_finite():
                raise ValueError("非有限值")
            if "display" in spec:
                if not all(k in spec for k in ("display_unit", "expected_display_unit", "scale")):
                    return ("SKIP", "展示换算须声明 display_unit/expected_display_unit/scale")
                scale = Decimal(str(spec["scale"]))
                if not scale.is_finite() or scale <= 0:
                    return ("SKIP", "scale 必须是有限正数")
                actual = Decimal(str(m[spec["display"]]))
                if not actual.is_finite() or actual != value / scale:
                    bad.append(f"{spec['metric']} 展示值 {actual}，契约预期 {value / scale}")
                if m.get(spec["display_unit"]) != spec["expected_display_unit"]:
                    bad.append(f"{spec['metric']} 展示单位不符合契约")
        except (KeyError, InvalidOperation, ValueError, TypeError) as exc:
            bad.append(f"{spec['metric']} 数值缺失或无效: {exc}")
    return (not bad), ("；".join(bad[:5]) if bad else f"{len(specs)} 项单位契约一致（不涉及未配置指标）")

# ---------- 辅助模式：--status 看进度 ／ --selftest 自检工具包 ----------
def _is_archive(p: Path) -> bool:
    return "归档" in p.name or "-v1." in p.name or "-v2." in p.name or "-v3." in p.name

def _parse_ledger(p: Path):
    """读取第一个实际台账表，忽略示例代码块和后续空白模板。"""
    rows, inside, fenced = [], False, False
    for line in p.read_text(encoding="utf-8").splitlines():
        if line.strip().startswith("```"):
            fenced = not fenced
            continue
        if fenced:
            continue
        if line.startswith("|") and all(k in line for k in ("步", "状态", "下一步")):
            if rows:
                break
            inside = True
            continue
        if inside:
            if not line.startswith("|"):
                if rows:
                    break
                inside = False
                continue
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if len(cells) >= 6 and cells[0].isdigit():
                rows.append(cells)
    return rows


def _find_project_doc(project: Path):
    candidates = []
    package = Path(__file__).parent.resolve()
    excluded = {"README.md", "AGENTS.md", "START-HERE.md", "01-通用手册.md",
                "02-知识库-K1-K11.md", "03-生成定制手册话术.md", "04-进度台账模板.md"}
    for base in (project, project.parent):
        if base.resolve() == package:
            continue
        for p in sorted(base.glob("*.md")):
            if p.name in excluded or _is_archive(p):
                continue
            rows = _parse_ledger(p)
            if len(rows) == 10 and sorted(int(r[0]) for r in rows) == list(range(10)) and p not in candidates:
                candidates.append(p)
        # 当前项目内有台账，就不再到父目录挑另一个项目。
        if candidates:
            break
    return candidates


def _resolve_batch(path: Path):
    """支持目录、软链接和一行文本指针；文本相对其所在目录解析。"""
    seen = set()
    for _ in range(8):
        path = path.resolve()
        if path in seen:
            raise ValueError("批次指针循环")
        seen.add(path)
        if path.is_dir() or not path.exists():
            return path
        target = path.read_text(encoding="utf-8").strip()
        if not target or len(target.splitlines()) != 1:
            raise ValueError(f"批次文本指针须恰有一行路径: {path}")
        next_path = Path(target)
        path = next_path if next_path.is_absolute() else path.parent / next_path
    raise ValueError("批次指针超过 8 层")


def _artifact_paths(cell):
    quoted = re.findall(r"`([^`]+)`", cell)
    return quoted or [x.strip() for x in re.split(r"[;；]", cell) if x.strip()]


def show_status(args):
    project = Path(args.project_root).resolve() if args.project_root else Path.cwd()
    if args.ledger:
        doc = Path(args.ledger)
        doc = doc if doc.is_absolute() else project / doc
        if not doc.is_file():
            print(f"MISSING  指定台账不存在: {doc}")
            return 2
    else:
        candidates = _find_project_doc(project)
        if len(candidates) != 1:
            print("MISSING  未找到唯一项目台账；用 --ledger 指定，脚本不会猜。")
            for candidate in candidates:
                print(f"  候选: {candidate}")
            return 2
        doc = candidates[0]
    rows = _parse_ledger(doc)
    if len(rows) != 10 or sorted(int(r[0]) for r in rows) != list(range(10)):
        print(f"MISSING  {doc}: 台账必须有 0～9 各一行，实际 {len(rows)} 行")
        return 2
    rows.sort(key=lambda r: int(r[0]))
    if not args.project_root and not (project / "artifacts").exists() and (doc.parent / "artifacts").exists():
        project = doc.parent
    print(f"项目文档：{doc}")
    done = [r for r in rows if "✅" in r[2]]
    print(f"台账标记完成：{len(done)}/10（路径检查不代替判据与回溯验收）")
    problems = []
    try:
        pointer = Path(args.batch) if args.batch else project / "artifacts/current"
        if args.batch and not pointer.is_absolute():
            pointer = project / pointer
        batch = _resolve_batch(pointer)
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"MISSING  批次指针无效: {exc}")
        return 2
    for row in rows:
        print(f"  {row[0]:>2}  {row[1]}  {row[2]}  {row[5]}")
        marks = [mark for mark in ("⬜", "🟡", "✅", "⛔") if mark in row[2]]
        if len(marks) != 1:
            problems.append(f"第 {row[0]} 步状态无效")
        paths = _artifact_paths(row[4])
        if "✅" in row[2] and not paths:
            problems.append(f"第 {row[0]} 步标记完成但没有产物路径")
        for name in paths:
            raw = Path(name)
            # 项目根路径优先；批次相对路径仅使用显式批次/current，不选最新目录。
            choices = [raw] if raw.is_absolute() else [project / raw]
            if not raw.is_absolute() and not name.startswith("artifacts/") and batch.is_dir():
                choices.append(batch / raw)
            hits = {x.resolve() for x in choices if x.exists()}
            if len(hits) != 1 and "✅" in row[2]:
                problems.append(f"第 {row[0]} 步产物{'未能按台账路径定位' if not hits else '路径有歧义'}: {name}")
    if problems:
        for problem in problems:
            print(f"MISSING  {problem}")
        print("下一步：先核对这些完成标记，不能据此宣布交付完成。")
        return 2
    pending = next((r for r in rows if "✅" not in r[2]), None)
    if pending is None:
        print("下一步：台账十步均标记完成，所列路径存在；仍须核对判据和回溯件。")
    elif "⛔" in pending[2]:
        print(f"下一步：第 {pending[0]} 步阻塞 {pending[2]}；先解决缺件或等回话。")
    elif "⬜" in pending[2]:
        step = int(pending[0])
        cmd = "C0-A（已有数据台账则 C0-B ＋ C0-T）" if step == 0 else f"C{step}-A ＋ C{step}-A2"
        print(f"下一步：第 {step} 步未开始 → 发 {cmd}")
    else:
        print(f"下一步：第 {pending[0]} 步进行中；先按该步判据验收，完成后再发 {pending[5]}。")
    return 0


def selftest():
    base = Path(__file__).parent
    need = ["README.md", "START-HERE.md", "AGENTS.md", "01-通用手册.md",
            "02-知识库-K1-K11.md", "03-生成定制手册话术.md", "04-进度台账模板.md",
            "check.py", "test_check.py", "check-config.example.json", "全景图-十步.svg", "LICENSE"]
    must_have = {
        "README.md": ["先看清，再动手", "行数对，不等于值对", "不知道的不估算", "第一次用：头一小时怎么过",
                      "按它的 AGENTS.md 执行"],
        "AGENTS.md": ["五条铁律", "一次只做一步", "遇到新情况先报影响面", "回溯件已生成"],
        "01-通用手册.md": ["验收四件套", "查回溯件", "中途变了怎么办", "必须停下来", "字段验"],
        "03-生成定制手册话术.md": ["我做四件事", "必留的回溯件", "只验数不验回溯", "手册是活文档"],
        "START-HERE.md": ["--status", "先跑一条命令", "按它的 AGENTS.md 执行"],
    }
    fails = []
    print(f"工具包自检：{base}")
    print("-" * 60)

    missing = [f for f in need if not (base / f).exists()]
    print(f"{'PASS' if not missing else 'FAIL'}  必备文件 {len(need) - len(missing)}/{len(need)}"
          + ("" if not missing else f"  缺: {missing}"))
    if missing:
        fails.append("必备文件")

    for f in sorted(base.glob("*.md")):
        lines = f.read_text(encoding="utf-8").splitlines()
        fences = sum(1 for l in lines if l.strip().startswith("```"))
        if fences % 2:
            fails.append(f"{f.name} 代码块未闭合")
            print(f"FAIL  {f.name} 代码块 {fences} 个（应为偶数）")
    print(f"{'PASS' if not any('代码块' in x for x in fails) else 'FAIL'}  代码块配对")

    for f, kws in must_have.items():
        p = base / f
        if not p.exists():
            continue
        text = p.read_text(encoding="utf-8")
        lost = [k for k in kws if k not in text]
        print(f"{'PASS' if not lost else 'FAIL'}  {f} 关键约束 {len(kws) - len(lost)}/{len(kws)}"
              + ("" if not lost else f"  缺: {lost}"))
        if lost:
            fails.append(f"{f} 关键约束")

    # 断言覆盖度：哪些步已内置、哪些还要 AI 补（防止未来误删断言）
    covered = sorted({str(st)[0] for st, _, _ in CHECKS if str(st)[0].isdigit()})
    uncovered = [str(i) for i in range(10) if str(i) not in covered]
    print(f"PASS  断言 {len(CHECKS)} 条，已覆盖第 {'/'.join(covered)} 步"
          + (f"；未覆盖 {','.join(uncovered)}（这几步靠 D 段人工或让 AI 补）" if uncovered else ""))

    ver_ok = False
    p01 = base / "01-通用手册.md"
    if p01.exists():
        head = p01.read_text(encoding="utf-8").splitlines()[:5]
        ver_ok = any("版本 v" in l for l in head)
    print(f"{'PASS' if ver_ok else 'FAIL'}  01 顶部有版本号")
    if not ver_ok:
        fails.append("版本号")

    regression = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", str(base), "-p", "test_check.py", "-v"],
        cwd=base, capture_output=True, text=True, timeout=120)
    regression_output = regression.stderr + regression.stdout
    count = re.search(r"Ran (\d+) tests?", regression_output)
    regression_ok = regression.returncode == 0 and count and int(count[1]) > 0
    print(f"{'PASS' if regression_ok else 'FAIL'}  行为回归（临时合成材料）")
    print(regression_output.strip())
    if not regression_ok:
        fails.append("行为回归")

    print("-" * 60)
    print("结果:", "全部通过" if not fails else f"{len(fails)} 项有问题：{fails}")
    return 1 if fails else 0

# ---------- 命令入口 ----------
def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="ETL 批次检查与台账导航；缺件不算通过")
    parser.add_argument("batch", nargs="?", help="批次目录或文本/软链接指针")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--status", action="store_true")
    mode.add_argument("--selftest", action="store_true")
    parser.add_argument("--stage", type=int, choices=range(10), help="验收范围：只验这一阶段的内置检查")
    parser.add_argument("--project-root", help="项目根，原始区默认为其 data/raw")
    parser.add_argument("--ledger", help="指定台账；相对项目根")
    parser.add_argument("--config", help="检查配置 JSON；相对项目根，默认 check_config.json（若存在）")
    return parser.parse_args(argv)


def configure(args):
    global BATCH, ROOT, RAW_DIR, CONFIG, RUN_JSON, PROFILE_CANDIDATES
    global DIAG_CANDIDATES, RULES, COUNTS, SECOND, RECON, COVER_CANDIDATES, METRICS_CANDIDATES
    project = Path(args.project_root).resolve() if args.project_root else Path.cwd()
    raw_batch = Path(args.batch or "artifacts/current")
    BATCH = _resolve_batch(raw_batch if raw_batch.is_absolute() else project / raw_batch)
    ROOT = project if args.project_root else _find_project_root(BATCH)
    RAW_DIR = ROOT / "data/raw"
    config_path = Path(args.config) if args.config else project / "check_config.json"
    if not config_path.is_absolute():
        config_path = project / config_path
    if args.config and not config_path.is_file():
        raise ValueError(f"指定配置不存在: {config_path}")
    CONFIG = load_json(config_path) if config_path.is_file() else {}
    if not isinstance(CONFIG, dict):
        raise ValueError("检查配置必须是 JSON 对象")
    unknown = set(CONFIG) - {"field_checks", "unit_checks"}
    if unknown:
        raise ValueError(f"未知配置键: {sorted(unknown)}")
    RUN_JSON = BATCH / "run.json"
    PROFILE_CANDIDATES = [BATCH / x for x in ("profile/profile_report.json", "reports/profile_report.json", "profile_report.json")]
    DIAG_CANDIDATES = [BATCH / x for x in ("reports/diagnosis.md", "reports/diagnosis/diagnosis.md", "diagnosis.md")]
    RULES = BATCH / "cleaning/cleaning_rules.yaml"
    COUNTS = BATCH / "reconcile/counts.json"
    SECOND = BATCH / "reconcile/second_run_accepted.txt"
    RECON = BATCH / "reports/reconciliation.json"
    COVER_CANDIDATES = [BATCH / x for x in ("reports/coverage_notes.md", "reports/coverage.md")]
    METRICS_CANDIDATES = [BATCH / x for x in ("reports/metrics_run.json", "metrics/metrics_run.json", "metrics_run.json")]


def main(argv=None):
    args = parse_args(argv)
    if args.selftest:
        return selftest()
    if args.status:
        return show_status(args)
    try:
        configure(args)
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"MISSING  无法确定检查输入: {exc}")
        return 2
    if not BATCH.is_dir():
        print(f"MISSING  批次目录不存在: {BATCH}")
        return 2
    selected = [(stage, name, fn) for stage, name, fn in CHECKS
                if args.stage is None or int(str(stage)[0]) == args.stage]
    if not selected:
        print(f"MISSING  第 {args.stage} 步没有内置断言，需要项目检查或人工验收")
        return 2
    fails = passed = missing = 0
    print(f"批次: {BATCH}；范围: {'巡检（不代表整体验收）' if args.stage is None else '第 ' + str(args.stage) + ' 步内置检查'}")
    for stage, name, fn in selected:
        try:
            r = fn()
        except Exception as exc:
            r = ("SKIP", f"无法执行检查: {exc}")
        if r is None or (isinstance(r, tuple) and r[0] == "SKIP"):
            missing += 1
            detail = r[1] if r else "必需产物未找到或条件不足"
            print(f"MISSING  {stage}  {name}  {detail}")
        else:
            ok, detail = r
            passed += bool(ok)
            fails += not ok
            print(f"{'PASS' if ok else 'FAIL'}  {stage}  {name}  {detail}")
    print(f"结果: {passed} 条通过、{fails} 条冲突、{missing} 条未完成验证。")
    print("内置检查不代替项目完整判据、记录级去向和人工验收。")
    return 1 if fails else 2 if missing or not passed else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"MISSING  输入无法读取: {exc}")
        sys.exit(2)
