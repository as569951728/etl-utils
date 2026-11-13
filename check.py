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
import re
import json
import hashlib
from pathlib import Path

# ---------- 定位批次 ----------
BATCH = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("artifacts/current")
BATCH = BATCH.resolve()

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
# 不绑定报告结构与键名：按"形状"识别 file→col→info（三种常见写法都认），认不出就 SKIP。
PROFILE_CANDIDATES = [
    BATCH / "profile" / "profile_report.json",
    BATCH / "reports" / "profile_report.json",
    BATCH / "profile_report.json",
]
TYPE_KEYS = ("type", "dtype", "data_type", "数据类型")
MISS_KEYS = ("missing", "nulls", "null_stats", "na", "empty",
             "empty_total", "empty_by_style", "null_count", "missing_count")
DIST_KEYS = ("distribution", "dist", "distinct", "top", "top10", "top_values",
             "values", "value_counts", "取值分布")

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
    checked, bad = 0, []
    for f, c, info in _iter_profile_fields(rep):
        total, by = _missing_of(info)
        if not isinstance(total, int) or not isinstance(by, dict):
            continue
        checked += 1
        if sum(by.values()) != total:
            bad.append(f"{f}.{c}: 分写法 {sum(by.values())} ≠ 总空值 {total}")
    if checked == 0:
        return ("SKIP", '报告里没有「分写法计数」这一项（只给了总数也算正常）')
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
RECON = BATCH / "reports" / "reconciliation.json"

@check("4对账", "read = accepted + quarantined + deduplicated")
def _():
    # 形式一：单表单文件 {"read","accepted","quarantined","deduplicated"}
    c = load_json(COUNTS)
    if c is not None:
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
            continue
        read = v.get("read")
        acc = v.get("accepted", v.get("accepted_derived"))
        if read is None or acc is None:
            continue
        rhs = acc + v.get("quarantined", 0) + v.get("deduplicated", 0)
        checked += 1
        if read != rhs:
            bad.append(f"{table}: read={read} ≠ {acc}+{v.get('quarantined', 0)}+{v.get('deduplicated', 0)}={rhs}")
    if checked == 0:
        return None
    return (not bad), (f"{checked} 张表等式全部成立" if not bad else "；".join(bad[:3]))

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
MAJOR_UNITS = ("元", "万元", "千元", "亿元", "美元")  # 主单位；最小单位（分／cent）不在其中

@check("7指标", "unit 描述的是 value（同一指标里出现 10/100/1000 倍的双数值且 unit 标主单位＝错位）")
def _():
    d = None
    for p in METRICS_CANDIDATES:
        d = load_json(p)
        if d:
            break
    if not d:
        return None
    items = d.get("metrics") if isinstance(d, dict) else d
    if not isinstance(items, list) or not items:
        return None

    bad, checked = [], 0
    for m in items:
        if not isinstance(m, dict):
            continue
        unit = str(m.get("unit") or "")
        nums = {k: v for k, v in m.items()
                if isinstance(v, (int, float)) and not isinstance(v, bool)}
        if len(nums) < 2:
            continue
        checked += 1
        keys = list(nums)
        for i in range(len(keys)):
            for j in range(i + 1, len(keys)):
                a, b = nums[keys[i]], nums[keys[j]]
                if not a or not b or a == b:
                    continue
                ratio = max(abs(a), abs(b)) / min(abs(a), abs(b))
                for p in (10, 100, 1000):
                    if abs(ratio - p) < 0.01 and any(u in unit for u in MAJOR_UNITS):
                        big, small = (keys[i], keys[j]) if abs(a) > abs(b) else (keys[j], keys[i])
                        bad.append(f"{m.get('id')}: {big}={max(abs(a), abs(b))} 与 {small}={min(abs(a), abs(b))} 差 {p} 倍，unit 却是「{unit}」")
                        break
    if checked == 0:
        return None
    if bad:
        seen, uniq = set(), []
        for x in bad:
            k = x.split(":")[0]
            if k not in seen:
                seen.add(k); uniq.append(x)
        return False, ("；".join(uniq[:3])
                       + "　→ unit 只能描述 value；两个形态各带各的单位（若这两个字段本就是不同量，忽略本条）")
    return True, f"{checked} 项含多个数值字段，未发现单位错位"

# ---------- 辅助模式：--status 看进度 ／ --selftest 自检工具包 ----------
def _is_archive(p: Path) -> bool:
    return "归档" in p.name or "-v1." in p.name or "-v2." in p.name or "-v3." in p.name

def _looks_like_ledger(p: Path) -> bool:
    """不靠文件名判断：内容里有『步 / 状态 / 下一步』的表头就算进度表。"""
    try:
        head = p.read_text(encoding="utf-8", errors="ignore")[:30000]
    except Exception:
        return False
    return "| 步" in head and "状态" in head and "下一步" in head

def _find_project_doc():
    """先按常见命名找；找不到就扫当前目录所有 md，按内容识别。"""
    named = []
    for base in (Path.cwd(), Path.cwd().parent):
        for pat in ("*工作手册*.md", "*进度台账*.md", "*ETL*.md"):
            named += [p for p in sorted(base.glob(pat)) if not _is_archive(p)]
    for p in named:
        if _looks_like_ledger(p):
            return p
    for p in sorted(Path.cwd().glob("*.md")):        # 名字完全自定义时也能找到
        if not _is_archive(p) and _looks_like_ledger(p):
            return p
    return named[0] if named else None

def _parse_ledger(p: Path):
    """从手册里解析台账表：返回 [[步, 名称, 状态, 判据, 产出物, 下一步], ...]"""
    rows, inside = [], False
    for line in p.read_text(encoding="utf-8").splitlines():
        if line.startswith("|") and "步" in line and "状态" in line and "下一步" in line:
            inside = True
            continue
        if inside:
            if not line.startswith("|"):
                break
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if len(cells) >= 6 and cells[0].isdigit():
                rows.append(cells)
    return rows

def show_status():
    doc = _find_project_doc()
    if doc is None:
        print("没有找到项目手册或台账。")
        print("→ 新项目：按 START-HERE.md 的 A 段开始；")
        print("→ 已有项目：cd 到项目根（有 artifacts/ 的那一层）再跑一次。")
        return 0

    text = doc.read_text(encoding="utf-8")
    ver = ""
    for line in text.splitlines()[:8]:
        if "版本" in line and "v" in line:
            ver = line.strip().lstrip("> ").replace("**", "")
            break
    rows = _parse_ledger(doc)

    print(f"项目文档：{doc.name}")
    if ver:
        print(f"　{ver[:90]}")
    if not rows:
        print("　⚠ 没解析到台账表（表头需含『步 / 状态 / 下一步』）。")
        return 0

    done = [r for r in rows if "✅" in r[2]]
    print(f"进度：{len(done)}/{len(rows)} 步完成")
    print("-" * 60)
    for r in rows:
        mark = r[2][:2]
        print(f"  {r[0]:>2}  {r[1]:<14} {mark}  {r[5][:30]}")
    print("-" * 60)

    pending = next((r for r in rows if "✅" not in r[2]), None)
    if pending is None:
        print("下一步：十步已全部完成 → 做交付与复盘（或按变更记录继续维护）。")
        return 0

    step = int(pending[0])
    paths = [s.strip("` ") for s in pending[4].replace("；", ";").split(";") if s.strip()]
    present = [pth for pth in paths if Path(pth).exists()]
    missing = [pth for pth in paths if not Path(pth).exists()]

    if "⬜" in pending[2]:
        cmd = ("C0-B ＋ C0-T（建批次目录与 run.json；若台账没写先补 C0-A）"
               if step == 0 else f"C{step}-A ＋ C{step}-A2")
        print(f"下一步：第 {step} 步「{pending[1]}」还没开始 → 发 {cmd}")
    elif "⛔" in pending[2]:
        print(f"下一步：第 {step} 步「{pending[1]}」阻塞 {pending[2].lstrip('⛔').strip()}")
        print("　先解决缺件／等回话，不要往下推进。")
    else:  # 🟡 进行中
        print(f"下一步：第 {step} 步「{pending[1]}」进行中")
        if missing:
            print(f"　产物还没齐（缺 {len(missing)} 个）→ 先把第 {step} 步做完，别急着发下一步")
        else:
            print("　产物已在 → 对照定制手册的验收节补跑断言、勾判据；勾满再发 " + pending[5])

    for pth in paths:
        print(f"　预期产物：{pth}  {'✓ 在' if Path(pth).exists() else '✗ 不存在'}")
    return 0

def selftest():
    base = Path(__file__).parent
    need = ["README.md", "START-HERE.md", "AGENTS.md", "01-通用手册.md",
            "02-知识库-K1-K11.md", "03-生成定制手册话术.md", "04-进度台账模板.md",
            "check.py", "全景图-十步.svg", "LICENSE"]
    must_have = {
        "AGENTS.md": ["五条铁律", "一次只做一步", "遇到新情况先报影响面", "回溯件已生成"],
        "01-通用手册.md": ["验收四件套", "查回溯件", "中途变了怎么办", "必须停下来", "字段验"],
        "03-生成定制手册话术.md": ["我做四件事", "必留的回溯件", "只验数不验回溯", "手册是活文档"],
        "START-HERE.md": ["--status", "先跑一条命令"],
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

    print("-" * 60)
    print("结果:", "全部通过" if not fails else f"{len(fails)} 项有问题：{fails}")
    return 1 if fails else 0

# ---------- 跑灯表 ----------
def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--status":
        sys.exit(show_status())
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        sys.exit(selftest())

    if not BATCH.exists():
        print(f"✗ 批次目录不存在：{BATCH}")
        print("  用法：python3 check.py <批次目录>，例：artifacts/b01-20261113")
        print("  不知道批次在哪：python3 check.py --status")
        sys.exit(2)

    markers = ("run.json", "profile", "cleaning", "reports", "reconcile", "metrics", "delivery")
    if not any((BATCH / m).exists() for m in markers):
        print(f"✗ 这个目录不像批次目录（没有 {'／'.join(markers)} 中任何一个）：{BATCH}")
        subs = sorted(p.name for p in BATCH.iterdir() if p.is_dir())[:6]
        if subs:
            print(f"  它下面有：{subs}")
            print("  是不是想传其中某一个？例：artifacts/<批次名>")
        sys.exit(2)

    fails = passed = skipped = 0
    print(f"批次: {BATCH}")
    print("-" * 64)
    for stage, name, fn in CHECKS:
        try:
            r = fn()
        except Exception as e:
            r = (False, f"断言自身报错: {e}")
        if r is None:
            status, detail = "SKIP", "产物未生成（到这一步再跑）"
        elif isinstance(r, tuple) and len(r) == 2 and r[0] == "SKIP":
            status, detail = "SKIP", r[1]
        else:
            ok, detail = r
            status = "PASS" if ok else "FAIL"
            if ok:
                passed += 1
            else:
                fails += 1
        if status == "SKIP":
            skipped += 1
        print(f"{status:4}  {stage}  {name}  {detail}")
    print("-" * 64)
    if fails:
        print("结果:", f"{fails} 条 FAIL，打回重做")
    elif passed == 0:
        print(f"⚠ 0 条通过、{skipped} 条跳过——**没有任何断言真正执行**")
        print("  多半是产物还没生成，或者路径指到了别的批次。先跑 --status 确认。")
        sys.exit(2)
    else:
        print("结果:", f"{passed} 条通过" + (f"、{skipped} 条跳过（未到那一步）" if skipped else ""))
    sys.exit(1 if fails else 0)

if __name__ == "__main__":
    main()
