#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gp_skew_inspector.py  (Greenplum 6.x 전용)

테이블의 **분산 스큐(distribution skew)** 를 점검한다.
= 세그먼트 간에 데이터가 얼마나 고르게 분포하는가.  (DISTRIBUTED BY 키가
나쁘면 특정 세그먼트로 데이터가 몰려 MPP 성능이 급락한다.)

각 베이스 테이블(파티션 루트 포함, 자식 파티션 제외)에 대해
  - 분산 정책(DISTRIBUTED BY / RANDOMLY)
  - 세그먼트별 행 수
를 수집하여 스큐 지표(max/avg, 변동계수 CV, 빈 세그먼트)를 계산하고,
심한 순으로 정렬해 보여준 뒤 데이터에 근거한 점검·조치 절차를 제시한다.

필요 패키지:
  pip install psycopg2-binary

사용 예:
  PGPASSWORD=secret python3 gp_skew_inspector.py \
      --host 10.0.0.10 --dbname mydb --schema myschema

  # DB 전체 스키마, 스큐 있는 것만
  python3 gp_skew_inspector.py --dbname mydb --only-skewed

  # 단일 테이블
  python3 gp_skew_inspector.py --schema myschema --table sales
"""

import argparse
import csv
import os
import shutil
import statistics
import sys
import unicodedata

try:
    import psycopg2
    import psycopg2.extras
    from psycopg2 import sql
except ImportError:
    sys.stderr.write(
        "psycopg2 가 필요합니다.  pip install psycopg2-binary 후 다시 실행하세요.\n"
    )
    sys.exit(1)

from gp_common import human_bytes, get_connection


# ---------------------------------------------------------------------------
# 색상 / 표시 헬퍼
# ---------------------------------------------------------------------------
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[31m"
YELLOW = "\033[33m"
GREEN = "\033[32m"

_SPARK = "▁▂▃▄▅▆▇█"
VERDICT_COLOR = {"OK": GREEN, "WARN": YELLOW, "CRIT": RED, "EMPTY": DIM, "SKIP": DIM}


def human_count(n):
    """행 수를 1.2K / 3.4M 처럼 1000 단위로 축약."""
    if n is None:
        return ""
    n = float(n)
    for unit in ("", "K", "M", "B", "T"):
        if abs(n) < 1000.0 or unit == "T":
            return f"{int(n)}" if unit == "" else f"{n:.1f}{unit}"
        n /= 1000.0


def _dw(s):
    """출력 폭(한글/전각 2칸)."""
    return sum(2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
               for ch in str(s))


def _pad(s, width, align="left"):
    s = str(s)
    gap = max(0, width - _dw(s))
    return (" " * gap + s) if align == "right" else (s + " " * gap)


# ---------------------------------------------------------------------------
# 카탈로그 수집
# ---------------------------------------------------------------------------
def segment_count(cur):
    """프라이머리 세그먼트 개수(마스터 제외)."""
    cur.execute(
        "SELECT count(*) AS n FROM gp_segment_configuration "
        "WHERE role = 'p' AND content >= 0;"
    )
    return cur.fetchone()["n"]


def list_user_schemas(cur):
    cur.execute(
        """
        SELECT nspname
        FROM   pg_catalog.pg_namespace
        WHERE  nspname NOT IN ('pg_catalog', 'information_schema',
                               'gp_toolkit', 'pg_aoseg', 'pg_bitmapindex',
                               'pg_toast')
          AND  nspname NOT LIKE 'pg_temp_%'
          AND  nspname NOT LIKE 'pg_toast_temp_%'
        ORDER  BY nspname;
        """
    )
    return [r["nspname"] for r in cur.fetchall()]


def fetch_base_tables(cur, schema):
    """스키마의 베이스 테이블 메타: [{oid, name, distributedby}].

    relkind='r' 이며 다른 테이블의 자식(파티션)이 아닌 것.
    외부/뷰(relstorage x/v)는 제외한다.
    """
    try:
        cur.execute(
            """
            SELECT c.oid,
                   c.relname AS name,
                   pg_catalog.pg_get_table_distributedby(c.oid) AS distributedby
            FROM   pg_catalog.pg_class c
            JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE  n.nspname = %s
              AND  c.relkind = 'r'
              AND  c.relstorage NOT IN ('x', 'v', 'f')
              AND  NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i
                               WHERE i.inhrelid = c.oid)
            ORDER  BY c.relname;
            """,
            (schema,),
        )
        return [dict(r) for r in cur.fetchall()]
    except psycopg2.Error:
        # pg_get_table_distributedby 미지원 등 → 정책 유형만이라도 가져온다.
        cur.connection.rollback()
        cur.execute(
            """
            SELECT c.oid,
                   c.relname AS name,
                   CASE p.policytype WHEN 'r' THEN 'DISTRIBUTED REPLICATED'
                                     WHEN 'p' THEN 'DISTRIBUTED (키 확인 필요)'
                                     ELSE NULL END AS distributedby
            FROM   pg_catalog.pg_class c
            JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT   JOIN gp_distribution_policy p ON p.localoid = c.oid
            WHERE  n.nspname = %s
              AND  c.relkind = 'r'
              AND  c.relstorage NOT IN ('x', 'v', 'f')
              AND  NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i
                               WHERE i.inhrelid = c.oid)
            ORDER  BY c.relname;
            """,
            (schema,),
        )
        return [dict(r) for r in cur.fetchall()]


def fetch_sizes(cur, schema):
    """베이스 테이블별 총 용량(파티션 자식 포함). 반환: dict[oid]=bytes."""
    cur.execute(
        """
        WITH RECURSIVE tree(root, oid) AS (
            SELECT c.oid, c.oid
            FROM   pg_catalog.pg_class c
            JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE  n.nspname = %s
              AND  c.relkind = 'r'
              AND  c.relstorage NOT IN ('x', 'v', 'f')
              AND  NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i2
                               WHERE i2.inhrelid = c.oid)
            UNION ALL
            SELECT t.root, i.inhrelid
            FROM   tree t JOIN pg_catalog.pg_inherits i ON i.inhparent = t.oid
        )
        SELECT root, sum(pg_total_relation_size(oid)) AS size_bytes
        FROM   tree
        GROUP  BY root;
        """,
        (schema,),
    )
    return {r["root"]: (r["size_bytes"] or 0) for r in cur.fetchall()}


def fetch_segment_rowcounts(cur, schema, table):
    """세그먼트별 행 수. 반환: dict[gp_segment_id]=rows (0 인 세그먼트는 없음)."""
    cur.execute(
        sql.SQL(
            "SELECT gp_segment_id AS seg, count(*)::bigint AS rows "
            "FROM {}.{} GROUP BY gp_segment_id"
        ).format(sql.Identifier(schema), sql.Identifier(table))
    )
    return {r["seg"]: r["rows"] for r in cur.fetchall()}


# ---------------------------------------------------------------------------
# 스큐 지표 계산
# ---------------------------------------------------------------------------
def compute_skew(counts, nseg):
    """counts: dict[seg]=rows, nseg: 전체 프라이머리 세그먼트 수.

    반환 dict: total, avg, max_rows/seg, min_rows/seg, empty, ratio, cv.
    """
    present = dict(counts)
    total = sum(present.values())
    # 0 행 세그먼트를 포함한 전체 분포 벡터
    full = list(present.values()) + [0] * max(0, nseg - len(present))
    avg = total / nseg if nseg else 0

    max_seg = max(present, key=present.get) if present else None
    max_rows = present[max_seg] if max_seg is not None else 0
    empty = nseg - len(present)
    if empty > 0:
        min_seg, min_rows = None, 0  # 비어있는 세그먼트가 곧 최소
    elif present:
        min_seg = min(present, key=present.get)
        min_rows = present[min_seg]
    else:
        min_seg, min_rows = None, 0

    ratio = (max_rows / avg) if avg else 0.0
    cv = (statistics.pstdev(full) / avg * 100.0) if avg and len(full) > 1 else 0.0

    return {
        "total": total, "avg": avg, "full": full,
        "max_seg": max_seg, "max_rows": max_rows,
        "min_seg": min_seg, "min_rows": min_rows,
        "empty": empty, "nseg": nseg, "ratio": ratio, "cv": cv,
    }


def verdict_of(m, warn, crit):
    if m["total"] == 0:
        return "EMPTY"
    v = "OK"
    if m["ratio"] >= crit:
        v = "CRIT"
    elif m["ratio"] >= warn:
        v = "WARN"
    if m["empty"] > 0 and v == "OK":
        v = "WARN"
    return v


def parse_distkey(distributedby):
    """'DISTRIBUTED BY (a, b)' → ['a','b'];  RANDOMLY/REPLICATED → []."""
    if not distributedby:
        return []
    up = distributedby.upper()
    if "RANDOMLY" in up or "REPLICATED" in up:
        return []
    l, r = distributedby.find("("), distributedby.rfind(")")
    if l == -1 or r <= l:
        return []
    return [c.strip().strip('"') for c in distributedby[l + 1:r].split(",") if c.strip()]


# ---------------------------------------------------------------------------
# 렌더링
# ---------------------------------------------------------------------------
def sparkline(values, width=50):
    """값 리스트를 유니코드 스파크라인으로. width 초과 시 평균으로 버킷팅."""
    if not values:
        return ""
    vals = list(values)
    if len(vals) > width:
        step = len(vals) / width
        vals = [
            statistics.mean(vals[int(i * step):max(int((i + 1) * step), int(i * step) + 1)])
            for i in range(width)
        ]
    mx = max(vals) or 1
    return "".join(_SPARK[int(round(v / mx * (len(_SPARK) - 1)))] for v in vals)


def render(results, nseg, warn, crit, only_skewed, use_color):
    c = (lambda code, s: f"{code}{s}{RESET}") if use_color else (lambda code, s: s)
    out = []

    out.append(c(BOLD, "분산 스큐 점검 (distribution skew)"))
    out.append(
        f"프라이머리 세그먼트: {nseg}개   "
        f"임계치: WARN max/avg≥{warn}, CRIT max/avg≥{crit}   "
        f"대상 테이블: {len(results)}개"
    )
    out.append("=" * 78)

    shown = [r for r in results if not (only_skewed and r["verdict"] in ("OK", "EMPTY", "SKIP"))]
    if not shown:
        out.append("표시할 테이블이 없습니다. (스큐 없음 또는 필터로 모두 제외)")
        return "\n".join(out)

    # ---- 순위 요약 그리드 ----
    headers = ["#", "TABLE", "DIST", "ROWS", "SIZE", "MAX/AVG", "CV%", "EMPTY", "VERDICT"]
    grid = []
    for i, r in enumerate(shown, 1):
        m = r["metrics"]
        if r["verdict"] == "SKIP":
            grid.append([str(i), f"{r['schema']}.{r['name']}", r["dist_short"],
                         "-", human_bytes(r["size"]), "-", "-", "-", "SKIP"])
            continue
        grid.append([
            str(i),
            f"{r['schema']}.{r['name']}",
            r["dist_short"],
            human_count(m["total"]),
            human_bytes(r["size"]),
            f"{m['ratio']:.2f}" if m["total"] else "-",
            f"{m['cv']:.0f}" if m["total"] else "-",
            f"{m['empty']}/{nseg}",
            r["verdict"],
        ])

    widths = [max(_dw(h), max((_dw(row[j]) for row in grid), default=0))
              for j, h in enumerate(headers)]
    aligns = ["right", "left", "left", "right", "right", "right", "right", "right", "left"]

    def fmt(cells, colorize=None):
        parts = [_pad(cell, widths[j], aligns[j]) for j, cell in enumerate(cells)]
        line = "  ".join(parts)
        return line

    out.append(c(BOLD, fmt(headers)))
    out.append("-" * _dw(fmt(headers)))
    for row, r in zip(grid, shown):
        line = fmt(row)
        col = VERDICT_COLOR.get(r["verdict"])
        out.append(c(col, line) if col else line)

    # ---- 스큐 테이블 상세 ----
    skewed = [r for r in shown if r["verdict"] in ("WARN", "CRIT")]
    if skewed:
        out.append("")
        out.append(c(BOLD, "▼ 스큐 상세 및 점검 포인트"))
        for r in skewed:
            m = r["metrics"]
            col = VERDICT_COLOR[r["verdict"]]
            out.append("")
            out.append(c(col, f"▶ {r['schema']}.{r['name']}  [{r['verdict']}]  "
                              f"max/avg={m['ratio']:.2f}  CV={m['cv']:.0f}%  "
                              f"empty={m['empty']}/{nseg}"))
            out.append(f"    분산: {r['distributedby'] or '(정보 없음)'}")
            spark = sparkline(sorted(m["full"], reverse=True))
            maxinfo = f"max={human_count(m['max_rows'])}(seg {m['max_seg']})"
            mininfo = (f"min={human_count(m['min_rows'])}"
                       + (f"(seg {m['min_seg']})" if m['min_seg'] is not None else "(빈 세그먼트)"))
            out.append(f"    세그먼트 분포(내림차순): {spark}")
            out.append(f"      {maxinfo}   {mininfo}   avg={human_count(m['avg'])}")
            for line in remediation_lines(r, m):
                out.append(line)

    return "\n".join(out)


def remediation_lines(r, m):
    """수집된 데이터에 근거한 점검/조치 제안."""
    keys = parse_distkey(r["distributedby"])
    schema, name = r["schema"], r["name"]
    fqtn = f"{schema}.{name}"
    lines = []

    if not keys:
        # RANDOMLY / REPLICATED 인데도 스큐
        lines.append("    ▷ 점검: RANDOM/REPLICATED 분산인데 편중 → AO/압축 파일 크기 "
                     "불균형이거나 세그먼트 하드웨어 이슈 가능. 데이터/디스크 확인.")
        return lines

    keycol = keys[0]
    keylist = ", ".join(keys)

    if m["empty"] > 0:
        lines.append(f"    ▷ 점검: 빈 세그먼트 {m['empty']}개 → 분산키 카디널리티 부족 의심")
        lines.append(f"        SELECT count(DISTINCT ({keylist})) AS distinct_keys FROM {fqtn};")
        lines.append("        -- distinct_keys 가 세그먼트 수보다 충분히 커야 고르게 분산됨")
    else:
        lines.append("    ▷ 점검: 특정 세그먼트 집중 → 분산키 값 편향/NULL 쏠림 의심")

    lines.append(f"        -- 분산키 값 빈도 상위 확인")
    lines.append(f"        SELECT {keylist}, count(*) AS c FROM {fqtn} "
                 f"GROUP BY {keylist} ORDER BY c DESC LIMIT 20;")
    lines.append(f"        -- NULL 쏠림 확인 (NULL 은 한 세그먼트로 몰림)")
    lines.append(f"        SELECT sum(CASE WHEN {keycol} IS NULL THEN 1 ELSE 0 END) AS null_rows "
                 f"FROM {fqtn};")
    lines.append(f"    ▷ 조치: 카디널리티 높고 균일한 컬럼으로 재분산")
    lines.append(f"        ALTER TABLE {fqtn} SET DISTRIBUTED BY (<더_균일한_키>) "
                 f"WITH (reorganize=true);")
    return lines


CHECKLIST = """\
==== 스큐 점검 절차 ====
1) VERDICT 가 CRIT 인 테이블부터, max/avg 가 큰 순으로 확인한다.
   (max/avg = 가장 무거운 세그먼트 / 평균.  1.0 이 완벽, 클수록 편중)
2) 분산키 값 분포 확인:
     SELECT <key>, count(*) c FROM <schema.table> GROUP BY <key> ORDER BY c DESC LIMIT 20;
   → 소수 값이 대부분을 차지하면 그 값들이 특정 세그먼트로 몰린다.
3) NULL 쏠림 확인:  NULL 은 모두 같은 세그먼트로 간다. NULL 비중이 크면 스큐 원인.
4) 카디널리티 확인:  count(DISTINCT <key>) 가 세그먼트 수보다 충분히 커야 한다.
   (빈 세그먼트가 있으면 카디널리티 부족 신호)
5) 재분산:
     ALTER TABLE <schema.table> SET DISTRIBUTED BY (<더 균일한 키>) WITH (reorganize=true);
   - 조인 콜로케이션을 유지하려면 자주 조인하는 키를, 균일성이 우선이면 고유도 높은 키를 택한다.
   - 마땅한 키가 없으면 DISTRIBUTED RANDOMLY (콜로케이션은 포기).
6) 재분산 후 본 스크립트를 다시 실행해 max/avg 가 1 에 가까워졌는지 확인한다.
"""


def write_csv(path, results, nseg):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["schema", "table", "distributedby", "total_rows", "size_bytes",
                    "segments", "max_rows", "avg_rows", "max_over_avg", "cv_pct",
                    "empty_segments", "verdict"])
        for r in results:
            m = r["metrics"]
            w.writerow([
                r["schema"], r["name"], r["distributedby"] or "",
                m["total"] if r["verdict"] != "SKIP" else "",
                r["size"], nseg,
                m["max_rows"] if r["verdict"] != "SKIP" else "",
                f"{m['avg']:.1f}" if r["verdict"] != "SKIP" else "",
                f"{m['ratio']:.3f}" if (r["verdict"] not in ("SKIP", "EMPTY")) else "",
                f"{m['cv']:.1f}" if (r["verdict"] not in ("SKIP", "EMPTY")) else "",
                m["empty"] if r["verdict"] != "SKIP" else "",
                r["verdict"],
            ])
    print(f"CSV 저장 완료: {path}")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def parse_args():
    ap = argparse.ArgumentParser(
        description="Greenplum 6.x 분산 스큐(세그먼트 간 데이터 편중) 점검"
    )
    ap.add_argument("--host", default=os.environ.get("PGHOST", "localhost"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", 5432)))
    ap.add_argument("--dbname", default=os.environ.get("PGDATABASE", "postgres"))
    ap.add_argument("--user", default=os.environ.get("PGUSER", "gpadmin"))
    ap.add_argument("--password", default=None, help="미지정 시 PGPASSWORD 또는 프롬프트")
    ap.add_argument("--schema", default=None,
                    help="대상 스키마. 미지정 시 DB 의 모든 사용자 스키마")
    ap.add_argument("--table", default=None, help="단일 테이블만 점검(--schema 필요)")
    ap.add_argument("--warn", type=float, default=1.3, help="WARN 임계치 max/avg (기본 1.3)")
    ap.add_argument("--crit", type=float, default=2.0, help="CRIT 임계치 max/avg (기본 2.0)")
    ap.add_argument("--min-size-mb", type=float, default=1.0,
                    help="이 크기 미만 테이블은 스캔 생략(기본 1MB)")
    ap.add_argument("--limit", type=int, default=None,
                    help="용량 상위 N개 테이블만 스캔(대규모 DB 비용 제한)")
    ap.add_argument("--only-skewed", action="store_true",
                    help="WARN/CRIT 테이블만 표시")
    ap.add_argument("--csv", default=None, help="지표를 CSV 로 저장")
    ap.add_argument("--no-color", action="store_true", help="ANSI 색상 비활성화")
    ap.add_argument("--color", action="store_true", help="비-TTY 에서도 색상 강제")
    ap.add_argument("--timeout", type=int, default=15)
    return ap.parse_args()


def collect(cur, schema, args, nseg, min_bytes):
    """한 스키마의 베이스 테이블을 점검하여 결과 리스트 반환."""
    tables = fetch_base_tables(cur, schema)
    if args.table:
        tables = [t for t in tables if t["name"] == args.table]
    sizes = fetch_sizes(cur, schema)
    for t in tables:
        t["size"] = sizes.get(t["oid"], 0)
    # 용량 큰 순 정렬 후 limit 적용(스캔 비용 제한)
    tables.sort(key=lambda t: t["size"], reverse=True)
    if args.limit is not None:
        tables = tables[: args.limit]

    results = []
    for t in tables:
        dist = t.get("distributedby")
        dist_short = (dist or "?").replace("DISTRIBUTED ", "").strip()
        base = {
            "schema": schema, "name": t["name"], "oid": t["oid"],
            "size": t["size"], "distributedby": dist, "dist_short": dist_short,
            "metrics": {}, "verdict": "SKIP",
        }
        if t["size"] < min_bytes:
            results.append(base)  # 스캔 생략
            continue
        counts = fetch_segment_rowcounts(cur, schema, t["name"])
        m = compute_skew(counts, nseg)
        base["metrics"] = m
        base["verdict"] = verdict_of(m, args.warn, args.crit)
        results.append(base)
    return results


def main():
    args = parse_args()
    if args.table and not args.schema:
        sys.stderr.write("--table 은 --schema 와 함께 지정해야 합니다.\n")
        sys.exit(2)

    conn = get_connection(args)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    nseg = segment_count(cur)
    if not nseg:
        sys.stderr.write("프라이머리 세그먼트를 찾지 못했습니다. Greenplum 이 맞는지 확인하세요.\n")
        sys.exit(1)

    schemas = [args.schema] if args.schema else list_user_schemas(cur)
    min_bytes = int(args.min_size_mb * 1024 * 1024)

    results = []
    for schema in schemas:
        results.extend(collect(cur, schema, args, nseg, min_bytes))

    # 스큐 심한 순(비스캔/빈 테이블은 뒤로)
    order = {"CRIT": 0, "WARN": 1, "OK": 2, "EMPTY": 3, "SKIP": 4}
    results.sort(key=lambda r: (order.get(r["verdict"], 9),
                                -(r["metrics"].get("ratio", 0) or 0)))

    if args.no_color:
        use_color = False
    elif args.color:
        use_color = True
    else:
        use_color = sys.stdout.isatty()

    print(render(results, nseg, args.warn, args.crit, args.only_skewed, use_color))
    print()
    print(CHECKLIST)

    if args.csv:
        write_csv(args.csv, results, nseg)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
