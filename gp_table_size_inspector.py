#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gp_table_size_inspector.py  (Greenplum 6.x 전용)

스키마(또는 DB 전체)의 테이블 **디스크 용량**을 조사하여 큰 순으로 보여준다.
각 베이스 테이블(파티션 루트 포함, 자식 파티션 제외)에 대해 파티션 자식까지
합산한 용량을 테이블/인덱스/TOAST 로 분해하고, 막대 바로 시각화한다.

Greenplum 에서 pg_*_relation_size 계열 함수는 전 세그먼트 용량을 합산한다.

필요 패키지:
  pip install psycopg2-binary

사용 예:
  PGPASSWORD=secret python3 gp_table_size_inspector.py \
      --host 10.0.0.10 --dbname mydb --schema myschema

  # DB 전체, 상위 20개, 인덱스 크기 순
  python3 gp_table_size_inspector.py --dbname mydb --top 20 --sort index

  # 단일 테이블 + CSV
  python3 gp_table_size_inspector.py --schema myschema --table sales --csv out.csv
"""

import argparse
import csv
import os
import shutil
import sys
import unicodedata

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    sys.stderr.write(
        "psycopg2 가 필요합니다.  pip install psycopg2-binary 후 다시 실행하세요.\n"
    )
    sys.exit(1)

from gp_common import human_bytes, human_count, get_connection


# ---------------------------------------------------------------------------
# 표시 헬퍼
# ---------------------------------------------------------------------------
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
CYAN = "\033[36m"

_BLOCKS = " ▏▎▍▌▋▊▉█"


def _dw(s):
    """출력 폭(한글/전각 2칸)."""
    return sum(2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
               for ch in str(s))


def _pad(s, width, align="left"):
    s = str(s)
    gap = max(0, width - _dw(s))
    return (" " * gap + s) if align == "right" else (s + " " * gap)


def _bar(fraction, width):
    """0.0~1.0 비율을 width 폭의 유니코드 막대로."""
    if width <= 0:
        return ""
    fraction = max(0.0, min(1.0, fraction))
    eighths = int(round(fraction * width * 8))
    full, rem = divmod(eighths, 8)
    s = "█" * full
    if rem:
        s += _BLOCKS[rem]
    return s + " " * max(0, width - full - (1 if rem else 0))


# ---------------------------------------------------------------------------
# 카탈로그 수집
# ---------------------------------------------------------------------------
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


def fetch_table_sizes(cur, schema):
    """스키마의 베이스 테이블별 용량 집계(파티션 자식 포함).

    반환: list[dict(name, parts, total, table, index, toast, rows)]
    """
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
        SELECT cr.relname                                  AS name,
               count(*) - 1                                AS parts,
               sum(pg_total_relation_size(t.oid))          AS total_bytes,
               sum(pg_table_size(t.oid))                   AS table_bytes,
               sum(pg_indexes_size(t.oid))                 AS index_bytes,
               sum(pg_relation_size(t.oid))                AS heap_bytes,
               sum(GREATEST(c.reltuples, 0))               AS est_rows
        FROM   tree t
        JOIN   pg_catalog.pg_class c  ON c.oid  = t.oid
        JOIN   pg_catalog.pg_class cr ON cr.oid = t.root
        GROUP  BY cr.relname
        ORDER  BY total_bytes DESC;
        """,
        (schema,),
    )
    rows = []
    for r in cur.fetchall():
        total = r["total_bytes"] or 0
        table = r["table_bytes"] or 0
        index = r["index_bytes"] or 0
        heap = r["heap_bytes"] or 0
        rows.append({
            "schema": schema,
            "name": r["name"],
            "parts": r["parts"] or 0,
            "total": total,
            "table": table,
            "index": index,
            "toast": max(0, table - heap),   # TOAST(+FSM/VM) ≈ table - heap
            "rows": int(r["est_rows"] or 0),
        })
    return rows


# ---------------------------------------------------------------------------
# 렌더링
# ---------------------------------------------------------------------------
SORT_KEYS = {
    "total": "total", "table": "table", "index": "index",
    "toast": "toast", "rows": "rows", "name": "name",
}


def render(results, grand_total, schemas, args, use_color):
    c = (lambda code, s: f"{code}{s}{RESET}") if use_color else (lambda code, s: s)
    out = []

    scope = (f"{args.schema}" if args.schema else f"{len(schemas)}개 스키마")
    out.append(c(BOLD, f"테이블 용량 조사  ({args.dbname} / {scope})"))
    out.append(f"총 용량: {human_bytes(grand_total)}   테이블: {len(results)}개"
               f"   정렬: {args.sort}")
    out.append("=" * 78)

    if not results:
        out.append("표시할 테이블이 없습니다. (스키마/필터 확인)")
        return "\n".join(out)

    # 헤더/정렬 컬럼
    headers = ["#", "TABLE", "PARTS", "ROWS≈", "TOTAL", "TABLE", "INDEX", "TOAST", "%"]
    grid = []
    max_total = max((r["total"] for r in results), default=0) or 1  # 막대 스케일 기준
    for i, r in enumerate(results, 1):
        pct = (r["total"] / grand_total * 100.0) if grand_total else 0.0
        grid.append([
            str(i),
            f"{r['schema']}.{r['name']}",
            str(r["parts"]) if r["parts"] else "-",
            human_count(r["rows"]),
            human_bytes(r["total"]),
            human_bytes(r["table"]),
            human_bytes(r["index"]),
            human_bytes(r["toast"]),
            f"{pct:.1f}",
        ])

    widths = [max(_dw(h), max((_dw(row[j]) for row in grid), default=0))
              for j, h in enumerate(headers)]
    aligns = ["right", "left", "right", "right", "right", "right",
              "right", "right", "right"]

    def fmt(cells):
        return "  ".join(_pad(cell, widths[j], aligns[j]) for j, cell in enumerate(cells))

    base_w = _dw(fmt(headers))
    # 인라인 막대 폭 계산
    term_w = shutil.get_terminal_size((100, 20)).columns
    bar_w = 0
    if not args.no_bar:
        bar_w = max(0, min(30, term_w - base_w - 3))

    def with_bar(line, r):
        if bar_w <= 0:
            return line
        bar = _bar(r["total"] / max_total, bar_w)
        return f"{line}  {c(CYAN, bar)}"

    out.append(c(BOLD, fmt(headers)))
    out.append("-" * base_w)
    for row, r in zip(grid, results):
        out.append(with_bar(fmt(row), r))

    # 합계
    tot = {k: sum(r[k] for r in results) for k in ("total", "table", "index", "toast", "rows")}
    out.append("-" * base_w)
    out.append(c(BOLD, fmt(["", "합계", "",
                            human_count(tot["rows"]), human_bytes(tot["total"]),
                            human_bytes(tot["table"]), human_bytes(tot["index"]),
                            human_bytes(tot["toast"]), "100.0"])))

    # 스키마별 소계(여러 스키마일 때)
    if not args.schema and len(schemas) > 1:
        out.append("")
        out.append(c(BOLD, "스키마별 소계:"))
        sub = {}
        for r in results:
            sub[r["schema"]] = sub.get(r["schema"], 0) + r["total"]
        sw = max((_dw(s) for s in sub), default=6)
        for s, b in sorted(sub.items(), key=lambda kv: kv[1], reverse=True):
            pct = (b / grand_total * 100.0) if grand_total else 0.0
            out.append(f"  {_pad(s, sw)}  {_pad(human_bytes(b), 10, 'right')}  ({pct:.1f}%)")

    return "\n".join(out)


def write_csv(path, results):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["schema", "table", "partitions", "est_rows",
                    "total_bytes", "table_bytes", "index_bytes", "toast_bytes"])
        for r in results:
            w.writerow([r["schema"], r["name"], r["parts"], r["rows"],
                        r["total"], r["table"], r["index"], r["toast"]])
    print(f"CSV 저장 완료: {path}")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def parse_args():
    ap = argparse.ArgumentParser(
        description="Greenplum 6.x 테이블 디스크 용량 조사(큰 순 정렬 + 분해)"
    )
    ap.add_argument("--host", default=os.environ.get("PGHOST", "localhost"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", 5432)))
    ap.add_argument("--dbname", default=os.environ.get("PGDATABASE", "postgres"))
    ap.add_argument("--user", default=os.environ.get("PGUSER", "gpadmin"))
    ap.add_argument("--password", default=None, help="미지정 시 PGPASSWORD 또는 프롬프트")
    ap.add_argument("--schema", default=None,
                    help="대상 스키마. 미지정 시 DB 의 모든 사용자 스키마")
    ap.add_argument("--table", default=None, help="단일 테이블만 조사(--schema 필요)")
    ap.add_argument("--sort", choices=sorted(SORT_KEYS), default="total",
                    help="정렬 기준 (기본 total)")
    ap.add_argument("--top", type=int, default=None, help="상위 N개만 표시")
    ap.add_argument("--min-size-mb", type=float, default=0.0,
                    help="총 용량이 이 값 미만인 테이블 제외(기본 0)")
    ap.add_argument("--no-bar", action="store_true", help="인라인 막대 바 숨김")
    ap.add_argument("--csv", default=None, help="결과를 CSV 로 저장")
    ap.add_argument("--no-color", action="store_true", help="ANSI 색상 비활성화")
    ap.add_argument("--color", action="store_true", help="비-TTY 에서도 색상 강제")
    ap.add_argument("--timeout", type=int, default=15)
    return ap.parse_args()


def main():
    args = parse_args()
    if args.table and not args.schema:
        sys.stderr.write("--table 은 --schema 와 함께 지정해야 합니다.\n")
        sys.exit(2)

    conn = get_connection(args)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    schemas = [args.schema] if args.schema else list_user_schemas(cur)

    results = []
    for schema in schemas:
        results.extend(fetch_table_sizes(cur, schema))

    if args.table:
        results = [r for r in results if r["name"] == args.table]

    min_bytes = int(args.min_size_mb * 1024 * 1024)
    if min_bytes > 0:
        results = [r for r in results if r["total"] >= min_bytes]

    grand_total = sum(r["total"] for r in results)

    # 정렬(이름은 오름차순, 나머지는 내림차순)
    key = SORT_KEYS[args.sort]
    reverse = key != "name"
    results.sort(key=lambda r: r[key], reverse=reverse)

    if args.top is not None:
        results = results[: args.top]

    if args.no_color:
        use_color = False
    elif args.color:
        use_color = True
    else:
        use_color = sys.stdout.isatty()

    print(render(results, grand_total, schemas, args, use_color))

    if args.csv:
        write_csv(args.csv, results)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
