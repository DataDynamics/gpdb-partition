#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gp_partition_inspector.py  (Greenplum 6.x 전용)

스키마를 입력받아, 해당 스키마의 모든 테이블에 대해
파티션 컬럼(파티션 키)과 파티션 값(range 경계 / list 값)을 조사한다.
--schema 를 생략하면 DB 의 모든 사용자 스키마를 조사한다.

GPDB 6.x 의 시스템 카탈로그 뷰를 사용:
  - pg_partition_columns : 파티션 키 컬럼
  - pg_partitions        : 각 파티션의 타입/경계/값

필요 패키지:
  pip install psycopg2-binary

사용 예:
  PGPASSWORD=secret python3 gp_partition_inspector.py \
      --host 10.0.0.10 --port 5432 --dbname mydb --user gpadmin \
      --schema myschema

  # CSV 로도 저장
  python3 gp_partition_inspector.py --schema myschema --csv out.csv

  # 파티션 테이블만 출력
  python3 gp_partition_inspector.py --schema myschema --only-partitioned
"""

import argparse
import csv
import os
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

from gp_common import get_connection, human_bytes


def get_server_version(cur):
    cur.execute("SELECT version();")
    # 커서가 RealDictCursor 이므로 fetchone() 은 컬럼명을 키로 갖는 dict 를 반환한다.
    # version() 의 결과 컬럼명은 'version' 이다.
    return cur.fetchone()["version"]


# ---------------------------------------------------------------------------
# 사용자 스키마 목록(시스템/임시 스키마 제외)
# ---------------------------------------------------------------------------
def list_schemas(cur):
    """--schema 미지정 시 조사 대상이 되는 사용자 스키마 이름 목록을 반환."""
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


# ---------------------------------------------------------------------------
# 스키마 내 전체 베이스 테이블(파티션 루트 포함, 자식 파티션 제외)
# ---------------------------------------------------------------------------
def list_all_base_tables(cur, schema):
    # 'r'(일반 테이블)만. GPDB6 의 파티션 루트도 relkind='r' 이며,
    # 자식 파티션은 pg_partitions 의 partitiontablename 으로 식별되므로
    # 여기서는 pg_inherits 로 부모가 있는(=자식) 테이블을 제외한다.
    cur.execute(
        """
        SELECT c.relname
        FROM   pg_catalog.pg_class c
        JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE  n.nspname = %s
          AND  c.relkind = 'r'
          AND  NOT EXISTS (
                 SELECT 1 FROM pg_catalog.pg_inherits i
                 WHERE  i.inhrelid = c.oid
               )
        ORDER  BY c.relname;
        """,
        (schema,),
    )
    return [r["relname"] for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# 파티션 키 컬럼
# ---------------------------------------------------------------------------
def fetch_partition_columns(cur, schema):
    """반환: dict[tablename][level] = [colname, ...] (키 순서 유지)"""
    cur.execute(
        """
        SELECT tablename,
               partitionlevel,
               columnname,
               position_in_partition_key
        FROM   pg_partition_columns
        WHERE  schemaname = %s
        ORDER  BY tablename, partitionlevel, position_in_partition_key;
        """,
        (schema,),
    )
    cols = {}
    for row in cur.fetchall():
        cols.setdefault(row["tablename"], {}) \
            .setdefault(row["partitionlevel"], []) \
            .append(row["columnname"])
    return cols


# ---------------------------------------------------------------------------
# 파티션(자식) 및 경계/값
# ---------------------------------------------------------------------------
def fetch_partitions(cur, schema, columns):
    """반환: dict[tablename] = {type, columns, partitions[]}"""
    cur.execute(
        """
        SELECT tablename,
               partitionlevel,
               partitiontype,
               partitionname,
               partitiontablename,
               partitionrank,
               partitionposition,
               partitionlistvalues,
               partitionrangestart,
               partitionstartinclusive,
               partitionrangeend,
               partitionendinclusive,
               partitioneveryclause,
               partitionisdefault,
               partitionboundary
        FROM   pg_partitions
        WHERE  schemaname = %s
        ORDER  BY tablename, partitionlevel, partitionrank, partitionposition,
                  partitionname;
        """,
        (schema,),
    )

    tables = {}
    for r in cur.fetchall():
        t = r["tablename"]
        entry = tables.setdefault(
            t,
            {
                "type": (r["partitiontype"] or "").upper(),
                "columns": columns.get(t, {}),
                "partitions": [],
            },
        )
        entry["partitions"].append(
            {
                "level": r["partitionlevel"],
                "name": r["partitionname"] or r["partitiontablename"],
                "child_table": r["partitiontablename"],
                "type": (r["partitiontype"] or "").upper(),
                "values": r["partitionlistvalues"],
                "range_start": r["partitionrangestart"],
                "start_inclusive": r["partitionstartinclusive"],
                "range_end": r["partitionrangeend"],
                "end_inclusive": r["partitionendinclusive"],
                "every": r["partitioneveryclause"],
                "is_default": r["partitionisdefault"],
                "boundary": r["partitionboundary"],
            }
        )
    return tables


# ---------------------------------------------------------------------------
# 파티션(자식 테이블)별 디스크 용량
# ---------------------------------------------------------------------------
def fetch_partition_sizes(cur, schema):
    """반환: dict[child_table] = size_bytes (전 세그먼트 합산, 인덱스/TOAST 포함).

    Greenplum 에서 pg_total_relation_size() 는 마스터가 각 세그먼트로 디스패치하여
    클러스터 전체 용량을 합산해 돌려준다. 따라서 각 파티션의 실제 자식 테이블
    (partitiontablename)에 대해 호출하면 파티션별 실제 차지 용량을 얻는다.
    """
    cur.execute(
        """
        SELECT partitiontablename AS child_table,
               pg_total_relation_size(
                   quote_ident(schemaname) || '.' || quote_ident(partitiontablename)
               ) AS size_bytes
        FROM   pg_partitions
        WHERE  schemaname = %s;
        """,
        (schema,),
    )
    sizes = {}
    for r in cur.fetchall():
        sizes[r["child_table"]] = r["size_bytes"]
    return sizes


def attach_sizes(partitioned, sizes):
    """각 파티션에 size_bytes 를 채우고, 테이블별 총용량(total_size)을 계산한다."""
    for info in partitioned.values():
        total = 0
        for p in info["partitions"]:
            sz = sizes.get(p.get("child_table"))
            p["size_bytes"] = sz
            if sz is not None:
                total += sz
        info["total_size"] = total


# ---------------------------------------------------------------------------
# 파티션 1건 → 사람이 읽는 경계 문자열
# ---------------------------------------------------------------------------
def describe_partition(p, raw=False):
    # raw=True 면 DBeaver 처럼 카탈로그의 partitionboundary 원문을 그대로 반환
    if raw:
        if p.get("boundary"):
            return p["boundary"]
        # GPDB 버전에 따라 boundary 가 비어있을 수 있어 아래 일반 포맷으로 폴백

    if p.get("is_default"):
        return "DEFAULT"

    ptype = (p.get("type") or "").upper()
    if ptype == "RANGE":
        lb = "[" if p.get("start_inclusive") else "("
        rb = "]" if p.get("end_inclusive") else ")"
        start = p.get("range_start")
        end = p.get("range_end")
        s = f"RANGE  {lb}{start} ~ {end}{rb}"
        if p.get("every"):
            s += f"  EVERY({p['every']})"
        return s
    if ptype == "LIST":
        return f"LIST   values=({p.get('values')})"

    # 타입을 못 읽으면 boundary 원문이라도 보여준다.
    return p.get("boundary") or ""


# ---------------------------------------------------------------------------
# 출력
# ---------------------------------------------------------------------------
def print_report(schema, all_tables, partitioned, raw=False, with_size=False):
    line = "=" * 72
    print(line)
    print(f"스키마: {schema}")
    print(line)

    for t in all_tables:
        if t in partitioned:
            info = partitioned[t]
            ncols = info.get("columns", {})
            key_parts = []
            for lvl in sorted(ncols.keys()):
                key_parts.append(f"L{lvl}: {', '.join(ncols[lvl])}")
            keystr = " | ".join(key_parts) if key_parts else "(키 정보 없음)"

            parts = info["partitions"]
            header = f"\n[{t}]  ▶ 파티션됨  타입={info['type'] or '?'}  파티션수={len(parts)}"
            if with_size:
                header += f"  총용량={human_bytes(info.get('total_size'))}"
            print(header)
            print(f"    파티션 키 : {keystr}")
            print(f"    파티션 목록:")
            for p in parts:
                indent = "      " + ("  " * p.get("level", 0))
                boundary = describe_partition(p, raw)
                if with_size:
                    size_str = human_bytes(p.get("size_bytes"))
                    print(f"{indent}- {p['name']:40s} {boundary:40s} {size_str:>10}")
                else:
                    print(f"{indent}- {p['name']:40s} {boundary}")
        else:
            print(f"\n[{t}]  ▶ 파티션 아님")
    print()


def write_csv(path, schema_results, raw=False, with_size=False):
    """schema_results: list[(schema, partitioned)]. 여러 스키마를 한 CSV 로 저장."""
    header = [
        "schema", "table", "partition_type", "partition_key",
        "level", "partition_name", "child_table", "partition_value",
    ]
    if with_size:
        header += ["size_bytes", "size_pretty"]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(header)
        for schema, partitioned in schema_results:
            for t, info in partitioned.items():
                ncols = info.get("columns", {})
                keystr = "; ".join(
                    f"L{lvl}:" + ",".join(ncols[lvl]) for lvl in sorted(ncols.keys())
                )
                for p in info["partitions"]:
                    row = [
                        schema, t, info["type"], keystr,
                        p.get("level", ""), p["name"],
                        p.get("child_table", ""), describe_partition(p, raw),
                    ]
                    if with_size:
                        sz = p.get("size_bytes")
                        row += ["" if sz is None else sz, human_bytes(sz)]
                    w.writerow(row)
    print(f"CSV 저장 완료: {path}")


# ---------------------------------------------------------------------------
# 행(row) 평탄화 — CSV / 테이블 출력 공용
# ---------------------------------------------------------------------------
HEADERS = [
    "table", "partitioned", "type", "partition_key",
    "level", "partition_name", "partition_value",
]
SIZE_HEADERS = ["size", "size_bytes"]


def build_headers(with_size=False):
    return HEADERS + SIZE_HEADERS if with_size else list(HEADERS)


def build_rows(all_tables, partitioned, only_partitioned=False, raw=False, with_size=False):
    rows = []
    for t in all_tables:
        if t in partitioned:
            info = partitioned[t]
            ncols = info.get("columns", {})
            keystr = "; ".join(
                f"L{lvl}:" + ",".join(ncols[lvl]) for lvl in sorted(ncols.keys())
            )
            for p in info["partitions"]:
                row = [
                    t, "Y", info["type"], keystr,
                    str(p.get("level", "")), p["name"], describe_partition(p, raw),
                ]
                if with_size:
                    sz = p.get("size_bytes")
                    row += [human_bytes(sz), "" if sz is None else str(sz)]
                rows.append(row)
        elif not only_partitioned:
            row = [t, "N", "", "", "", "", ""]
            if with_size:
                row += ["", ""]
            rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# 격자(grid) 테이블 출력 — 한글 폭(East Asian Width) 고려
# ---------------------------------------------------------------------------
def _disp_width(s):
    """문자열의 출력 폭. 한글/전각 문자는 2칸으로 계산."""
    w = 0
    for ch in str(s):
        w += 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
    return w


def _pad(s, width):
    s = str(s)
    return s + " " * max(0, width - _disp_width(s))


def render_table(headers, rows):
    cols = list(zip(*([headers] + rows))) if rows else [[h] for h in headers]
    widths = [max(_disp_width(c) for c in col) for col in cols]

    def fmt_row(cells):
        return "| " + " | ".join(_pad(c, widths[i]) for i, c in enumerate(cells)) + " |"

    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"

    out = [sep, fmt_row(headers), sep]
    out.extend(fmt_row(r) for r in rows)
    out.append(sep)
    return "\n".join(out)


def print_table(schema, all_tables, partitioned, only_partitioned=False, raw=False,
                with_size=False):
    print("=" * 72)
    print(f"스키마: {schema}")
    print("=" * 72)
    rows = build_rows(all_tables, partitioned, only_partitioned, raw, with_size)
    if not rows:
        print("(표시할 테이블 없음)")
        return
    print(render_table(build_headers(with_size), rows))


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def parse_args():
    ap = argparse.ArgumentParser(
        description="Greenplum 6.x 스키마 파티션(컬럼/값) 조사 스크립트"
    )
    ap.add_argument("--host", default=os.environ.get("PGHOST", "localhost"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", 5432)))
    ap.add_argument("--dbname", default=os.environ.get("PGDATABASE", "postgres"))
    ap.add_argument("--user", default=os.environ.get("PGUSER", "gpadmin"))
    ap.add_argument("--password", default=None, help="미지정 시 PGPASSWORD 또는 프롬프트")
    ap.add_argument(
        "--schema",
        default=None,
        help="조사할 스키마명. 미지정 시 DB 의 모든 사용자 스키마를 조사",
    )
    ap.add_argument("--csv", default=None, help="결과를 CSV 파일로 저장")
    ap.add_argument(
        "--table",
        action="store_true",
        help="결과를 격자(grid) 테이블 형태로 출력",
    )
    ap.add_argument(
        "--raw-boundary",
        action="store_true",
        help="파티션 값을 DBeaver 처럼 카탈로그 원문(partitionboundary) 그대로 출력",
    )
    ap.add_argument("--timeout", type=int, default=15)
    ap.add_argument(
        "--only-partitioned",
        action="store_true",
        help="파티션 테이블만 출력(비파티션 테이블 생략)",
    )
    ap.add_argument(
        "--size",
        action="store_true",
        help="파티션별 디스크 용량(인덱스/TOAST 포함, 전 세그먼트 합산)을 함께 표시",
    )
    return ap.parse_args()


def process_schema(cur, schema, args):
    """한 스키마를 조사하여 화면 출력하고 partitioned dict 를 반환한다."""
    columns = fetch_partition_columns(cur, schema)
    partitioned = fetch_partitions(cur, schema, columns)
    all_tables = list_all_base_tables(cur, schema)

    if args.size:
        sizes = fetch_partition_sizes(cur, schema)
        attach_sizes(partitioned, sizes)

    if not all_tables:
        print(f"\n경고: 스키마 '{schema}' 에서 테이블을 찾지 못했습니다. "
              f"(스키마명/권한 확인)")

    display_tables = all_tables
    if args.only_partitioned:
        display_tables = [t for t in all_tables if t in partitioned]

    raw = args.raw_boundary
    if args.table:
        print_table(schema, display_tables, partitioned, args.only_partitioned, raw,
                    args.size)
    else:
        print_report(schema, display_tables, partitioned, raw, args.size)

    return partitioned


def main():
    args = parse_args()
    conn = get_connection(args)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print(f"서버: {get_server_version(cur)}")

    if args.schema:
        schemas = [args.schema]
    else:
        schemas = list_schemas(cur)
        if not schemas:
            print("경고: 조사할 사용자 스키마가 없습니다.")
        else:
            print(f"대상 스키마({len(schemas)}개): {', '.join(schemas)}")

    schema_results = []
    for schema in schemas:
        partitioned = process_schema(cur, schema, args)
        schema_results.append((schema, partitioned))

    if args.csv:
        write_csv(args.csv, schema_results, args.raw_boundary, args.size)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
