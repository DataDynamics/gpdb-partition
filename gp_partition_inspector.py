#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gp_partition_inspector.py  (Greenplum 6.x 전용)

특정 스키마를 입력받아, 해당 스키마의 모든 테이블에 대해
파티션 컬럼(파티션 키)과 파티션 값(range 경계 / list 값)을 조사한다.

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
import getpass
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


# ---------------------------------------------------------------------------
# 연결
# ---------------------------------------------------------------------------
def get_connection(args):
    password = args.password or os.environ.get("PGPASSWORD")
    if not password:
        password = getpass.getpass(f"Password for {args.user}@{args.host}: ")

    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=password,
        connect_timeout=args.timeout,
    )
    conn.set_session(readonly=True, autocommit=True)
    return conn


def get_server_version(cur):
    cur.execute("SELECT version();")
    # 커서가 RealDictCursor 이므로 fetchone() 은 컬럼명을 키로 갖는 dict 를 반환한다.
    # version() 의 결과 컬럼명은 'version' 이다.
    return cur.fetchone()["version"]


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
def print_report(schema, all_tables, partitioned, raw=False):
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
            print(f"\n[{t}]  ▶ 파티션됨  타입={info['type'] or '?'}  파티션수={len(parts)}")
            print(f"    파티션 키 : {keystr}")
            print(f"    파티션 목록:")
            for p in parts:
                indent = "      " + ("  " * p.get("level", 0))
                print(f"{indent}- {p['name']:40s} {describe_partition(p, raw)}")
        else:
            print(f"\n[{t}]  ▶ 파티션 아님")
    print()


def write_csv(path, schema, partitioned, raw=False):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "schema", "table", "partition_type", "partition_key",
                "level", "partition_name", "child_table", "partition_value",
            ]
        )
        for t, info in partitioned.items():
            ncols = info.get("columns", {})
            keystr = "; ".join(
                f"L{lvl}:" + ",".join(ncols[lvl]) for lvl in sorted(ncols.keys())
            )
            for p in info["partitions"]:
                w.writerow(
                    [
                        schema, t, info["type"], keystr,
                        p.get("level", ""), p["name"],
                        p.get("child_table", ""), describe_partition(p, raw),
                    ]
                )
    print(f"CSV 저장 완료: {path}")


# ---------------------------------------------------------------------------
# 행(row) 평탄화 — CSV / 테이블 출력 공용
# ---------------------------------------------------------------------------
HEADERS = [
    "table", "partitioned", "type", "partition_key",
    "level", "partition_name", "partition_value",
]


def build_rows(all_tables, partitioned, only_partitioned=False, raw=False):
    rows = []
    for t in all_tables:
        if t in partitioned:
            info = partitioned[t]
            ncols = info.get("columns", {})
            keystr = "; ".join(
                f"L{lvl}:" + ",".join(ncols[lvl]) for lvl in sorted(ncols.keys())
            )
            for p in info["partitions"]:
                rows.append([
                    t, "Y", info["type"], keystr,
                    str(p.get("level", "")), p["name"], describe_partition(p, raw),
                ])
        elif not only_partitioned:
            rows.append([t, "N", "", "", "", "", ""])
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


def print_table(schema, all_tables, partitioned, only_partitioned=False, raw=False):
    print("=" * 72)
    print(f"스키마: {schema}")
    print("=" * 72)
    rows = build_rows(all_tables, partitioned, only_partitioned, raw)
    if not rows:
        print("(표시할 테이블 없음)")
        return
    print(render_table(HEADERS, rows))


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
    ap.add_argument("--schema", required=True, help="조사할 스키마명")
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
    return ap.parse_args()


def main():
    args = parse_args()
    conn = get_connection(args)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print(f"서버: {get_server_version(cur)}")

    columns = fetch_partition_columns(cur, args.schema)
    partitioned = fetch_partitions(cur, args.schema, columns)
    all_tables = list_all_base_tables(cur, args.schema)

    if not all_tables:
        print(f"\n경고: 스키마 '{args.schema}' 에서 테이블을 찾지 못했습니다. "
              f"(스키마명/권한 확인)")

    if args.only_partitioned:
        all_tables = [t for t in all_tables if t in partitioned]

    raw = args.raw_boundary
    if args.table:
        print_table(args.schema, all_tables, partitioned, args.only_partitioned, raw)
    else:
        print_report(args.schema, all_tables, partitioned, raw)

    if args.csv:
        write_csv(args.csv, args.schema, partitioned, raw)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
