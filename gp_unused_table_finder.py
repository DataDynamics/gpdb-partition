#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gp_unused_table_finder.py  (Greenplum 6.x 전용)

"안 쓰는 테이블" 후보를 찾는다.  Greenplum/PostgreSQL 에는 테이블의
'마지막 SELECT 시각'이 없으므로, 다음 신호를 종합한다:

  1. 스캔/DML 카운터   pg_stat_all_tables (기본: 세그먼트 합산 gp_dist_random)
                        seq_scan+idx_scan=0 → 통계 리셋 이후 미읽음
  2. 유지보수 시각      last_vacuum / last_analyze
  3. 마지막 DDL 시각    pg_stat_operations (GPDB 전용)
  4. 용량               정리 우선순위를 위해 함께 표시

카운터 절대값은 pg_stat_reset() 이후 값이므로, 진짜 미사용 판별에는
스냅샷 델타를 권장한다:
  # 지금 카운터 저장
  python3 gp_unused_table_finder.py --dbname mydb --save-snapshot snap.json
  # 며칠 뒤, 그 사이 안 읽힌 테이블 판별
  python3 gp_unused_table_finder.py --dbname mydb --compare-snapshot snap.json

필요 패키지:
  pip install psycopg2-binary
"""

import argparse
import csv
import datetime
import json
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

from gp_common import human_bytes, human_count, get_connection


# ---------------------------------------------------------------------------
# 표시 헬퍼
# ---------------------------------------------------------------------------
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[31m"
YELLOW = "\033[33m"
GREEN = "\033[32m"
CYAN = "\033[36m"

STATUS_COLOR = {"UNUSED": RED, "WRITE-ONLY": YELLOW, "used": GREEN,
                "NEW": CYAN, "SKIP": DIM}
STATUS_RANK = {"UNUSED": 0, "WRITE-ONLY": 1, "NEW": 2, "used": 3, "SKIP": 4}


def _dw(s):
    return sum(2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
               for ch in str(s))


def _pad(s, width, align="left"):
    s = str(s)
    gap = max(0, width - _dw(s))
    return (" " * gap + s) if align == "right" else (s + " " * gap)


def _d(ts):
    """timestamp → 'YYYY-MM-DD' (None → '-')."""
    if not ts:
        return "-"
    return ts.strftime("%Y-%m-%d")


def _max_ts(*vals):
    ts = [v for v in vals if v]
    return max(ts) if ts else None


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


def fetch_table_tree(cur, schema):
    """베이스 테이블별 (자식 파티션 포함) oid 목록과 총 용량.

    반환: dict[root_oid] = {schema, name, size, oids:set}
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
        SELECT t.root, t.oid, cr.relname AS name,
               pg_total_relation_size(t.oid) AS sz
        FROM   tree t JOIN pg_catalog.pg_class cr ON cr.oid = t.root;
        """,
        (schema,),
    )
    roots = {}
    for r in cur.fetchall():
        e = roots.setdefault(r["root"], {"schema": schema, "name": r["name"],
                                         "size": 0, "oids": set()})
        e["oids"].add(r["oid"])
        e["size"] += (r["sz"] or 0)
    return roots


def fetch_scan_stats(cur, per_segment=True):
    """oid 별 스캔/DML 카운터. 반환: (dict[oid]=counts, source_label).

    per_segment=True 면 gp_dist_random 로 세그먼트 통계를 합산한다(정확).
    실패 시 마스터 pg_stat_all_tables 로 폴백한다.
    """
    agg = (
        "SELECT relid, "
        "sum(coalesce(seq_scan,0)) AS seq_scan, "
        "sum(coalesce(idx_scan,0)) AS idx_scan, "
        "sum(coalesce(n_tup_ins,0)) AS ins, "
        "sum(coalesce(n_tup_upd,0)) AS upd, "
        "sum(coalesce(n_tup_del,0)) AS del "
    )
    if per_segment:
        try:
            cur.execute(agg + "FROM gp_dist_random('pg_stat_all_tables') GROUP BY relid;")
            return {r["relid"]: r for r in cur.fetchall()}, "per-segment"
        except psycopg2.Error:
            cur.connection.rollback()  # 뷰에 gp_dist_random 미지원 등 → 폴백
    cur.execute(agg + "FROM pg_stat_all_tables GROUP BY relid;")
    return {r["relid"]: r for r in cur.fetchall()}, "master"


def fetch_maint_times(cur, schema):
    """oid 별 마지막 vacuum/analyze 시각(마스터 통계)."""
    cur.execute(
        """
        SELECT relid,
               GREATEST(last_vacuum, last_autovacuum)   AS last_vac,
               GREATEST(last_analyze, last_autoanalyze) AS last_ana
        FROM   pg_stat_all_tables
        WHERE  schemaname = %s;
        """,
        (schema,),
    )
    return {r["relid"]: r for r in cur.fetchall()}


def fetch_last_op(cur, schema):
    """oid 별 마지막 DDL 작업 시각(pg_stat_operations, GPDB 전용). 실패 시 {}."""
    try:
        cur.execute(
            "SELECT objid, max(statime) AS last_op "
            "FROM pg_stat_operations WHERE schemaname = %s GROUP BY objid;",
            (schema,),
        )
        return {r["objid"]: r["last_op"] for r in cur.fetchall()}
    except psycopg2.Error:
        cur.connection.rollback()
        return {}


# ---------------------------------------------------------------------------
# 집계 / 분류
# ---------------------------------------------------------------------------
def classify(scans, writes, has_prior=True):
    if not has_prior:
        return "NEW"
    if scans == 0 and writes == 0:
        return "UNUSED"
    if scans == 0:
        return "WRITE-ONLY"
    return "used"


def build_results(roots, stats, maint, lastop):
    """root 별로 트리 전체를 합산하여 지표 dict 리스트 반환."""
    results = []
    for root_oid, e in roots.items():
        seq = idx = ins = upd = dele = 0
        last_vac = last_ana = last_op = None
        for oid in e["oids"]:
            s = stats.get(oid)
            if s:
                # sum(bigint) 은 numeric → decimal.Decimal 로 오므로 int 로 캐스팅한다.
                # (Decimal 은 json.dump 불가 + float 혼합 연산 시 TypeError)
                seq += int(s["seq_scan"] or 0)
                idx += int(s["idx_scan"] or 0)
                ins += int(s["ins"] or 0)
                upd += int(s["upd"] or 0)
                dele += int(s["del"] or 0)
            mt = maint.get(oid)
            if mt:
                last_vac = _max_ts(last_vac, mt["last_vac"])
                last_ana = _max_ts(last_ana, mt["last_ana"])
            last_op = _max_ts(last_op, lastop.get(oid))
        scans = seq + idx
        writes = ins + upd + dele
        results.append({
            "schema": e["schema"], "name": e["name"], "size": e["size"],
            "scans": scans, "seq": seq, "idx": idx, "writes": writes,
            "last_vac": last_vac, "last_ana": last_ana, "last_op": last_op,
            "status": classify(scans, writes),
        })
    return results


# ---------------------------------------------------------------------------
# 스냅샷
# ---------------------------------------------------------------------------
def save_snapshot(path, dbname, source, results, now_iso):
    data = {
        "created_at": now_iso, "db": dbname, "stats_source": source,
        "tables": {f"{r['schema']}.{r['name']}":
                   {"scans": r["scans"], "writes": r["writes"]}
                   for r in results},
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"스냅샷 저장 완료: {path}  ({len(results)}개 테이블, {now_iso})")


def apply_compare(results, snapshot):
    """스냅샷과 비교하여 기간 델타 및 창(window) 기준 상태를 채운다."""
    prior = snapshot.get("tables", {})
    for r in results:
        key = f"{r['schema']}.{r['name']}"
        p = prior.get(key)
        if p is None:
            r["dscans"], r["dwrites"], r["status"] = None, None, "NEW"
        else:
            r["dscans"] = r["scans"] - p.get("scans", 0)
            r["dwrites"] = r["writes"] - p.get("writes", 0)
            r["status"] = classify(r["dscans"], r["dwrites"], has_prior=True)
    return results


# ---------------------------------------------------------------------------
# 렌더링
# ---------------------------------------------------------------------------
def render(results, dbname, scope, source, compare_snap, use_color):
    c = (lambda code, s: f"{code}{s}{RESET}") if use_color else (lambda code, s: s)
    out = []
    compare = compare_snap is not None

    out.append(c(BOLD, f"미사용 테이블 후보 조사  ({dbname} / {scope})"))
    line2 = f"통계 출처: {source}   테이블: {len(results)}개"
    if compare:
        line2 += f"   비교 스냅샷: {compare_snap.get('created_at', '?')}"
    out.append(line2)
    if not compare:
        out.append(c(DIM, "주의: 카운터는 pg_stat_reset() 이후 누적값입니다. "
                          "확실한 판별은 --save-snapshot 후 --compare-snapshot 을 쓰세요."))
    out.append("=" * 80)

    if not results:
        out.append("표시할 테이블이 없습니다. (스키마/필터 확인)")
        return "\n".join(out)

    if compare:
        headers = ["#", "TABLE", "SIZE", "SCANS", "ΔSCANS", "WRITES", "ΔWRITES", "STATUS"]
        aligns = ["right", "left", "right", "right", "right", "right", "right", "left"]
    else:
        headers = ["#", "TABLE", "SIZE", "SCANS", "WRITES", "ANALYZED", "LAST_OP", "STATUS"]
        aligns = ["right", "left", "right", "right", "right", "right", "right", "left"]

    grid = []
    for i, r in enumerate(results, 1):
        if compare:
            grid.append([
                str(i), f"{r['schema']}.{r['name']}", human_bytes(r["size"]),
                human_count(r["scans"]),
                ("-" if r.get("dscans") is None else human_count(r["dscans"])),
                human_count(r["writes"]),
                ("-" if r.get("dwrites") is None else human_count(r["dwrites"])),
                r["status"],
            ])
        else:
            grid.append([
                str(i), f"{r['schema']}.{r['name']}", human_bytes(r["size"]),
                human_count(r["scans"]), human_count(r["writes"]),
                _d(r["last_ana"]), _d(r["last_op"]), r["status"],
            ])

    widths = [max(_dw(h), max((_dw(row[j]) for row in grid), default=0))
              for j, h in enumerate(headers)]

    def fmt(cells):
        return "  ".join(_pad(cell, widths[j], aligns[j]) for j, cell in enumerate(cells))

    out.append(c(BOLD, fmt(headers)))
    out.append("-" * _dw(fmt(headers)))
    for row, r in zip(grid, results):
        col = STATUS_COLOR.get(r["status"])
        line = fmt(row)
        out.append(c(col, line) if col else line)

    # 요약: 미사용 후보 합계 용량
    cand = [r for r in results if r["status"] in ("UNUSED", "WRITE-ONLY")]
    cand_bytes = sum(r["size"] for r in cand)
    out.append("-" * _dw(fmt(headers)))
    out.append(c(BOLD, f"미사용/쓰기전용 후보: {len(cand)}개, "
                       f"합계 용량 {human_bytes(cand_bytes)}"))
    if cand:
        out.append(c(DIM, "→ 정리 전 확인: 참조(FK)·뷰·ETL/외부 연동·백업 여부를 반드시 점검하세요."))
    return "\n".join(out)


def write_csv(path, results, compare):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        cols = ["schema", "table", "size_bytes", "scans", "seq_scan", "idx_scan",
                "writes", "last_vacuum", "last_analyze", "last_op", "status"]
        if compare:
            cols += ["delta_scans", "delta_writes"]
        w.writerow(cols)
        for r in results:
            row = [r["schema"], r["name"], r["size"], r["scans"], r["seq"], r["idx"],
                   r["writes"], _d(r["last_vac"]), _d(r["last_ana"]),
                   _d(r["last_op"]), r["status"]]
            if compare:
                row += [r.get("dscans", ""), r.get("dwrites", "")]
            w.writerow(row)
    print(f"CSV 저장 완료: {path}")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def parse_args():
    ap = argparse.ArgumentParser(
        description="Greenplum 6.x 미사용 테이블 후보 조사(스캔 통계 + 스냅샷 델타)"
    )
    ap.add_argument("--host", default=os.environ.get("PGHOST", "localhost"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", 5432)))
    ap.add_argument("--dbname", default=os.environ.get("PGDATABASE", "postgres"))
    ap.add_argument("--user", default=os.environ.get("PGUSER", "gpadmin"))
    ap.add_argument("--password", default=None, help="미지정 시 PGPASSWORD 또는 프롬프트")
    ap.add_argument("--schema", default=None,
                    help="대상 스키마. 미지정 시 DB 의 모든 사용자 스키마")
    ap.add_argument("--table", default=None, help="단일 테이블만 조사(--schema 필요)")
    ap.add_argument("--master-stats", action="store_true",
                    help="세그먼트 합산 대신 마스터 통계만 사용")
    ap.add_argument("--min-size-mb", type=float, default=0.0,
                    help="총 용량이 이 값 미만인 테이블 제외(기본 0)")
    ap.add_argument("--only-unused", action="store_true",
                    help="UNUSED/WRITE-ONLY 후보만 표시")
    ap.add_argument("--top", type=int, default=None, help="상위 N개만 표시")
    ap.add_argument("--save-snapshot", default=None, help="현재 카운터를 JSON 으로 저장")
    ap.add_argument("--compare-snapshot", default=None,
                    help="이전 스냅샷과 비교(기간 델타로 미사용 판별)")
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
    stats, source = fetch_scan_stats(cur, per_segment=not args.master_stats)

    results = []
    for schema in schemas:
        roots = fetch_table_tree(cur, schema)
        maint = fetch_maint_times(cur, schema)
        lastop = fetch_last_op(cur, schema)
        results.extend(build_results(roots, stats, maint, lastop))

    if args.table:
        results = [r for r in results if r["name"] == args.table]

    # 스냅샷 저장은 필터/정렬 전 전체 대상으로
    if args.save_snapshot:
        now_iso = datetime.datetime.now().replace(microsecond=0).isoformat()
        save_snapshot(args.save_snapshot, args.dbname, source, results, now_iso)

    snapshot = None
    if args.compare_snapshot:
        with open(args.compare_snapshot, encoding="utf-8") as f:
            snapshot = json.load(f)
        apply_compare(results, snapshot)

    min_bytes = int(args.min_size_mb * 1024 * 1024)
    if min_bytes > 0:
        results = [r for r in results if r["size"] >= min_bytes]
    if args.only_unused:
        results = [r for r in results if r["status"] in ("UNUSED", "WRITE-ONLY")]

    # 상태(미사용 먼저) → 용량 큰 순
    results.sort(key=lambda r: (STATUS_RANK.get(r["status"], 9), -r["size"]))
    if args.top is not None:
        results = results[: args.top]

    if args.no_color:
        use_color = False
    elif args.color:
        use_color = True
    else:
        use_color = sys.stdout.isatty()

    scope = args.schema if args.schema else f"{len(schemas)}개 스키마"
    print(render(results, args.dbname, scope, source, snapshot, use_color))

    if args.csv:
        write_csv(args.csv, results, args.compare_snapshot is not None)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
