#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gp_partition_size_chart.py  (Greenplum 6.x 전용)

특정 테이블(db / schema / table)의 파티션별 디스크 용량을
터미널 막대(bar) 차트로 표시하고, 용량이 통계적으로 이상치(outlier)인
파티션을 강조 표시한다.

용량은 각 파티션의 실제 자식 테이블에 대해 pg_total_relation_size() 를
호출해 얻는다. Greenplum 에서 이 함수는 전 세그먼트에 걸친 클러스터 전체
용량을 합산하며 힙 + 인덱스 + TOAST 를 모두 포함한다.

필요 패키지:
  pip install psycopg2-binary

사용 예:
  PGPASSWORD=secret python3 gp_partition_size_chart.py \
      --host 10.0.0.10 --dbname mydb --schema myschema --table sales

  # MAD(수정 z-score) 방식으로 이상치 판정
  python3 gp_partition_size_chart.py --schema myschema --table sales --method mad

  # 색상 없이(파이프/로그 저장용)
  python3 gp_partition_size_chart.py --schema myschema --table sales --no-color
"""

import argparse
import os
import shutil
import statistics
import sys

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    sys.stderr.write(
        "psycopg2 가 필요합니다.  pip install psycopg2-binary 후 다시 실행하세요.\n"
    )
    sys.exit(1)

# 공용 헬퍼는 gp_common 모듈에서 단일 소스로 공유한다.
from gp_common import human_bytes, get_connection


# ---------------------------------------------------------------------------
# 파티션별 용량 수집
# ---------------------------------------------------------------------------
def fetch_partition_sizes(cur, schema, table, leaf_only=True):
    """반환: list[dict(name, child_table, level, boundary, size_bytes)] (용량 순서 아님).

    leaf_only=True 면 자식이 없는 리프 파티션만 조회한다. 다단계 파티션에서
    중간 레벨 부모 테이블은 자체 저장 데이터가 없어(≈0) 차트를 왜곡하므로
    기본적으로 제외한다.
    """
    leaf_clause = (
        "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i "
        "WHERE i.inhparent = c.oid)"
        if leaf_only
        else ""
    )
    # leaf_clause 는 사용자 입력이 아닌 고정 리터럴이므로 문자열 결합이 안전하다.
    cur.execute(
        """
        SELECT p.partitionname       AS name,
               p.partitiontablename  AS child_table,
               p.partitionlevel      AS level,
               p.partitionboundary   AS boundary,
               pg_total_relation_size(c.oid) AS size_bytes
        FROM   pg_partitions p
        JOIN   pg_catalog.pg_namespace n ON n.nspname = p.schemaname
        JOIN   pg_catalog.pg_class c
                 ON c.relname = p.partitiontablename
                AND c.relnamespace = n.oid
        WHERE  p.schemaname = %s AND p.tablename = %s
        """
        + leaf_clause
        + """
        ORDER  BY p.partitionlevel, p.partitionrank,
                  p.partitionposition, p.partitionname;
        """,
        (schema, table),
    )
    rows = []
    for r in cur.fetchall():
        rows.append(
            {
                "name": r["name"] or r["child_table"],
                "child_table": r["child_table"],
                "level": r["level"],
                "boundary": r["boundary"],
                "size_bytes": r["size_bytes"] or 0,
            }
        )
    return rows


# ---------------------------------------------------------------------------
# 이상치(outlier) 탐지
# ---------------------------------------------------------------------------
def detect_outliers(values, method="iqr", k=None):
    """values: 숫자 리스트. 반환: (flags, stats).

    flags[i] 는 'high' / 'low' / None.
    stats 는 요약 통계 dict (표시에 사용).
    """
    n = len(values)
    flags = [None] * n
    stats = {"n": n, "method": method}
    if n == 0:
        return flags, stats

    stats["min"] = min(values)
    stats["max"] = max(values)
    stats["total"] = sum(values)
    stats["mean"] = statistics.mean(values)
    stats["median"] = statistics.median(values)
    stats["stdev"] = statistics.pstdev(values) if n >= 2 else 0.0

    if n < 4:
        stats["note"] = "표본이 적어(<4) 이상치 판정을 생략했습니다."
        return flags, stats

    if method == "iqr":
        kk = 1.5 if k is None else k
        q1, _q2, q3 = statistics.quantiles(values, n=4, method="inclusive")
        iqr = q3 - q1
        low_fence = q1 - kk * iqr
        high_fence = q3 + kk * iqr
        stats.update(
            q1=q1, q3=q3, iqr=iqr,
            low_fence=low_fence, high_fence=high_fence, k=kk,
        )
        for i, v in enumerate(values):
            if v > high_fence:
                flags[i] = "high"
            elif v < low_fence:
                flags[i] = "low"

    elif method == "mad":
        kk = 3.5 if k is None else k
        med = stats["median"]
        mad = statistics.median([abs(v - med) for v in values])
        stats.update(mad=mad, k=kk)
        if mad == 0:
            stats["note"] = "MAD=0(대부분 동일 용량)이라 이상치 판정을 생략했습니다."
            return flags, stats
        for i, v in enumerate(values):
            # 0.6745 = 정규분포에서 MAD 를 표준편차로 환산하는 상수
            mz = 0.6745 * (v - med) / mad
            if mz > kk:
                flags[i] = "high"
            elif mz < -kk:
                flags[i] = "low"
    else:
        raise ValueError(f"알 수 없는 method: {method}")

    return flags, stats


# ---------------------------------------------------------------------------
# 막대 차트 렌더링
# ---------------------------------------------------------------------------
# 1/8 단위 부분 블록으로 해상도를 높인다.
_BLOCKS = " ▏▎▍▌▋▊▉█"

RESET = "\033[0m"
BOLD = "\033[1m"
RED = "\033[31m"
YELLOW = "\033[33m"
DIM = "\033[2m"


def _bar(fraction, width):
    """0.0~1.0 비율을 width 폭의 유니코드 막대로."""
    fraction = max(0.0, min(1.0, fraction))
    eighths = int(round(fraction * width * 8))
    full, rem = divmod(eighths, 8)
    s = "█" * full
    if rem:
        s += _BLOCKS[rem]
    # 남는 칸은 공백으로 채워 정렬을 맞춘다(블록/부분블록은 폭 1칸).
    return s + " " * max(0, width - full - (1 if rem else 0))


def _chart_layout(rows, width):
    """차트 열 폭(name_w, size_w, bar_w)과 최대 용량을 계산."""
    term_w = shutil.get_terminal_size((100, 20)).columns
    name_w = min(max((len(r["name"]) for r in rows), default=4), 32)
    size_w = max((len(human_bytes(r["size_bytes"])) for r in rows), default=6)
    if width is not None:
        bar_w = max(5, width)
    else:
        bar_w = max(10, term_w - name_w - size_w - 20)
    max_size = max((r["size_bytes"] for r in rows), default=0) or 1
    return name_w, size_w, bar_w, max_size


def _bar_line(r, name_w, size_w, bar_w, max_size, marker="", color=None, c=None):
    """파티션 1건을 'name │bar│ size marker' 형태의 한 줄로."""
    name = r["name"]
    if len(name) > name_w:
        name = name[: name_w - 1] + "…"
    bar = _bar(r["size_bytes"] / max_size, bar_w)
    size_str = human_bytes(r["size_bytes"]).rjust(size_w)
    raw = f"{name.ljust(name_w)} │{bar}│ {size_str}{marker}"
    return c(color, raw) if (color and c) else raw


def _method_line(stats, c):
    """IQR/MAD 판정 파라미터를 한 줄 요약(없으면 None)."""
    if stats.get("method") == "iqr" and "high_fence" in stats:
        return c(DIM, "IQR(k={k}): Q1={q1}  Q3={q3}  상한={hi}  하한={lo}".format(
            k=stats["k"], q1=human_bytes(stats["q1"]), q3=human_bytes(stats["q3"]),
            hi=human_bytes(stats["high_fence"]),
            lo=human_bytes(max(0, stats["low_fence"]))))
    if stats.get("method") == "mad" and "mad" in stats:
        return c(DIM, "MAD(k={k}): 중앙값={med}  MAD={mad}".format(
            k=stats["k"], med=human_bytes(stats["median"]),
            mad=human_bytes(stats["mad"])))
    return None


def _summary_line(stats):
    return "총합 {total}   평균 {mean}   중앙값 {median}   최대 {mx}   최소 {mn}".format(
        total=human_bytes(stats["total"]),
        mean=human_bytes(stats["mean"]),
        median=human_bytes(stats["median"]),
        mx=human_bytes(stats["max"]),
        mn=human_bytes(stats["min"]),
    )


def render_chart(schema, table, rows, flags, stats, width=None, use_color=True):
    lines = []
    c = (lambda code, s: f"{code}{s}{RESET}") if use_color else (lambda code, s: s)

    title = f"{schema}.{table}  파티션 용량 차트  (파티션 {stats['n']}개)"
    lines.append(c(BOLD, title))
    lines.append("=" * min(72, max(40, len(title) + 2)))

    if not rows:
        lines.append("(파티션을 찾지 못했습니다. 스키마/테이블명 또는 파티션 여부를 확인하세요.)")
        return "\n".join(lines)

    name_w, size_w, bar_w, max_size = _chart_layout(rows, width)

    for r, fl in zip(rows, flags):
        if fl == "high":
            marker, color = " ⚠ HIGH", RED
        elif fl == "low":
            marker, color = " ⚠ low", YELLOW
        else:
            marker, color = "", None
        lines.append(_bar_line(r, name_w, size_w, bar_w, max_size, marker, color, c))

    # 요약 통계
    lines.append("-" * min(72, name_w + bar_w + size_w + 6))
    lines.append(_summary_line(stats))
    ml = _method_line(stats, c)
    if ml:
        lines.append(ml)

    outliers = [
        (r["name"], fl, r["size_bytes"]) for r, fl in zip(rows, flags) if fl
    ]
    if "note" in stats:
        lines.append(c(DIM, stats["note"]))
    elif outliers:
        lines.append(c(BOLD, f"이상치 {len(outliers)}건:"))
        for name, fl, sz in outliers:
            tag = "HIGH" if fl == "high" else "low "
            color = RED if fl == "high" else YELLOW
            lines.append(c(color, f"  [{tag}] {name}  {human_bytes(sz)}"))
    else:
        lines.append("이상치 없음.")

    return "\n".join(lines)


def render_chart_compare(schema, table, rows, res_iqr, res_mad,
                         width=None, use_color=True):
    """IQR 와 MAD 결과를 나란히 비교 표시한다.

    res_iqr / res_mad: detect_outliers() 가 돌려준 (flags, stats) 튜플.
    각 파티션 라인에는 두 방식이 각각 이상치로 판정했는지를 표기하고,
    하단에 방식별/공통/전용 집합을 요약한다.
    """
    flags_i, stats_i = res_iqr
    flags_m, stats_m = res_mad
    lines = []
    c = (lambda code, s: f"{code}{s}{RESET}") if use_color else (lambda code, s: s)

    title = f"{schema}.{table}  파티션 용량 차트  (파티션 {stats_i['n']}개)  [IQR vs MAD]"
    lines.append(c(BOLD, title))
    lines.append("=" * min(88, max(40, len(title) + 2)))

    if not rows:
        lines.append("(파티션을 찾지 못했습니다. 스키마/테이블명 또는 파티션 여부를 확인하세요.)")
        return "\n".join(lines)

    name_w, size_w, bar_w, max_size = _chart_layout(rows, width)

    for r, fi, fm in zip(rows, flags_i, flags_m):
        flagged = [(nm, fl) for nm, fl in (("IQR", fi), ("MAD", fm)) if fl]
        if flagged:
            highs = [nm for nm, fl in flagged if fl == "high"]
            lows = [nm for nm, fl in flagged if fl == "low"]
            parts = []
            if highs:
                parts.append("HIGH(" + "·".join(highs) + ")")
            if lows:
                parts.append("low(" + "·".join(lows) + ")")
            marker = " ⚠ " + "  ".join(parts)
            color = RED if highs else YELLOW
        else:
            marker, color = "", None
        lines.append(_bar_line(r, name_w, size_w, bar_w, max_size, marker, color, c))

    # 요약 통계 + 두 방식 파라미터
    lines.append("-" * min(88, name_w + bar_w + size_w + 6))
    lines.append(_summary_line(stats_i))
    for ml in (_method_line(stats_i, c), _method_line(stats_m, c)):
        if ml:
            lines.append(ml)

    # 표본 부족 등으로 판정을 생략한 경우
    note = stats_i.get("note") or stats_m.get("note")
    if note:
        lines.append(c(DIM, note))
        return "\n".join(lines)

    # 방식별 집합 비교
    def sizemap():
        return {r["name"]: r["size_bytes"] for r in rows}

    szmap = sizemap()
    iqr_set = {r["name"] for r, fl in zip(rows, flags_i) if fl}
    mad_set = {r["name"] for r, fl in zip(rows, flags_m) if fl}
    common = iqr_set & mad_set
    iqr_only = iqr_set - mad_set
    mad_only = mad_set - iqr_set

    def fmt_set(names):
        names = sorted(names, key=lambda n: szmap.get(n, 0), reverse=True)
        return ", ".join(f"{n}({human_bytes(szmap.get(n))})" for n in names) or "-"

    lines.append(c(BOLD, "이상치 비교:"))
    lines.append(f"  IQR 감지 : {len(iqr_set)}건  [{fmt_set(iqr_set)}]")
    lines.append(f"  MAD 감지 : {len(mad_set)}건  [{fmt_set(mad_set)}]")
    lines.append(f"  공통     : {len(common)}건  [{fmt_set(common)}]")
    lines.append(f"  IQR 전용 : {len(iqr_only)}건  [{fmt_set(iqr_only)}]")
    if mad_only:
        lines.append(
            c(YELLOW, f"  MAD 전용 : {len(mad_only)}건  [{fmt_set(mad_only)}]"
                      "   ← IQR 은 놓친 항목")
        )
    else:
        lines.append(f"  MAD 전용 : 0건  [-]")

    if not iqr_set and not mad_set:
        lines.append("두 방식 모두 이상치 없음.")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def parse_args():
    ap = argparse.ArgumentParser(
        description="Greenplum 6.x 테이블의 파티션별 용량 차트 + 이상치 탐지"
    )
    ap.add_argument("--host", default=os.environ.get("PGHOST", "localhost"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", 5432)))
    ap.add_argument("--dbname", default=os.environ.get("PGDATABASE", "postgres"))
    ap.add_argument("--user", default=os.environ.get("PGUSER", "gpadmin"))
    ap.add_argument("--password", default=None, help="미지정 시 PGPASSWORD 또는 프롬프트")
    ap.add_argument("--schema", required=True, help="대상 스키마명")
    ap.add_argument("--table", required=True, help="대상(루트) 테이블명")
    ap.add_argument(
        "--method",
        choices=["iqr", "mad", "both"],
        default="iqr",
        help="이상치 판정 방식 (기본 iqr). both = IQR·MAD 를 모두 돌려 비교",
    )
    ap.add_argument(
        "--k",
        type=float,
        default=None,
        help="이상치 임계 계수 (iqr 기본 1.5, mad 기본 3.5). "
             "--method both 에서는 무시되고 각 방식의 기본값을 사용",
    )
    ap.add_argument("--width", type=int, default=None, help="막대 최대 폭(칸)")
    ap.add_argument(
        "--all-levels",
        action="store_true",
        help="리프 파티션뿐 아니라 모든 레벨(중간 부모 포함) 표시",
    )
    ap.add_argument("--no-color", action="store_true", help="ANSI 색상 비활성화")
    ap.add_argument("--color", action="store_true", help="비-TTY 에서도 색상 강제")
    ap.add_argument("--timeout", type=int, default=15)
    return ap.parse_args()


def main():
    args = parse_args()
    conn = get_connection(args)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    rows = fetch_partition_sizes(
        cur, args.schema, args.table, leaf_only=not args.all_levels
    )
    values = [r["size_bytes"] for r in rows]

    if args.no_color:
        use_color = False
    elif args.color:
        use_color = True
    else:
        use_color = sys.stdout.isatty()

    if args.method == "both":
        # 각 방식은 자체 기본 k(IQR 1.5 / MAD 3.5)를 사용한다.
        res_iqr = detect_outliers(values, method="iqr", k=None)
        res_mad = detect_outliers(values, method="mad", k=None)
        print(
            render_chart_compare(
                args.schema, args.table, rows, res_iqr, res_mad,
                width=args.width, use_color=use_color,
            )
        )
    else:
        flags, stats = detect_outliers(values, method=args.method, k=args.k)
        print(
            render_chart(
                args.schema, args.table, rows, flags, stats,
                width=args.width, use_color=use_color,
            )
        )

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
