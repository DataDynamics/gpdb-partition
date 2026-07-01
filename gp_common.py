#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gp_common.py

Greenplum 파티션 유틸리티 스크립트들이 공유하는 헬퍼 모듈.
- get_connection(): 읽기 전용 세션으로 접속
- human_bytes():    바이트 → 사람이 읽는 문자열
"""

import getpass
import os
import sys

try:
    import psycopg2
except ImportError:
    sys.stderr.write(
        "psycopg2 가 필요합니다.  pip install psycopg2-binary 후 다시 실행하세요.\n"
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# 연결
# ---------------------------------------------------------------------------
def get_connection(args):
    """args(host/port/dbname/user/password/timeout)로 읽기 전용 접속을 연다."""
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


# ---------------------------------------------------------------------------
# 바이트 → 사람이 읽는 문자열
# ---------------------------------------------------------------------------
def human_bytes(n):
    """정수 바이트를 KB/MB/GB 등으로 포맷. n 이 None 이면 빈 문자열."""
    if n is None:
        return ""
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(n) < 1024.0 or unit == "PB":
            if unit == "B":
                return f"{int(n)} {unit}"
            return f"{n:.1f} {unit}"
        n /= 1024.0
