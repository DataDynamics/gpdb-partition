# gpdb-partition

Greenplum 6.x 파티션 관리 유틸리티 모음.

## 구성

| 스크립트 | 설명 |
|---|---|
| `gp_partition_inspector.py` | 스키마의 모든 테이블에 대해 파티션 키 컬럼과 파티션 값(range 경계 / list 값)을 조사·출력 |

## 요구사항

- Python 3
- `psycopg2`

```bash
pip3 install psycopg2-binary
```

- 대상: **Greenplum 6.x** (시스템 카탈로그 뷰 `pg_partitions`, `pg_partition_columns` 사용)

## gp_partition_inspector.py

특정 스키마를 입력받아, 해당 스키마의 모든 베이스 테이블(파티션 루트 포함, 자식 파티션 제외)에 대해
파티션 여부 · 파티션 타입 · 파티션 키 컬럼 · 각 파티션의 경계/값을 조사한다.

### 접속 정보

명령행 인자 또는 환경변수로 지정한다.

| 인자 | 환경변수 | 기본값 |
|---|---|---|
| `--host` | `PGHOST` | `localhost` |
| `--port` | `PGPORT` | `5432` |
| `--dbname` | `PGDATABASE` | `postgres` |
| `--user` | `PGUSER` | `gpadmin` |
| `--password` | `PGPASSWORD` | 미지정 시 프롬프트로 입력 |

### 옵션

| 옵션 | 설명 |
|---|---|
| `--schema` | (필수) 조사할 스키마명 |
| `--csv <path>` | 결과를 CSV 파일로 저장 (UTF-8 BOM) |
| `--table` | 결과를 격자(grid) 테이블 형태로 출력 (한글 폭 고려) |
| `--raw-boundary` | 파티션 값을 DBeaver처럼 카탈로그 원문(`partitionboundary`) 그대로 출력 |
| `--only-partitioned` | 파티션 테이블만 출력(비파티션 테이블 생략) |
| `--timeout <sec>` | 접속 타임아웃(기본 15초) |

> 조사 전용 스크립트로, 읽기 전용(readonly) 세션으로 접속한다.

### 사용 예

```bash
# 기본 조사 (비밀번호는 PGPASSWORD 또는 프롬프트)
PGPASSWORD=secret python3 gp_partition_inspector.py \
    --host 10.0.0.10 --port 5432 --dbname mydb --user gpadmin \
    --schema myschema

# 격자 테이블 형태로 출력
python3 gp_partition_inspector.py --schema myschema --table

# 파티션 테이블만 출력
python3 gp_partition_inspector.py --schema myschema --only-partitioned

# 결과를 CSV로 저장
python3 gp_partition_inspector.py --schema myschema --csv out.csv

# 카탈로그 경계 원문 그대로 출력
python3 gp_partition_inspector.py --schema myschema --raw-boundary
```

### 출력 예

```
========================================================================
스키마: myschema
========================================================================

[sales]  ▶ 파티션됨  타입=RANGE  파티션수=12
    파티션 키 : L0: sale_date
    파티션 목록:
      - sales_1_prt_p202601                 RANGE  [2026-01-01 ~ 2026-02-01)
      - sales_1_prt_p202602                 RANGE  [2026-02-01 ~ 2026-03-01)
      ...

[customers]  ▶ 파티션 아님
```
