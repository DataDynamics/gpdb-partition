# gpdb-partition

Greenplum 6.x 파티션 관리 유틸리티 모음.

## 구성

| 스크립트 | 설명 |
|---|---|
| `gp_partition_inspector.py` | 스키마의 모든 테이블에 대해 파티션 키 컬럼과 파티션 값(range 경계 / list 값)을 조사·출력 |
| `gp_partition_size_chart.py` | 단일 테이블의 파티션별 용량을 터미널 막대 차트로 표시하고 용량 이상치(outlier) 파티션을 강조 |

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
| `--size` | 파티션별 디스크 용량을 함께 표시(테이블별 총용량 포함) |
| `--timeout <sec>` | 접속 타임아웃(기본 15초) |

> `--size` 는 각 파티션의 실제 자식 테이블에 대해 `pg_total_relation_size()` 를 호출한다.
> Greenplum 에서 이 함수는 전 세그먼트에 걸친 클러스터 전체 용량을 합산하며, 힙 + 인덱스 + TOAST 를
> 모두 포함한다. `--table` / `--csv` 출력에도 용량 컬럼이 추가된다(CSV: `size_bytes`, `size_pretty`).

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

# 파티션별 용량과 함께 출력
python3 gp_partition_inspector.py --schema myschema --size

# 격자 테이블 + 용량
python3 gp_partition_inspector.py --schema myschema --table --size
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

`--size` 를 지정하면 각 파티션의 용량과 테이블별 총용량이 함께 표시된다.

```
[sales]  ▶ 파티션됨  타입=RANGE  파티션수=12  총용량=3.0 GB
    파티션 키 : L0: sale_date
    파티션 목록:
      - sales_1_prt_p202601      RANGE  [2026-01-01 ~ 2026-02-01)          256.0 MB
      - sales_1_prt_p202602      RANGE  [2026-02-01 ~ 2026-03-01)          260.5 MB
      ...
```

## gp_partition_size_chart.py

단일 테이블(`--dbname` / `--schema` / `--table`)의 파티션별 디스크 용량을 터미널 막대 차트로
표시하고, 용량이 통계적으로 이상치(outlier)인 파티션을 강조 표시한다. 접속 인자·환경변수는
`gp_partition_inspector.py` 와 동일하다.

- 용량은 각 파티션의 실제 자식 테이블에 `pg_total_relation_size()` 를 호출해 얻는다
  (전 세그먼트 합산, 인덱스/TOAST 포함).
- 기본적으로 **리프 파티션만** 대상으로 한다(중간 레벨 부모는 데이터가 없어 차트를 왜곡하므로 제외).
- 이상치 판정: **IQR**(기본, `Q3 ± k·IQR`, k=1.5) 또는 **MAD**(수정 z-score, k=3.5). 표본이 4개
  미만이면 판정을 생략한다. HIGH(비정상적으로 큼)와 LOW(비정상적으로 작음)를 모두 표시한다.
- 색상은 TTY 에서 자동 활성화(HIGH=빨강, LOW=노랑). 파이프/리다이렉트 시 자동 비활성화.

### 옵션

| 옵션 | 설명 |
|---|---|
| `--schema` | (필수) 대상 스키마명 |
| `--table` | (필수) 대상(루트) 테이블명 |
| `--method {iqr,mad}` | 이상치 판정 방식 (기본 `iqr`) |
| `--k <float>` | 이상치 임계 계수 (iqr 기본 1.5, mad 기본 3.5) |
| `--width <n>` | 막대 최대 폭(칸). 미지정 시 터미널 폭에 맞춤 |
| `--all-levels` | 리프뿐 아니라 모든 레벨(중간 부모 포함) 표시 |
| `--no-color` / `--color` | 색상 강제 비활성화 / 활성화 |
| `--timeout <sec>` | 접속 타임아웃(기본 15초) |

### 사용 예

```bash
# 파티션 용량 차트 + 이상치(IQR)
PGPASSWORD=secret python3 gp_partition_size_chart.py \
    --host 10.0.0.10 --dbname mydb --schema myschema --table sales

# MAD(수정 z-score) 방식으로 판정
python3 gp_partition_size_chart.py --schema myschema --table sales --method mad

# 색상 없이(로그 저장용)
python3 gp_partition_size_chart.py --schema myschema --table sales --no-color
```

### 출력 예

```
myschema.sales  파티션 용량 차트  (파티션 12개)
========================================
sales_1_prt_p00 │█████▌                                  │ 250.0 MB
sales_1_prt_p01 │█████▊                                  │ 260.0 MB
...
sales_1_prt_p06 │████████████████████████████████████████│   1.8 GB ⚠ HIGH
...
sales_1_prt_p08 │                                        │   2.0 MB ⚠ low
...
---------------------------------------------------------------------
총합 4.3 GB   평균 363.7 MB   중앙값 256.5 MB   최대 1.8 GB   최소 2.0 MB
IQR(k=1.5): Q1=253.2 MB  Q3=259.2 MB  상한=268.2 MB  하한=244.2 MB
이상치 2건:
  [HIGH] sales_1_prt_p06  1.8 GB
  [low ] sales_1_prt_p08  2.0 MB
```
