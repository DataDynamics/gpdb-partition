# gpdb-partition

Greenplum 6.x 파티션 관리 유틸리티 모음.

## 구성

| 파일 | 설명 |
|---|---|
| `gp_partition_inspector.py` | 스키마의 모든 테이블에 대해 파티션 키 컬럼과 파티션 값(range 경계 / list 값)을 조사·출력 |
| `gp_partition_size_chart.py` | 단일 테이블의 파티션별 용량을 터미널 막대 차트로 표시하고 용량 이상치(outlier) 파티션을 강조 |
| `gp_table_size_inspector.py` | 스키마/DB 의 테이블 용량을 큰 순으로 조사(테이블/인덱스/TOAST 분해 + 막대 바) |
| `gp_skew_inspector.py` | 테이블의 **분산 스큐**(세그먼트 간 데이터 편중)를 점검하고 점검·조치 절차를 제시 |
| `gp_common.py` | 위 스크립트들이 공유하는 헬퍼 모듈(접속·용량/개수 포맷). 단독 실행용 아님 |
| `requirements.txt` | 파이썬 의존성 목록 (`psycopg2-binary`) |

```
gpdb-partition/
├── gp_partition_inspector.py   # 파티션 컬럼/값 조사 (+ --size 용량)
├── gp_partition_size_chart.py  # 파티션 용량 차트 + 이상치 탐지
├── gp_table_size_inspector.py  # 테이블 용량 순위 조사(분해 + 막대)
├── gp_skew_inspector.py        # 분산 스큐(세그먼트 편중) 점검
├── gp_common.py                # 공용 헬퍼(get_connection, human_bytes, human_count)
└── requirements.txt

의존 관계:  gp_common.py  ←  gp_partition_inspector.py / gp_partition_size_chart.py
                            /  gp_table_size_inspector.py / gp_skew_inspector.py
```

> **두 가지 "편중"의 구분**
> - **파티션 스큐** (`gp_partition_size_chart.py`): 한 테이블의 *파티션 간* 용량 불균형.
> - **분산 스큐** (`gp_skew_inspector.py`): 한 테이블의 *세그먼트 간* 행 분포 불균형. `DISTRIBUTED BY`
>   키가 나쁠 때 발생하며 MPP 성능에 가장 치명적이다.

## 요구사항

- Python 3 (표준 라이브러리의 `statistics`, `argparse`, `csv` 등 사용)
- `psycopg2`

```bash
pip3 install -r requirements.txt
# 또는
pip3 install psycopg2-binary
```

- 대상: **Greenplum 6.x** (시스템 카탈로그 뷰 `pg_partitions`, `pg_partition_columns` 사용)
- 모든 스크립트는 **읽기 전용(readonly) 세션**으로 접속하며, 조회 외 변경을 수행하지 않는다.
- 두 실행 스크립트(`gp_partition_inspector.py`, `gp_partition_size_chart.py`)는 `gp_common.py` 를
  import 하므로 같은 디렉터리에 함께 두고 실행한다.

## gp_partition_inspector.py

스키마(`--schema`)를 입력받아, 해당 스키마의 모든 베이스 테이블(파티션 루트 포함, 자식 파티션 제외)에 대해
파티션 여부 · 파티션 타입 · 파티션 키 컬럼 · 각 파티션의 경계/값을 조사한다.
`--schema` 를 생략하면 DB 의 모든 사용자 스키마를 순회하며 조사한다.

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
| `--schema` | 조사할 스키마명. **미지정 시 DB 의 모든 사용자 스키마**를 조사 |
| `--csv <path>` | 결과를 CSV 파일로 저장 (UTF-8 BOM). 여러 스키마는 `schema` 컬럼으로 구분되어 한 파일에 저장 |
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

# --schema 생략 → DB 의 모든 사용자 스키마 조사
python3 gp_partition_inspector.py --dbname mydb

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
| `--method {iqr,mad,both}` | 이상치 판정 방식 (기본 `iqr`). `both` = IQR·MAD 를 모두 돌려 비교 |
| `--k <float>` | 이상치 임계 계수 (iqr 기본 1.5, mad 기본 3.5). `both` 에서는 무시(각 방식 기본값 사용) |
| `--width <n>` | 막대 최대 폭(칸). 미지정 시 터미널 폭에 맞춤 |
| `--all-levels` | 리프뿐 아니라 모든 레벨(중간 부모 포함) 표시 |
| `--no-color` / `--color` | 색상 강제 비활성화 / 활성화 |
| `--timeout <sec>` | 접속 타임아웃(기본 15초) |

### 이상치(outlier) 판단 기준

두 가지 방식을 제공하며, **HIGH(비정상적으로 큼)** 와 **LOW(비정상적으로 작음)** 를 모두 표시한다.

**1) IQR — 기본값 (`--method iqr`)**

사분위수 기반 Tukey's fences 방식.

- `Q1` = 25백분위, `Q3` = 75백분위, `IQR = Q3 − Q1`
- 상한 = `Q3 + k·IQR`, 하한 = `Q1 − k·IQR` (기본 `k = 1.5`)
- 파티션 용량 `v` 에 대해: `v > 상한` → **HIGH**, `v < 하한` → **LOW**

**2) MAD — 수정 z-score (`--method mad`)**

중앙값 절대편차 기반으로, 극단값에 덜 휘둘리는 강건(robust)한 방식.

- `median` = 중앙값, `MAD = median(|vᵢ − median|)`
- 수정 z-score `Mz = 0.6745 · (v − median) / MAD`
  (0.6745 = 정규분포에서 MAD 를 표준편차로 환산하는 상수)
- `Mz > k` → **HIGH**, `Mz < −k` → **LOW** (기본 `k = 3.5`)
- `MAD = 0`(대부분 동일 용량)이면 판정을 생략한다.

**공통 규칙**

- `--k` 로 임계 계수를 조절한다. 값이 **작을수록 더 민감**하게(더 많이) 이상치를 잡는다.
- **표본이 4개 미만**이면 사분위/편차 통계가 무의미하므로 판정을 생략한다.
- 데이터 스큐가 심하면 극단값에 강건한 `--method mad` 를 권장한다.

**3) 두 방식 비교 (`--method both`)**

데이터 편차가 커서 한 방식이 이상치를 못 잡을 때 유용하다. IQR·MAD 를 **각각의 기본
k(1.5 / 3.5)로** 함께 돌려, 파티션별로 어느 방식이 감지했는지(`HIGH(IQR·MAD)`, `HIGH(MAD)` …)를
표기하고, 하단에 방식별 감지 집합을 비교한다.

- **공통** — 두 방식이 모두 이상치로 본 파티션
- **IQR 전용 / MAD 전용** — 한 방식만 감지한 파티션

특히 **강한 우편향(long tail)** 데이터에서는 큰 꼬리값이 `Q3` 를 끌어올려 IQR 울타리가 넓어지면서
IQR 이 아무것도 못 잡는 경우가 있는데, 이때 중앙값 기반 MAD 는 잡아내므로 **`MAD 전용`** 으로
드러난다(`← IQR 은 놓친 항목` 으로 표시). 이 경우 `--method mad` 채택을 고려한다.

```
이상치 비교:
  IQR 감지 : 0건  [-]
  MAD 감지 : 3건  [p07(883.0 MB), p06(724.0 MB), p05(353.0 MB)]
  공통     : 0건  [-]
  IQR 전용 : 0건  [-]
  MAD 전용 : 3건  [p07(883.0 MB), p06(724.0 MB), p05(353.0 MB)]   ← IQR 은 놓친 항목
```

### 사용 예

```bash
# 파티션 용량 차트 + 이상치(IQR)
PGPASSWORD=secret python3 gp_partition_size_chart.py \
    --host 10.0.0.10 --dbname mydb --schema myschema --table sales

# MAD(수정 z-score) 방식으로 판정
python3 gp_partition_size_chart.py --schema myschema --table sales --method mad

# IQR·MAD 를 함께 돌려 비교
python3 gp_partition_size_chart.py --schema myschema --table sales --method both

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

## gp_table_size_inspector.py

스키마(또는 DB 전체)의 테이블 **디스크 용량**을 큰 순으로 조사한다. 각 베이스 테이블(파티션 루트
포함, 자식 파티션 제외)에 대해 파티션 자식까지 합산한 용량을 **테이블 / 인덱스 / TOAST** 로 분해하고
인라인 막대 바로 시각화한다. 접속 인자·환경변수는 다른 스크립트와 동일하다.

- 용량은 `pg_total_relation_size`·`pg_table_size`·`pg_indexes_size`(전 세그먼트 합산)로 계산.
- `ROWS≈` 는 `pg_class.reltuples` 추정치(ANALYZE 기반, 스캔 없음).
- 여러 스키마를 조사하면 하단에 **스키마별 소계**를 함께 표시한다.

### 옵션

| 옵션 | 설명 |
|---|---|
| `--schema` | 대상 스키마. 미지정 시 DB 의 모든 사용자 스키마 |
| `--table` | 단일 테이블만 조사 (`--schema` 필요) |
| `--sort {total,table,index,toast,rows,name}` | 정렬 기준 (기본 `total`) |
| `--top <n>` | 상위 N개만 표시 |
| `--min-size-mb <float>` | 총 용량이 이 값 미만인 테이블 제외 (기본 0) |
| `--no-bar` | 인라인 막대 바 숨김 |
| `--csv <path>` | 결과를 CSV 로 저장 |
| `--no-color` / `--color` | 색상 강제 비활성화 / 활성화 |
| `--timeout <sec>` | 접속 타임아웃(기본 15초) |

### 사용 예

```bash
# 스키마 테이블 용량 순위
PGPASSWORD=secret python3 gp_table_size_inspector.py \
    --host 10.0.0.10 --dbname mydb --schema myschema

# DB 전체 상위 20개, 인덱스 크기 순
python3 gp_table_size_inspector.py --dbname mydb --top 20 --sort index

# 단일 테이블 + CSV
python3 gp_table_size_inspector.py --schema myschema --table sales --csv out.csv
```

### 출력 예

```
테이블 용량 조사  (mydb / myschema)
총 용량: 60.5 GB   테이블: 4개   정렬: total
==============================================================================
#  TABLE            PARTS   ROWS≈     TOTAL     TABLE     INDEX     TOAST     %
-------------------------------------------------------------------------------
1  sales.orders        12  120.0M   50.0 GB   40.0 GB   10.0 GB    4.0 GB  82.7  ██████████████████
2  sales.customers      -    5.0M    8.0 GB    7.0 GB    1.0 GB  716.8 MB  13.2  ██▉
3  sales.items          4    9.0M    2.0 GB    1.5 GB  548.0 MB  150.0 MB   3.3  ▊
-------------------------------------------------------------------------------
   합계                    134.3M   60.5 GB   48.9 GB   11.6 GB    4.9 GB  100.0
```

## gp_skew_inspector.py

테이블의 **분산 스큐**(세그먼트 간 데이터 편중)를 점검한다. `DISTRIBUTED BY` 키가 나쁘면 특정
세그먼트로 데이터가 몰려 MPP 성능이 급락하는데, 이를 지표로 정량화하고 **무엇을·어떻게 점검·조치**할지
데이터에 근거해 제시한다. 접속 인자·환경변수는 다른 스크립트와 동일하다.

각 베이스 테이블(파티션 루트 포함, 자식 파티션 제외)에 대해 세그먼트별 행 수를 집계하여:

- `max/avg` — 가장 무거운 세그먼트 ÷ 평균 (1.0 = 완벽, 클수록 편중). **핵심 지표**
- `CV%` — 세그먼트 간 행 수의 변동계수(표준편차/평균)
- `empty` — 비어 있는 세그먼트 수 (분산키 카디널리티 부족 신호)
- **VERDICT** — `max/avg` 기준 `OK`(<1.3) / `WARN`(<2.0) / `CRIT`(≥2.0). 빈 세그먼트가 있으면 최소 `WARN`

스큐가 심한 순으로 정렬한 요약 그리드 + 세그먼트 분포 스파크라인 + **데이터 기반 점검 SQL·재분산
명령**을 출력하고, 마지막에 공통 **점검 절차 체크리스트**를 제시한다.

> 세그먼트별 행 수는 `count(*)` 스캔이 필요하다. `--min-size-mb`(기본 1MB) 미만 테이블은 스캔을
> 생략(`SKIP`)하고, `--limit N` 으로 용량 상위 N개만 점검해 비용을 제한할 수 있다. 읽기 전용 세션이다.

### 옵션

| 옵션 | 설명 |
|---|---|
| `--schema` | 대상 스키마. 미지정 시 DB 의 모든 사용자 스키마 |
| `--table` | 단일 테이블만 점검 (`--schema` 필요) |
| `--warn <float>` | WARN 임계치 `max/avg` (기본 1.3) |
| `--crit <float>` | CRIT 임계치 `max/avg` (기본 2.0) |
| `--min-size-mb <float>` | 이 크기 미만 테이블은 스캔 생략 (기본 1MB) |
| `--limit <n>` | 용량 상위 N개 테이블만 스캔 |
| `--only-skewed` | WARN/CRIT 테이블만 표시 |
| `--csv <path>` | 지표를 CSV 로 저장 |
| `--no-color` / `--color` | 색상 강제 비활성화 / 활성화 |
| `--timeout <sec>` | 접속 타임아웃(기본 15초) |

### 사용 예

```bash
# 스키마 전체 점검
PGPASSWORD=secret python3 gp_skew_inspector.py \
    --host 10.0.0.10 --dbname mydb --schema myschema

# DB 전체에서 스큐 있는 것만
python3 gp_skew_inspector.py --dbname mydb --only-skewed

# 단일 테이블 + CSV 저장
python3 gp_skew_inspector.py --schema myschema --table sales --csv skew.csv
```

### 출력 예

```
분산 스큐 점검 (distribution skew)
프라이머리 세그먼트: 8개   임계치: WARN max/avg≥1.3, CRIT max/avg≥2.0   대상 테이블: 3개
==============================================================================
#  TABLE         DIST               ROWS    SIZE  MAX/AVG  CV%  EMPTY  VERDICT
------------------------------------------------------------------------------
1  sales.orders  BY (customer_id)   1.0M  5.0 GB     7.20  235    4/8  CRIT
2  sales.logs    RANDOMLY          95.0K  1.0 GB     5.05  153    0/8  CRIT
3  sales.items   BY (item_id)        800  2.0 MB     1.03    2    0/8  OK

▼ 스큐 상세 및 점검 포인트

▶ sales.orders  [CRIT]  max/avg=7.20  CV=235%  empty=4/8
    분산: DISTRIBUTED BY (customer_id)
    세그먼트 분포(내림차순): █▁▁▁▁▁▁▁
      max=900.0K(seg 0)   min=0(빈 세그먼트)   avg=125.0K
    ▷ 점검: 빈 세그먼트 4개 → 분산키 카디널리티 부족 의심
        SELECT count(DISTINCT (customer_id)) AS distinct_keys FROM sales.orders;
        SELECT customer_id, count(*) AS c FROM sales.orders GROUP BY customer_id ORDER BY c DESC LIMIT 20;
        SELECT sum(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_rows FROM sales.orders;
    ▷ 조치: ALTER TABLE sales.orders SET DISTRIBUTED BY (<더_균일한_키>) WITH (reorganize=true);
```

이어서 공통 **점검 절차**(분산키 값 분포 → NULL 쏠림 → 카디널리티 → 재분산 → 재점검)가 출력된다.

## gp_common.py

세 실행 스크립트가 공유하는 헬퍼 모듈이다. **직접 실행하지 않는다.**

| 함수 | 설명 |
|---|---|
| `get_connection(args)` | `args`(host/port/dbname/user/password/timeout)로 **읽기 전용** 세션을 연다 |
| `human_bytes(n)` | 정수 바이트를 `B`/`KB`/`MB`/`GB`/`TB`/`PB` 문자열로 포맷 (`None` → 빈 문자열) |
| `human_count(n)` | 개수(행 수 등)를 `1.2K`/`3.4M` 처럼 1000 단위로 축약 (`None` → 빈 문자열) |

접속 인자·환경변수 처리(`--password` 미지정 시 `PGPASSWORD` 또는 프롬프트)와 용량/개수 포맷을 한
곳에서 관리하여 스크립트들의 동작을 일치시킨다.
