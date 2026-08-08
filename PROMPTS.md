# 🤖 PROMPTS.md — AI/LLM Usage Transparency

> *"Use of AI/LLMs is permitted; however, you must be able to explain all code generated without use of AI."*

---

## 📋 Summary

| Attribute | Detail |
|-----------|--------|
| **LLM Used** | Claude Sonnet 4 (Anthropic, 2025) |
| **Interface** | Kiro CLI — terminal-based AI pair-programming assistant |
| **Total Prompts** | 7 major prompts across architecture, implementation, testing, and monitoring |
| **AI Role** | Accelerated scaffolding, code generation, and documentation |
| **Human Role** | All design decisions, trade-off analysis, and code review |

---

## 🧠 Design Philosophy

I used AI as a **force multiplier**, not a replacement for engineering judgment:

1. **I made all architectural decisions** — AI helped me express them faster
2. **I validated every line of code** — ran tests, debugged E2E issues, fixed real runtime errors
3. **I can explain everything without AI** — see the "Explainability" section below

The AI saved me ~4 hours of boilerplate typing while I focused on:
- Why ClickHouse over Druid/DuckDB (trade-off analysis)
- Why partition by `title_type + start_year` (query pattern analysis)
- Why Snappy over ZSTD (read-path optimization)
- How to handle Spark's hive-style partitioning when loading into ClickHouse

---

## 📝 Prompts Used

### Prompt 1: Initial Project Scaffold

```
Prompt: "Read this assignment [challenge pasted] and complete this with proper 
architecture, tech selection and comparison, scalability HLD and LLD 
documentation, and podman compose testing with all services and dependencies."

LLM: Claude Sonnet 4 via Kiro CLI
```

**What AI generated:**
- Project directory structure
- README.md skeleton with architecture diagrams
- docker-compose.yml with Spark + ClickHouse services
- Script stubs (etl_job.py, load_to_olap.py, download_dataset.py)
- DDL for ClickHouse tables

**What I reviewed/modified:**
- Validated all port mappings and volume mounts
- Verified ClickHouse DDL follows MergeTree best practices
- Adjusted partition strategy from single-key to dual-key (title_type + start_year)
- Fixed Parquet compression setting from default to explicit Snappy

---

### Prompt 2: OLAP Engine Selection & Justification

```
Prompt: "Compare ClickHouse vs Druid vs DuckDB vs StarRocks for this use case - 
local Docker, Parquet integration, sub-second queries, minimal resource footprint. 
Which is best and why?"

LLM: Claude Sonnet 4 via Kiro CLI
```

**What AI provided:**
- Comparison matrix across 8 criteria
- Pros/cons for each engine

**What I decided:**
- ClickHouse — because it's the only engine that combines:
  - Native `file('*.parquet')` ingestion (no ETL connector needed)
  - Single container, 512MB RAM (Druid needs 5+ containers)
  - Production-representative (Cloudflare, Uber, Spotify scale)
  - Actual separate OLAP server (DuckDB is embedded, not an architecture demo)

---

### Prompt 3: ETL Partitioning Strategy

```
Prompt: "For the partitioning strategy, what makes sense for time-series or 
category-based analysis on IMDb data? The challenge specifically asks for a 
strategy that makes sense."

LLM: Claude Sonnet 4 via Kiro CLI
```

**Decision rationale (my own analysis):**

| Option | Parquet Partitions | ClickHouse Partitions | Verdict |
|--------|-------------------|----------------------|---------|
| `title_type` only | ~15 dirs | ~15 partitions | ❌ Too few, year queries scan everything |
| `start_year` only | ~130 dirs | ~130 partitions | ❌ Type queries scan everything |
| `title_type + start_year` | ~800 dirs | N/A (too many for CH) | ✅ Dual predicate pushdown |
| CH: decade grouping | N/A | ~13 partitions | ✅ CH recommends < 1000 |

**Final strategy:**
- Parquet: `partitionBy("title_type", "start_year")` — enables Spark partition pruning
- ClickHouse: `PARTITION BY decade` — coarser grouping, enables time-range pruning

---

### Prompt 4: Performance Benchmark Script

```
Prompt: "Create an analytics benchmark script that runs identical queries on both 
Spark SQL (reading Parquet) and ClickHouse, measuring median of 3 runs, with 
warm-up, to fairly demonstrate the OLAP advantage."

LLM: Claude Sonnet 4 via Kiro CLI
```

**What AI generated:**
- 6 benchmark queries covering aggregation, filtering, joins, string ops
- Timing framework with warm-up and median calculation
- Comparison table output

**What I validated:**
- Queries are semantically identical between Spark SQL and ClickHouse SQL
- Spark uses `explode(split(...))` while ClickHouse uses `arrayJoin(splitByString(...))`
- Both use `local[*]` for Spark and single-node for ClickHouse (fair comparison)

---

### Prompt 5: Unit & Integration Tests

```
Prompt: "Create comprehensive unit and integration tests for the ETL pipeline. 
Unit tests should cover each transformation function in isolation. Integration 
tests should validate the full pipeline flow and data quality."

LLM: Claude Sonnet 4 via Kiro CLI
```

**What AI generated:**
- `conftest.py` with SparkSession fixture, sample DataFrames, temp directories
- 25 unit tests covering `clean_titles`, `clean_ratings`, `clean_episodes`, `join_datasets`, `write_partitioned_parquet`
- 12 integration tests covering full pipeline E2E, data quality, schema validation
- 8 analytics tests verifying query correctness

**What I validated:**
- All 45 tests pass locally (`pytest tests/ → 45 passed in 28s`)
- Tests cover edge cases: null tconst, invalid years, zero votes, duplicate rows, non-tt prefixes
- ClickHouse interactions are properly mocked (no external dependency)

---

### Prompt 6: CI/CD Pipeline

```
Prompt: "Create a GitHub Actions CI/CD workflow with lint, unit tests, integration 
tests, docker build validation, and E2E test on main branch."

LLM: Claude Sonnet 4 via Kiro CLI
```

**What AI generated:**
- `.github/workflows/ci.yml` — 5-job pipeline
- `.github/workflows/release.yml` — auto-release on main

**What I designed:**
- Job dependency graph: `lint → unit-tests → docker-build → e2e-test`
- Integration tests run in parallel (no dependency on lint)
- E2E only runs on `main` (expensive, uses real containers)
- Coverage report uploaded as artifact

---

### Prompt 7: Monitoring Stack (Prometheus + Grafana)

```
Prompt: "Add Prometheus and Grafana monitoring to the pipeline. Include scrape 
configs for ClickHouse and Spark, alerting rules, and a pre-provisioned 
dashboard with ClickHouse metrics and host metrics."

LLM: Claude Sonnet 4 via Kiro CLI
```

**What AI generated:**
- Prometheus scrape configuration
- Grafana datasource + dashboard provisioning
- Dashboard JSON with multiple panels

**What I debugged and fixed (real E2E issues):**
- ClickHouse alpine image doesn't expose `/metrics` by default → created `prometheus.xml` config
- Spark doesn't have native Prometheus endpoint → documented JMX agent requirement
- Grafana datasource UID mismatch → fixed provisioning to use consistent UID
- Dashboard showed duplicate service names → redesigned to 1 stat panel per service

---

## ✅ Explainability — What I Can Explain Without AI

### PySpark ETL (`etl_job.py`)

| Concept | Explanation |
|---------|-------------|
| `schema=StructType([...])` | Explicit schema avoids inference cost on 10M+ rows and prevents type mismatches |
| `.option("nullValue", "\\N")` | IMDb uses literal `\N` string for NULL values in TSV format |
| `.filter(F.col("tconst").startswith("tt"))` | IMDb title IDs always start with "tt"; "nm" is name IDs |
| `.dropDuplicates(["tconst"])` | Source data may have duplicates across file versions |
| `.coalesce(4).partitionBy(...)` | Reduces small file problem while maintaining partition structure |
| `cache()` before join | Prevents re-computation of cleaned DataFrames during multi-join |

### ClickHouse DDL (`ddl.sql`)

| Concept | Explanation |
|---------|-------------|
| `LowCardinality(String)` | `title_type` has ~15 values → dictionary encoding stores as integers internally |
| `PARTITION BY decade` | Groups data by 10-year blocks → enables partition pruning for time-range queries |
| `ORDER BY (title_type, start_year, tconst)` | Primary key determines sparse index; matches our common WHERE patterns |
| `index_granularity = 8192` | Default; one index entry per 8192 rows balances index size vs. skip efficiency |
| `tokenbf_v1(10240, 3, 0)` | Bloom filter skip index for text search on titles/genres without full scan |

### Docker Compose Architecture

| Concept | Explanation |
|---------|-------------|
| `:z` volume suffix | SELinux relabeling for podman rootless containers |
| `service_healthy` condition | Worker waits for master's HTTP 200 before starting (avoids connection refused) |
| Shared `./data` volume | All services mount the same data directory for Parquet file passing |
| `ulimits.nofile: 262144` | ClickHouse needs high file descriptor limit for many data parts |

### Load Strategy (`load_to_olap.py`)

| Concept | Explanation |
|---------|-------------|
| Path-based partition extraction | Spark writes partition columns as directory names, not inside Parquet files |
| `re.search(r'title_type=([^/]+)', filepath)` | Extract partition value from hive-style directory path |
| `fillna('')` for String columns | ClickHouse `LowCardinality(String)` doesn't accept NULL; must be empty string |
| Batch insert (100K rows) | Balances memory usage vs. network round-trip overhead |

---

## 🔍 Bugs I Found & Fixed During E2E Testing

These were real issues discovered during live testing — not AI-generated code working first try:

| Bug | Root Cause | Fix |
|-----|-----------|-----|
| `bitnami/spark:3.5.1` image not found | Docker Hub removed the tag | Switched to `apache/spark:3.5.1` |
| `pandas==2.2.2` fails in container | Apache Spark image uses Python 3.8 | Downgraded to `pandas==2.0.3` |
| `python` not found in container | Image only has `python3` binary | Used `python3` in exec commands |
| Pickle recursion error in tests | Python 3.14 incompatible with PySpark 3.5.1 | Used Python 3.12 venv for testing |
| `Invalid None in non-Nullable column` | Parquet partitioned files don't contain partition columns | Extracted `title_type`/`start_year` from directory paths |
| Port 8080 already in use | Another Java process on host | Remapped to 8085 |
| Grafana "datasource not found" | Auto-generated UID didn't match dashboard reference | Fixed provisioning with explicit UID |
| Dashboard showing duplicate service names | Single stat panel rendered all `up{}` results as separate boxes | Split into 4 individual stat panels |

---

## 📊 What AI Did vs. What I Did

```
┌─────────────────────────────────────────────────────────────────────┐
│                         CONTRIBUTION SPLIT                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  AI (Claude):                         Me (Engineer):                 │
│  ─────────────                        ──────────────                 │
│  • Boilerplate code generation        • Architecture decisions       │
│  • Documentation templates            • Trade-off analysis           │
│  • Test scaffolding                   • OLAP engine selection        │
│  • Prometheus/Grafana JSON            • Partition strategy design    │
│  • CI/CD workflow YAML                • E2E debugging & fixing       │
│  • README structure                   • Performance validation       │
│  • SQL query generation               • Bug diagnosis & resolution  │
│                                       • Code review & understanding  │
│                                       • Runtime testing & validation │
│                                                                      │
│  ~40% of keystrokes                   100% of decisions              │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🏁 Final Note

Every architectural choice, every configuration decision, and every line of code in this project can be explained in a live technical interview. The AI accelerated implementation — it did not replace engineering understanding.
