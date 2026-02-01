# Architecture Decision Records (ADR)

This document records the architectural decisions made for the project, including the alternatives considered and the reasoning behind the final choices.

## ADR-001: Data Processing Engine (Spark vs. Pandas)

| Criteria | **Apache Spark (Chosen)** | Pandas (Rejected) |
| :--- | :--- | :--- |
| **Scalability** | **Horizontal Scaling.** Can handle TBs of telemetry data across clusters. | **Vertical Scaling only.** Limited by single machine RAM. |
| **Architecture** | Distributed computing (Driver/Executor model). | Single-node, in-memory. |
| **Streaming** | Native support via **Structured Streaming**. | No native streaming support (requires loops/hacks). |
| **Decision** | **Spark** was chosen to simulate a production-grade Big Data environment and handle the high throughput of telemetry logs. | |

---

## ADR-002: Data Warehouse Storage (PostgreSQL vs. Others)

| Alternative | Pros | Cons | Decision |
| :--- | :--- | :--- | :--- |
| **PostgreSQL** | Standard SQL, open-source, easy Docker integration, wide BI support. | Not columnar (row-oriented), slower for massive OLAP aggregations. | **CHOSEN.** Best balance of ease of use and industry standard for this scale. |
| **MotherDuck** | Serverless DuckDB, great for OLAP, separation of compute/storage. | Cloud dependency, additional latency for local dev, requires token management. | **Rejected.** Wanted a fully local, self-contained Docker environment. |
| **ClickHouse** | Extremely fast columnar OLAP. | High operational complexity (ZooKeeper), steeper learning curve. | **Rejected.** Overkill for the current data volume. |
| **Supabase** | Backend-as-a-Service (Postgres wrapper). | Great for App Devs, handles Auth/API automatically. | **Rejected.** This is a Data Engineering project, not a Web App. We need raw DB control. |

---

## ADR-003: Architecture Pattern (Lambda Architecture)

| Aspect | Detail |
| :--- | :--- |
| **Problem** | The system needs **Real-Time Fleet Tracking** (Latency < 5s) AND **Financial Reporting** (100% Accuracy). |
| **Conflict** | Streaming is fast but can handle late data poorly (approximation). Batch is accurate but slow (latency > 24h). |
| **Decision** | **Lambda Architecture**. |
| **Implementation** | **Speed Layer:** Spark Streaming writes to `live_orders`. **Batch Layer:** Spark Batch recalculates `fact_orders` nightly. |

---

## ADR-004: Business Intelligence Tool (Metabase vs. PowerBI)

| Criteria | **Metabase (Chosen)** | PowerBI (Rejected) |
| :--- | :--- | :--- |
| **Deployment** | **Docker Container.** Runs seamlessly in the `docker-compose` stack. | **Desktop App.** Heavy Windows dependency. |
| **Cost** | Open Source (Free). | Pro License required for sharing/server. |
| **Integration** | Direct JDBC connection to Postgres within Docker network. | Requires Gateway configuration to access local Docker. |
| **Decision** | **Metabase** allows for a reproducible, "Infrastructure-as-Code" BI setup. | |

---

## ADR-005: Data Layering (Medallion Architecture)

| Layer | Format | Purpose |
| :--- | :--- | :--- |
| **Bronze** | JSON/CSV | **Raw & Immutable.** Allows reprocessing (Time Travel) if logic changes. |
| **Silver** | Parquet | **Cleaned & Optimized.** Columnar storage for fast Spark reads. Schema enforcement. |
| **Gold** | Postgres Table | **Aggregated & Served.** Optimized for JOINs and BI queries (Star Schema). |

---

## ADR-006: Orchestration Engine (Airflow vs. Cron vs. Others)

| Tool | Type | Key Features | Verdict |
| :--- | :--- | :--- | :--- |
| **Apache Airflow** | **Workflow Orchestrator** | **Dependency Management (DAGs),** Retries, Logging UI, Backfilling. | **CHOSEN.** Essential for ensuring that Batch jobs finish *before* aggregates start. Industry Standard. |
| **Cron (Linux)** | Time-based Scheduler | Simple, lightweight, available everywhere. | **Rejected.** "Fire and forget" mechanism. No way to handle complex dependencies (e.g., "Wait for file X, then run Y, then run Z"). |
| **Custom Python Script** | Process Manager | Zero overhead, easy to debug locally. | **Used for Demo.** In the local environment, a Python script (`orchestrator.py`) simulates Airflow to save RAM, but Airflow is the target production architecture. |
| **Mage.ai / Prefect**| Modern Orchestrators | "Code-first" approach, lighter than Airflow. | **Rejected.** While modern, Airflow remains the market leader in job offers and enterprise deployments. |

### Justification for Airflow (Logical Choice)
We selected Airflow (conceptually) because our pipeline has **Directed Acyclic Graph (DAG)** dependencies:
1.  *Wait* for `orders.json` to appear in Bronze.
2.  *Run* `slv_btch_orders.py` (Spark).
3.  *If Success:* Run `gld_fact_orders.py`.
4.  *If Fail:* Send Alert.

**Cron** cannot natively handle step 3 ("If Success").
