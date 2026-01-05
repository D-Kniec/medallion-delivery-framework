Pizza Delivery Analytics Platform
End-to-End Medallion Data Pipeline
1. Project Overview

The Pizza Analytics Platform is designed for comprehensive tracking of order lifecycles and delivery logistics analysis. The project focuses on solving "Last-Mile" challenges by processing real-time courier telemetry data. The system transforms raw events and GPS streams into high-value business metrics using a Medallion Architecture (Bronze, Silver, Gold).
2. Key Features

    Telemetry Processing: Aggregating raw GPS pings into actionable metrics such as actual distance and average speed.

    Dimensional Modeling: Implementation of a Star Schema in the Gold layer with historical data tracking (SCD Type 2).

    Route Variance Analysis: Automated calculation of the difference between planned and actual distance to identify inefficiencies.

    Modern Tech Stack: Leveraging DuckDB as a high-performance OLAP engine for fast, local analytical processing.

3. Architecture Overview

The project is built on containerization (Docker) and a three-layer data model:

    Bronze (Raw): Landing zone for raw JSON files from the Order API and IoT streams.

    Silver (Refined): Data cleansing, deduplication, and reconstruction of courier delivery paths.

    Gold (Curated): Final Star Schema optimized for reporting and ML model consumption.

Detailed documentation is available in: docs/architecture.md.
4. Data Model

The Gold layer centers on the fact_order_performance table, enabling the analysis of:

    Operational Bottlenecks: Duration tracking for production, waiting, and delivery stages.

    Logistics Accuracy: Comparison between planned and actual distances.

    Financial Efficiency: Revenue breakdown by customer segments and delivery zones.

5. Tech Stack

    Storage / OLAP: DuckDB (In-process Analytical Database).

    Orchestration: Apache Airflow.

    Language: Python, SQL.

    Infrastructure: Docker, Docker Compose.

    Modeling: DBML, Star Schema.

6. Project Roadmap

    Phase 1: Data Model Design (Gold Layer, DBML, Documentation).

    Phase 2: Ingestion Engine Implementation (Python scripts for the Bronze layer).

    Phase 3: Transformation Logic (SQL/Python for Silver and Gold layers).

    Phase 4: Orchestration (Building Airflow DAGs).

    Phase 5: Consumption Layer (Integration with BI tools).

7. Setup and Exploration

Note: The project is currently in Phase 1 (Design and Modeling). To explore the data model:

    Navigate to the models/dbml/ folder.

    Use the dbdiagram.io tool to visualize the schema files.

    Review the technical specifications in the docs/ folder.