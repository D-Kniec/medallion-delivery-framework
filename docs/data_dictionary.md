# Data Dictionary (Gold Layer)

Technical documentation for the Pizza Delivery OLAP Data Warehouse (PostgreSQL).
The data model is designed using **Star Schema** principles for high-performance analytics, supplemented by real-time tables for the Speed Layer.

## 1. Dimension Tables (Wymiary)

### `gold.dim_courier` (SCD Type 2)
Stores historical profiles of couriers. Tracks changes in vehicle, contract, or risk factors over time.

| Column | Type | Key | Description |
| :--- | :--- | :--- | :--- |
| `courier_key` | INTEGER | **PK** | Surrogate Key (Unique for every version of a courier). |
| `courier_id` | VARCHAR | NK | Natural Business Key (Constant for the courier). |
| `courier_name` | VARCHAR | | Full name of the courier. |
| `vehicle` | VARCHAR | | Descriptive vehicle info (e.g., "Bike - Trek District"). |
| `vehicle_ownership` | VARCHAR | | 'Company' vs 'Private'. Affects cost calculations. |
| `equipment_set` | VARCHAR | | 'Standard', 'Pro', etc. |
| `performance_level` | VARCHAR | | 'Low', 'Medium', 'High'. |
| `employment_status` | VARCHAR | | 'Active', 'On Leave', 'Terminated'. |
| `contract_type` | VARCHAR | | 'B2B', 'Full-time', 'Part-time'. |
| `insurance_expiry_date` | DATE | | Critical for compliance monitoring. |
| `accident_risk_factor` | DECIMAL(10,4) | | Risk score (0.0 - 1.0) used for insurance optimization. |
| `fatigue_resistance` | DECIMAL(10,2) | | Bio-metric score affecting max shift length. |
| `vehicle_range_km` | DECIMAL(10,2) | | Max distance before refuel/recharge. |
| `valid_from` | TIMESTAMP | | SCD2: Start validity of this record. |
| `valid_to` | TIMESTAMP | | SCD2: End validity (NULL if current). |
| `is_current` | BOOLEAN | | SCD2: Quick filter for current state. |

### `gold.dim_pizzeria` (SCD Type 1)
Static information about pizza storage locations (nodes).

| Column | Type | Key | Description |
| :--- | :--- | :--- | :--- |
| `pizzeria_key` | INTEGER | **PK** | Surrogate Key. |
| `pizzeria_id` | VARCHAR | NK | Natural Key (e.g., 'PIZ-WAW-01'). |
| `pizzeria_name` | VARCHAR | | Display name of the restaurant. |
| `store_format` | VARCHAR | | 'Express', 'Dine-In', 'Dark Kitchen'. |
| `operating_hours_type`| VARCHAR | | 'Standard', '24h'. |
| `kitchen_capacity_level`| VARCHAR | | Throughput capacity ('High', 'Low'). |
| `manager_name` | VARCHAR | | Current responsible manager. |
| `full_address` | VARCHAR | | Physical address. |
| `city` | VARCHAR | | City (e.g., 'Warsaw'). |
| `region` | VARCHAR | | Operational region. |
| `country` | VARCHAR | | Country code. |
| `has_outdoor_seating` | BOOLEAN | | Context for summer sales analysis. |
| `kitchen_performance_factor`| DECIMAL(10,2) | | Efficiency multiplier (0.8 - 1.2). |
| `lat` / `lon` | DECIMAL(10,4)| | Geographic coordinates. |

### `gold.dim_customer`
Customer profiles and segmentation.

| Column | Type | Key | Description |
| :--- | :--- | :--- | :--- |
| `customer_key` | INTEGER | **PK** | Surrogate Key. |
| `customer_id` | VARCHAR | NK | Natural Key. |
| `customer_name` | VARCHAR | | Full Name (GDPR sensitive). |
| `customer_segment` | VARCHAR | | 'Retail', 'Corporate', 'VIP'. |
| `loyalty_tier` | VARCHAR | | 'Bronze', 'Silver', 'Gold'. |
| `acquisition_channel` | VARCHAR | | 'App', 'Web', 'Partner'. |
| `preferred_payment_method`| VARCHAR | | 'Card', 'Cash', 'ApplePay'. |
| `birth_year` | INTEGER | | Demographics analysis. |
| `city` / `postal_code` | VARCHAR | | Location data. |
| `dropoff_difficulty_score`| DECIMAL(10,2)| | 0.0-1.0 score (e.g. 4th floor without elevator). |

### `gold.dim_order_status`
Standardized dictionary of possible order states.

| Column | Type | Key | Description |
| :--- | :--- | :--- | :--- |
| `status_key` | INTEGER | **PK** | ID (1=IDLE, 5=DELIVERED). |
| `status_name` | VARCHAR | | 'PREPARING', 'IN_REALIZATION', etc. |
| `status_category` | VARCHAR | | 'Active', 'Completed', 'Cancelled'. |
| `is_final_status` | BOOLEAN | | True if the order workflow ends here. |

### `gold.dim_delivery_zone`
Geographical classification of delivery areas.

| Column | Type | Key | Description |
| :--- | :--- | :--- | :--- |
| `zone_key` | INTEGER | **PK** | Zone ID. |
| `zone_name` | VARCHAR | | e.g., 'Zone 1 (Center)'. |
| `zone_type` | VARCHAR | | 'Urban', 'Suburban', 'Rural'. |
| `traffic_drag` | DECIMAL(10,2)| | Coefficient affecting estimated time (higher = slower). |

### `gold.dim_date`
Standard date dimension for time-series analysis.

| Column | Type | Description |
| :--- | :--- | :--- |
| `date_key` | INTEGER | **PK** YYYYMMDD format. |
| `full_date` | DATE | Standard date format. |
| `is_weekend` | BOOLEAN | True for Sat/Sun. |
| `is_holiday` | BOOLEAN | True for national holidays. |
| `season` | VARCHAR | 'Winter', 'Spring', etc. |

---

## 2. Fact Tables (Fakty)

### `gold.fact_order_performance`
Central fact table combining timestamps, financial metrics, and references to dimensions.

| Column | Type | Key | Description |
| :--- | :--- | :--- | :--- |
| `order_id` | VARCHAR | **PK** | Natural Order ID. |
| `courier_key` | INTEGER | **FK** | Reference to `dim_courier`. |
| `pizzeria_key` | INTEGER | **FK** | Reference to `dim_pizzeria`. |
| `customer_key` | INTEGER | **FK** | Reference to `dim_customer`. |
| `status_key` | INTEGER | **FK** | Reference to `dim_order_status`. |
| `zone_key` | INTEGER | **FK** | Reference to `dim_delivery_zone`. |
| `date_key` | INTEGER | **FK** | Partitioning Key (YYYYMMDD). |
| `order_timestamp` | TIMESTAMP | | Time order was placed. |
| `preparation_timestamp`| TIMESTAMP | | Time kitchen started working. |
| `pickup_timestamp` | TIMESTAMP | | Time courier picked up pizza. |
| `delivery_timestamp` | TIMESTAMP | | Time customer received pizza. |
| `return_timestamp` | TIMESTAMP | | Time courier returned to base (if applicable). |
| `total_distance` | DECIMAL(10,2)| | Distance covered (km). |
| `total_time` | DECIMAL(10,2)| | Total duration (min). |
| `outside_geofence` | BOOLEAN | | Flag: Did courier go out of bounds? |
| `is_completed` | BOOLEAN | | Flag: Was order successful? |

---

## 3. Streaming / Speed Layer Tables

### `gold.live_orders_log`
Real-time log of incoming orders (Append-Only).

| Column | Type | Description |
| :--- | :--- | :--- |
| `order_id` | VARCHAR | Unique Order ID. |
| `customer_id` | VARCHAR | Reference to customer. |
| `gross_revenue` | DECIMAL | Value of the order. |
| `order_timestamp` | TIMESTAMP | Event time. |
| `ingestion_timestamp`| TIMESTAMP | Processing time (for lag monitoring). |

### `gold.live_fleet_status`
Real-time snapshot of the fleet (Upsert / Latest State).

| Column | Type | Description |
| :--- | :--- | :--- |
| `courier_id` | VARCHAR | Courier ID. |
| `current_lat/lon` | DECIMAL | Live GPS position. |
| `order_status` | VARCHAR | What are they doing now? |
| `speed_kmh` | DECIMAL | Current speed. |
| `time_left_sec` | DECIMAL | ETA to customer. |
| `is_outside_geofence`| BOOLEAN | Alert flag. |

---

## 4. Business Views (Widoki)

### `gold.v_order_360`
Composite view joining Fact, Dims, and Live tables. Used by BI tools (Metabase).

* **Purpose:** Single source of truth for "What happened with Order X?".
* **Calculated Metrics:**
    * `preparation_time_sec`: `pickup_timestamp` - `preparation_timestamp`
    * `pizza_idle_time_sec`: Time pizza waited on the counter.
    * `realization_percent`: Progress bar (0-100%).
    * `delayed_delivery`: True if total time > Promised time.
