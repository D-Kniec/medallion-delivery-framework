# Business Logic & KPI Definitions

Definitions of metrics and processing logic used in the **Silver** and **Gold** layers.

## 1. Key Performance Indicators (KPIs)

### A. On-Time Delivery Rate
Percentage of orders delivered within the guaranteed time window.

* **Formula:** `(Count of Orders where delivery_time_mins <= 45) / Total Orders * 100%`
* **Threshold:** 45 minutes from `order_placed_at` to `delivered_at`.
* **Implementation:** Calculated in `fact_daily_kpi.py`.

### B. Courier Efficiency Score
A metric determining courier performance based on delivery speed and vehicle type.

* **Logic:**
    * If `vehicle_type` = 'bike' AND `delivery_distance` < 3km: **High Efficiency**
    * If `vehicle_type` = 'car' AND `delivery_distance` < 1km: **Low Efficiency** (Fuel waste)

## 2. Data Engineering Patterns

### SCD Type 2 Logic (Courier History)
When a courier changes their vehicle (e.g., Bike -> Car):
1.  **Close Old Record:** Update `valid_to` = `current_timestamp` and `is_current` = `False` for the existing record.
2.  **Open New Record:** Insert new row with new `vehicle_type`, `valid_from` = `current_timestamp`, `valid_to` = `NULL`, and `is_current` = `True`.
3.  **Why?** Ensures that historical orders delivered by bike remain associated with "Bike" in reports, even if the courier drives a car today.

### Speed Layer Logic (Real-Time)
* **Late Arrival Handling:** If streaming data arrives > 10 minutes late (Watermark), it is dropped from the real-time view but captured by the Batch Layer next day.
* **Deduplication:** Events are deduplicated based on `order_id` within a 10-minute sliding window.
