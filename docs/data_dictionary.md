Data Dictionary: Gold Layer (Star Schema)
1. Introduction

This data dictionary describes the structure and business meaning of the tables within the Gold Layer (Star Schema). The model is designed to analyze pizza delivery performance and monitor courier telemetry data.
2. Fact Table: fact_order_performance

The fact table stores data regarding individual orders and their associated time and distance metrics. Grain: One row per unique order.

    order_id: Unique business identifier for the order.

    courier_key: Foreign key to the courier dimension (dim_courier).

    pizzeria_key: Foreign key to the pizzeria dimension (dim_pizzeria).

    customer_key: Foreign key to the customer dimension (dim_customer).

    date_key: Foreign key to the date dimension (dim_date).

    status_key: Foreign key to the order status dimension (dim_order_status).

    zone_key: Foreign key to the delivery zone dimension (dim_delivery_zone).

    order_timestamp: The date and time the order was placed.

    kitchen_acceptance_timestamp: The time the kitchen accepted the order for production.

    ready_for_pickup_timestamp: The time production ended and the order was ready for delivery.

    pickup_timestamp: The time the courier physically picked up the order.

    delivery_timestamp: The time the order was delivered to the customer.

    production_duration_seconds: Time taken to prepare the pizza (Ready - Acceptance).

    waiting_for_courier_seconds: Time the order waited for the courier (Pickup - Ready).

    delivery_duration_seconds: The duration of the actual delivery transit (Delivery - Pickup).

    total_lead_time_seconds: The end-to-end fulfillment time (Delivery - Order).

    planned_distance_meters: The optimal distance to the destination calculated at the time of order.

    actual_distance_meters: The real distance traveled by the courier based on GPS telemetry pings.

    distance_variance_pct: Percentage difference between actual and planned distance.

    average_speed_kmh: The courier's average speed during delivery calculated from telemetry data.

    is_outside_geofence: Boolean flag indicating if the courier deviated significantly from the optimal route.

    gross_revenue: Total gross revenue from the order.

    delivery_charge: The delivery fee paid by the customer.

    is_delayed_delivery: Boolean flag indicating if the delivery exceeded the promised SLA time.

3. Dimension: dim_courier

Contains information about couriers. This table uses SCD Type 2 to track changes in equipment, vehicles, and status over time.

    courier_key: Surrogate primary key.

    courier_id: Business identifier for the courier.

    courier_name: Full name of the courier.

    vehicle: Type of vehicle used (e.g., bike, scooter, car).

    vehicle_ownership: Indicates if the vehicle is personal or company-owned.

    equipment_set: Set of equipment assigned (e.g., thermal bag, helmet).

    performance_level: Calculated tier based on historical delivery performance.

    employment_status: Current employment status (e.g., Active, Terminated).

    contract_type: Type of contract (e.g., B2B, Full-time).

    insurance_expiry_date: Expiration date of the courier's insurance.

    valid_from: Start timestamp for the record version validity.

    valid_to: End timestamp for the record version validity.

    is_current: Flag indicating the most recent version of the courier record.

4. Dimension: dim_pizzeria

Describes the pizzerias (dispatch points). Also implements SCD Type 2.

    pizzeria_key: Surrogate primary key.

    pizzeria_id: Business identifier for the store location.

    pizzeria_name: Name of the pizzeria branch.

    store_format: Format of the location (e.g., Express, Dine-in Restaurant).

    operating_hours_type: Type of operating hours (e.g., 24/7, Standard).

    kitchen_capacity_level: Categorization of the kitchen's throughput capacity.

    full_address: Full physical address of the store.

    has_outdoor_seating: Indicates if the location has outdoor seating.

    valid_from: Start timestamp for record validity.

    valid_to: End timestamp for record validity.

    is_current: Flag indicating the current active record.

5. Dimension: dim_customer

Stores demographic and behavioral data about customers.

    customer_key: Surrogate primary key.

    customer_id: Business identifier for the customer.

    customer_name: Full name or alias of the customer.

    customer_segment: Customer segmentation (e.g., VIP, New, Churn-risk).

    loyalty_tier: Tier level in the loyalty program.

    acquisition_channel: Channel through which the customer was acquired (e.g., Web, Mobile App).

    birth_year: Year of birth for demographic analysis.

6. Dimension: dim_date

A standardized date dimension for easy filtering and time-series aggregation.

    date_key: Primary key in YYYYMMDD format.

    full_date: The complete date.

    day_name: Name of the day of the week.

    month_name: Name of the month.

    quarter: Quarter of the year.

    is_weekend: Boolean indicating if the day falls on a weekend.

    is_holiday: Boolean indicating if the day is a public holiday.

7. Dimension: dim_order_status

Describes the possible states of an order.

    status_key: Surrogate primary key.

    status_name: Name of the status (e.g., Delivered, Cancelled, Returned).

    status_category: Higher-level category (e.g., Success, Failure).

    is_final_status: Indicates if this status terminates the order process.

8. Dimension: dim_delivery_zone

Describes the geographic delivery zones.

    zone_key: Surrogate primary key.

    zone_name: Name of the zone (e.g., Downtown, Suburbs).

    zone_type: Type of terrain or area (e.g., Urban, Residential, Industrial).

    average_traffic_density: Average historical traffic density level in the zone.