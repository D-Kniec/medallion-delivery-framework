Project PizzaWarehouse {
  database_type: 'Postgres'
  Note: 'Gold Layer Schema for Pizza Delivery OLAP'
}

// ==========================================
// Dimensions
// ==========================================

Table gold.dim_courier {
  courier_key integer [pk]
  courier_id varchar
  courier_name varchar
  vehicle varchar
  vehicle_ownership varchar
  equipment_set varchar
  performance_level varchar
  employment_status varchar
  contract_type varchar
  insurance_expiry_date date
  accident_risk_factor decimal(10,4)
  fatigue_resistance decimal(10,2)
  vehicle_range_km decimal(10,2)
  valid_from timestamp
  valid_to timestamp
  is_current boolean
}

Table gold.dim_pizzeria {
  pizzeria_key integer [pk]
  pizzeria_id varchar
  pizzeria_name varchar
  store_format varchar
  operating_hours_type varchar
  kitchen_capacity_level varchar
  manager_name varchar
  full_address varchar
  city varchar
  region varchar
  country varchar
  has_outdoor_seating boolean
  kitchen_performance_factor decimal(10,2)
  lat decimal(10,4)
  lon decimal(10,4)
  valid_from timestamp
  valid_to timestamp
  is_current boolean
}

Table gold.dim_customer {
  customer_key integer [pk]
  customer_id varchar
  customer_name varchar
  customer_segment varchar
  loyalty_tier varchar
  acquisition_channel varchar
  preferred_payment_method varchar
  birth_year integer
  city varchar
  postal_code varchar
  dropoff_difficulty_score decimal(10,2)
}

Table gold.dim_order_status {
  status_key integer [pk]
  status_name varchar
  status_category varchar
  is_final_status boolean
}

Table gold.dim_delivery_zone {
  zone_key integer [pk]
  zone_name varchar
  zone_type varchar
  traffic_drag decimal(10,2)
}

Table gold.dim_date {
  date_key integer [pk]
  full_date date
  day_name varchar
  day_of_week integer
  month_name varchar
  month_number integer
  quarter varchar
  year integer
  is_weekend boolean
  is_holiday boolean
  season varchar
}

// ==========================================
// Live / Streaming Tables
// ==========================================

Table gold.live_orders_log {
  order_id varchar [pk]
  customer_id varchar
  gross_revenue decimal(10,2)
  delivery_charge decimal(10,2)
  pizzeria_lat decimal(10,4)
  pizzeria_lon decimal(10,4)
  order_timestamp timestamp
  ingestion_timestamp timestamp
}

Table gold.live_fleet_status {
  courier_id varchar [pk]
  order_id varchar
  current_lat decimal(10,4)
  current_lon decimal(10,4)
  order_status varchar
  updated_at timestamp
  speed_kmh decimal(10,2)
  is_outside_geofence boolean
  time_left_sec decimal(10,2)
  distance_left_km decimal(10,2)
}

// ==========================================
// Fact Table
// ==========================================

Table gold.fact_order_performance {
  order_id varchar [pk]
  courier_key integer
  pizzeria_key integer
  customer_key integer
  status_key integer
  zone_key integer
  date_key integer
  order_timestamp timestamp
  preparation_timestamp timestamp
  pickup_timestamp timestamp
  delivery_timestamp timestamp
  return_timestamp timestamp
  total_distance decimal(10,2)
  total_time decimal(10,2)
  outside_geofence boolean
  is_completed boolean
}

// ==========================================
// Business Views (Calculated)
// ==========================================

Table gold.v_order_360 {
  order_id varchar
  courier_name varchar
  vehicle_type varchar
  pizzeria_name varchar
  customer_name varchar
  raw_status varchar
  zone_name varchar
  
  // Calculated Metrics
  realization_percent decimal
  simple_order_status varchar
  
  // Timestamps
  order_timestamp timestamp
  preparation_timestamp timestamp
  pickup_timestamp timestamp
  delivery_timestamp timestamp
  
  // Performance
  preparation_time_sec integer
  pizza_idle_time_sec integer
  total_time_sec decimal
  average_speed_kmh decimal
  
  // Flags
  outside_geofence boolean
  delayed_delivery boolean
  
  // Financials
  gross_revenue decimal(10,2)
  delivery_charge decimal(10,2)
  
  Note: "Composite view joining Fact, Dims, and Live tables with calculated KPIs"
}

// ==========================================
// Relationships
// ==========================================

Ref: gold.fact_order_performance.courier_key > gold.dim_courier.courier_key
Ref: gold.fact_order_performance.pizzeria_key > gold.dim_pizzeria.pizzeria_key
Ref: gold.fact_order_performance.customer_key > gold.dim_customer.customer_key
Ref: gold.fact_order_performance.status_key > gold.dim_order_status.status_key
Ref: gold.fact_order_performance.zone_key > gold.dim_delivery_zone.zone_key
Ref: gold.fact_order_performance.date_key > gold.dim_date.date_key