Table dim_courier {
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
  valid_from timestamp
  valid_to timestamp
  is_current boolean
}

Table dim_pizzeria {
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
  valid_from timestamp
  valid_to timestamp
  is_current boolean
}

Table dim_customer {
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
}

Table dim_date {
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

Table dim_order_status {
  status_key integer [pk]
  status_name varchar
  status_category varchar
  is_final_status boolean
}

Table dim_delivery_zone {
  zone_key integer [pk]
  zone_name varchar
  zone_type varchar
  average_traffic_density varchar
}

Table fact_order_performance {
  order_id varchar [pk]
  courier_key integer [ref: > dim_courier.courier_key]
  pizzeria_key integer [ref: > dim_pizzeria.pizzeria_key]
  customer_key integer [ref: > dim_customer.customer_key]
  date_key integer [ref: > dim_date.date_key]
  status_key integer [ref: > dim_order_status.status_key]
  zone_key integer [ref: > dim_delivery_zone.zone_key]
  
  order_timestamp timestamp
  kitchen_acceptance_timestamp timestamp
  ready_for_pickup_timestamp timestamp
  pickup_timestamp timestamp
  delivery_timestamp timestamp
  
  production_duration_seconds integer
  waiting_for_courier_seconds integer
  delivery_duration_seconds integer
  total_lead_time_seconds integer
  
  planned_distance_meters integer
  actual_distance_meters integer
  distance_variance_pct float
  average_speed_kmh float
  is_outside_geofence boolean
  
  gross_revenue decimal(10,2)
  delivery_charge decimal(10,2)
  is_delayed_delivery boolean
}