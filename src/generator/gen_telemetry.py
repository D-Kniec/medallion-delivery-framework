import pandas as pd
import random
import time
import math
import uuid
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta

CURRENT_DIR = Path(__file__).resolve().parent
BASE_BRONZE_PATH = CURRENT_DIR.parent / 'bronze'

STEP_DURATION = 0.5 
KMH_CONVERSION_FACTOR = 111.1 * (3600 / STEP_DURATION)
ORDER_CHANCE = 0.08
LOG_THROTTLE = 10      
GEOFENCE_RADIUS_KM = 2.0
DELAY_THRESHOLD_MIN = 10
GEO_RADIUS_LIMIT = 0.05

PATHS = {
    "customers": BASE_BRONZE_PATH / 'customers' / 'customers.csv',
    "couriers": BASE_BRONZE_PATH / 'couriers' / 'couriers.csv',
    "pizzerias": BASE_BRONZE_PATH / 'pizzerias' / 'pizzerias.csv'
}

telemetry_buffer = []
orders_buffer = []
last_flush_time = time.time()

def flush_buffers():
    global last_flush_time
    now = datetime.now(timezone.utc)
    dt_str = now.strftime("%Y-%m-%d")
    timestamp_suffix = int(now.timestamp())

    for data_type, buffer in [("telemetry", telemetry_buffer), ("orders", orders_buffer)]:
        if not buffer:
            continue
        
        partition_path = BASE_BRONZE_PATH / data_type / f"dt={dt_str}"
        partition_path.mkdir(parents=True, exist_ok=True)
        
        file_path = partition_path / f"{data_type}_batch_{timestamp_suffix}.json"
        
        with open(file_path, 'a', encoding='utf-8') as f:
            for record in buffer:
                f.write(json.dumps(record) + '\n')
        
        buffer.clear()
    
    last_flush_time = time.time()

def calculate_dist_km(lat1, lon1, lat2, lon2):
    return math.sqrt((lat2 - lat1)**2 + (lon2 - lon1)**2) * 111.1

def get_zone_data(lat, lon, b_lat, b_lon):
    dist = calculate_dist_km(lat, lon, b_lat, b_lon)
    if dist < 1.0: return 1, 0.4
    if dist < 2.0: return 2, 0.6
    if dist < 3.5: return 3, 0.8
    if dist < 5.0: return 4, 0.9
    return 5, 1.1

def load_data():
    cust = pd.read_csv(PATHS["customers"])
    cur_df = pd.read_csv(PATHS["couriers"])
    store = pd.read_csv(PATHS["pizzerias"])
    for df in [cust, store, cur_df]:
        if 'lat' in df.columns: df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        if 'lon' in df.columns: df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
    cur_df['pizzeria_id'] = cur_df['pizzeria_id'].astype(str)
    store['pizzeria_id'] = store['pizzeria_id'].astype(str)
    return cust, cur_df.merge(store, on='pizzeria_id', how='inner')

cust_db, couriers_db = load_data()

couriers_states = []
for _, row in couriers_db.iterrows():
    v_type = row['vehicle_type'].upper()
    couriers_states.append({
        "courier_id": row['courier_id'],
        "vehicle_type": v_type,
        "base_lat": row['lat'], "base_lon": row['lon'],
        "current_lat": row['lat'], "current_lon": row['lon'],
        "status": "IDLE",
        "current_order": None,
        "log_counter": 0,
        "trip_total_dist": 0.0,
        "speed_params": {
            "range": (1, 5) if "BIKE" in v_type or "BICYCLE" in v_type else (4, 15),
            "noise": (-0.1, 0.1) if "BIKE" in v_type or "BICYCLE" in v_type else (-4.0, 0.4)
        },
        "force_initial_prep_log": False
    })

try:
    while True:
        now_dt = datetime.now(timezone.utc)
        now_iso = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        idle_couriers = [c for c in couriers_states if c["status"] == "IDLE"]
        
        if idle_couriers and random.random() < ORDER_CHANCE:
            selected_courier = random.choice(idle_couriers)
            local_cust = cust_db[
                (cust_db['lat'].between(selected_courier['base_lat'] - GEO_RADIUS_LIMIT, selected_courier['base_lat'] + GEO_RADIUS_LIMIT)) &
                (cust_db['lon'].between(selected_courier['base_lon'] - GEO_RADIUS_LIMIT, selected_courier['base_lon'] + GEO_RADIUS_LIMIT))
            ]
            if not local_cust.empty:
                customer = local_cust.sample(1).iloc[0]
                order_id = f"ORD-{random.randint(100000, 999999)}"
                selected_courier["current_order"] = {
                    "order_id": order_id,
                    "customer_id": customer["customer_id"],
                    "target_lat": customer["lat"], "target_lon": customer["lon"],
                    "order_timestamp": (now_dt - timedelta(minutes=random.randint(1, 2))).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "kitchen_acceptance_timestamp": now_iso,
                    "ready_for_pickup_timestamp": None,
                    "pickup_timestamp": None,
                    "delivery_timestamp": None,
                    "gross_revenue": float(round(random.uniform(30, 180), 2)),
                    "delivery_charge": float(round(random.uniform(5, 15), 2)),
                    "prep_ticks_left": random.randint(15, 40)
                }
                
                order_event = {
                    "event_type": "order_created",
                    "order_id": selected_courier["current_order"]["order_id"],
                    "customer_id": selected_courier["current_order"]["customer_id"],
                    "order_timestamp": selected_courier["current_order"]["order_timestamp"],
                    "gross_revenue": selected_courier["current_order"]["gross_revenue"],
                    "delivery_charge": selected_courier["current_order"]["delivery_charge"],
                    "pizzeria_lat": selected_courier["base_lat"],
                    "pizzeria_lon": selected_courier["base_lon"]
                }
                print(json.dumps(order_event))
                orders_buffer.append(order_event)
                
                selected_courier["trip_total_dist"] = calculate_dist_km(selected_courier["base_lat"], selected_courier["base_lon"], customer["lat"], customer["lon"])
                selected_courier["status"] = "PREPARING"
                selected_courier["force_initial_prep_log"] = True

        for c in couriers_states:
            force_log = False
            if c["force_initial_prep_log"]:
                force_log = True
                c["force_initial_prep_log"] = False
            current_speed_kmh = 0.0
            current_remaining_dist = 0.0
            realization_percent = 0.0
            if c["status"] == "IDLE":
                continue
            elif c["status"] == "PREPARING":
                c["current_order"]["prep_ticks_left"] -= 1
                if c["current_order"]["prep_ticks_left"] <= 0:
                    c["current_order"]["ready_for_pickup_timestamp"] = now_iso
                    c["current_order"]["pickup_timestamp"] = now_iso
                    c["status"] = "IN_REALIZATION"
                    force_log = True
            elif c["status"] in ["IN_REALIZATION", "RETURNING"]:
                is_delivery = (c["status"] == "IN_REALIZATION")
                t_lat = c["current_order"]["target_lat"] if is_delivery else c["base_lat"]
                t_lon = c["current_order"]["target_lon"] if is_delivery else c["base_lon"]
                zone_key, zone_mult = get_zone_data(c["current_lat"], c["current_lon"], c["base_lat"], c["base_lon"])
                noise_range = c["speed_params"]["noise"]
                if "CAR" in c["vehicle_type"] and c["status"] == "RETURNING": noise_range = (-4.0, 2.0)
                base_speed = random.uniform(*c["speed_params"]["range"])
                step_speed = ((base_speed + random.uniform(*noise_range)) * zone_mult) / 100000
                current_speed_kmh = round(step_speed * KMH_CONVERSION_FACTOR, 2)
                current_remaining_dist = calculate_dist_km(c["current_lat"], c["current_lon"], t_lat, t_lon)
                if c["trip_total_dist"] > 0:
                    progress = 1 - (current_remaining_dist / c["trip_total_dist"])
                    realization_percent = round(max(0.0, min(100.0, progress * 100.0)), 2)
                if current_remaining_dist < (step_speed * 111.1):
                    c["current_lat"], c["current_lon"] = t_lat, t_lon
                    realization_percent = 100.0
                    if is_delivery:
                        c["status"] = "DELIVERED"
                        c["current_order"]["delivery_timestamp"] = now_iso
                    else:
                        c["status"] = "IDLE"
                        c["current_order"] = None
                        c["trip_total_dist"] = 0.0
                    force_log = True
                else:
                    angle = math.atan2(t_lat - c["current_lat"], t_lon - c["current_lon"])
                    c["current_lat"] += math.sin(angle) * step_speed
                    c["current_lon"] += math.cos(angle) * step_speed
                    c["log_counter"] += 1
            if c["status"] != "IDLE":
                if force_log or (c["log_counter"] >= LOG_THROTTLE):
                    ord_ref = c["current_order"]
                    order_time_dt = datetime.strptime(ord_ref["order_timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                    dist_from_base = calculate_dist_km(c["current_lat"], c["current_lon"], c["base_lat"], c["base_lon"])
                    zone_key, _ = get_zone_data(c["current_lat"], c["current_lon"], c["base_lat"], c["base_lon"])
                    
                    telemetry_event = {
                        "event_type": "telemetry",
                        "event_id": str(uuid.uuid4()),
                        "status_key": c["status"],
                        "order_id": ord_ref["order_id"],
                        "order_timestamp": ord_ref["order_timestamp"],
                        "ready_for_pickup_timestamp": ord_ref["ready_for_pickup_timestamp"],
                        "pickup_timestamp": ord_ref["pickup_timestamp"],
                        "delivery_timestamp": ord_ref["delivery_timestamp"],
                        "courier_id": c["courier_id"],
                        "timestamp": now_iso,
                        "lat": round(c["current_lat"], 6),
                        "lon": round(c["current_lon"], 6),
                        "speed": current_speed_kmh,
                        "realization_percent": realization_percent,
                        "customer_id": ord_ref["customer_id"],
                        "is_outside_geofence": dist_from_base > GEOFENCE_RADIUS_KM,
                        "zone_key": zone_key,
                        "is_delayed_delivery": (now_dt - order_time_dt) > timedelta(minutes=DELAY_THRESHOLD_MIN)
                    }
                    print(json.dumps(telemetry_event))
                    telemetry_buffer.append(telemetry_event)
                    
                    c["log_counter"] = 0
            if c["status"] == "DELIVERED":
                c["trip_total_dist"] = calculate_dist_km(c["current_lat"], c["current_lon"], c["base_lat"], c["base_lon"])
                c["status"] = "RETURNING"
                c["log_counter"] = 0

        if (time.time() - last_flush_time > 60) or (len(telemetry_buffer) > 100) or (len(orders_buffer) > 100):
            flush_buffers()

        time.sleep(STEP_DURATION)
except KeyboardInterrupt:
    flush_buffers()
    pass