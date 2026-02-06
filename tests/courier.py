import json
import math
import uuid
from datetime import datetime, timedelta

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def get_next_position(current_lat, current_lon, target_lat, target_lon, speed_kmh, step_sec):
    dist_total = haversine(current_lat, current_lon, target_lat, target_lon)
    
    dist_step_km = (speed_kmh / 3600.0) * step_sec
    
    if dist_step_km >= dist_total:
        return target_lat, target_lon, True

    ratio = dist_step_km / dist_total
    new_lat = current_lat + (target_lat - current_lat) * ratio
    new_lon = current_lon + (target_lon - current_lon) * ratio
    
    return new_lat, new_lon, False

def log_telemetry(sim_time, courier_id, order_id, lat, lon, status, speed, dist_left, time_left):
    record = {
        "event_id": uuid.uuid4().hex,
        "timestamp": sim_time.isoformat(),
        "courier_id": courier_id,
        "order_id": order_id,
        "status": status,
        "lat": round(lat, 6),
        "lon": round(lon, 6),
        "speed_kmh": round(speed, 1),
        "distance_left_km": round(dist_left, 3),
        "time_left_sec": round(time_left, 2) if time_left is not None else None
    }
    print(json.dumps(record))

def generate_full_log_immediately():
    pizzeria_pos = (52.229676, 21.012229) 
    customer_pos = (52.242500, 21.035000) 
    
    courier_id = "C-FAST-01"
    order_id = "ORD-INSTANT-01"
    
    sim_time = datetime.now()
    step = timedelta(seconds=1)
    
    deadline = sim_time + timedelta(seconds=120) 
    
    curr_lat, curr_lon = pizzeria_pos
    total_dist = haversine(*pizzeria_pos, *customer_pos)
    
    for _ in range(3):
        log_telemetry(sim_time, courier_id, None, curr_lat, curr_lon, "IDLE", 0.0, 0.0, None)
        sim_time += step

    for _ in range(10):
        time_left = (deadline - sim_time).total_seconds()
        log_telemetry(sim_time, courier_id, order_id, curr_lat, curr_lon, "PREPARING", 0.0, total_dist, time_left)
        sim_time += step

    speed = 45.0 
    arrived = False
    
    while not arrived:
        time_left = (deadline - sim_time).total_seconds()
        dist_left = haversine(curr_lat, curr_lon, *customer_pos)
        
        log_telemetry(sim_time, courier_id, order_id, curr_lat, curr_lon, "IN_REALIZATION", speed, dist_left, time_left)
        
        curr_lat, curr_lon, arrived = get_next_position(curr_lat, curr_lon, *customer_pos, speed, 1.0)
        sim_time += step

    for _ in range(15):
        time_left = (deadline - sim_time).total_seconds()
        log_telemetry(sim_time, courier_id, order_id, curr_lat, curr_lon, "AT_CUSTOMER", 0.0, 0.0, time_left)
        sim_time += step

    final_margin = (deadline - sim_time).total_seconds()
    log_telemetry(sim_time, courier_id, order_id, curr_lat, curr_lon, "DELIVERED", 0.0, 0.0, final_margin)
    sim_time += step

    speed = 50.0 
    arrived_base = False
    
    while not arrived_base:
        log_telemetry(sim_time, courier_id, order_id, curr_lat, curr_lon, "RETURNING", speed, 0.0, final_margin)
        
        curr_lat, curr_lon, arrived_base = get_next_position(curr_lat, curr_lon, *pizzeria_pos, speed, 1.0)
        sim_time += step

    for _ in range(3):
        log_telemetry(sim_time, courier_id, None, curr_lat, curr_lon, "IDLE", 0.0, 0.0, None)
        sim_time += step

if __name__ == "__main__":
    generate_full_log_immediately()