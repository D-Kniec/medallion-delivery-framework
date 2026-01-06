import time
import random

courier_id = 1
base_lat = 52.2297
target_lat = 52.2500
step_duration = 0.5 
kmh_conversion_factor = 111.1 * (3600 / step_duration)

vehicle_type = "BICYCLE" if courier_id % 2 == 0 else "CAR"

if vehicle_type == "BICYCLE":
    speed_range = (1, 5)
    noise_range = (-0.1, 0.1)
else:
    speed_range = (4, 15)
    noise_range = (-4.0, 0.40)

current_lat = base_lat
status = "TO_DELIVERY"

print(f"Starting simulation for {vehicle_type} (ID: {courier_id})")

while True:

    if vehicle_type == "CAR" and status == "RETURNING":
        noise_range = (-4.0, 2.0)

    base_speed = random.uniform(*speed_range)
    step_speed = (base_speed + random.uniform(*noise_range)) / 100000

    if status == "TO_DELIVERY":
        if current_lat < target_lat:
            current_lat += step_speed
        else:
            print("Target reached!")
            status = "RETURNING"
            continue 
            
    elif status == "RETURNING":
        if current_lat > base_lat:
            current_lat -= step_speed
        else:
            print("Returned to base!")
            break 

    speed_kmh = step_speed * kmh_conversion_factor

    event = {
        "courier_id": courier_id,
        "vehicle": vehicle_type,
        "lat": round(current_lat, 6),
        "status": status,
        "step_speed": round(step_speed, 6),
        "speed_kmh": round(speed_kmh, 2) 
    }
    
    print(event)
    time.sleep(step_duration)