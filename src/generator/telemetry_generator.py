from datetime import datetime, timezone
import random
import string
import time
events_history = []
order_history = []
current_id = 1

while True:
    numbers = f"{random.randint(0, 99999):03}"
    if numbers not in order_history:
        courier_id = f"{random.randint(0, 10):03}"
        new_order = f"{numbers}-k-{courier_id}"
        order_history.append(numbers)

        new_event = {
            "event_id": current_id,
            "order_id": new_order,
            "courier_id":courier_id,
            "timestamp":datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        }
    events_history.append(new_event)
    current_id = current_id + 1
    print(events_history)
    time.sleep(3)