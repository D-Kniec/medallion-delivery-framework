import json
import logging
import math
import os
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("PhysicsGenerator")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BASE_BRONZE_PATH = PROJECT_ROOT / 'data' / 'bronze'

@dataclass
class Config:
    sim_step_seconds: float = 3.0   
    real_sleep_seconds: float = 3.0
    flush_interval: int = 1         
    buffer_limit: int = 100
    sla_seconds: int = 120        
    speed_bike_ms: Tuple[float, float] = (5.0, 10.0) 
    speed_scooter_ms: Tuple[float, float] = (10.0, 18.0)

CONF = Config()

class ZonePhysics:
    @staticmethod
    def get_demand_factor(hour: int) -> float:
        if 0 <= hour < 10: return 0.30  
        if 10 <= hour < 23: return 0.98 
        return 0.50

class GeoMath:
    EARTH_RADIUS_KM = 6371.0
    GPS_NOISE_STD_DEV = 0.000015

    @staticmethod
    def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        
        a = (math.sin(dlat / 2) ** 2 +
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
        
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return GeoMath.EARTH_RADIUS_KM * c

    @staticmethod
    def apply_gps_jitter(lat: float, lon: float) -> Tuple[float, float]:
        noise_lat = random.gauss(0, GeoMath.GPS_NOISE_STD_DEV)
        noise_lon = random.gauss(0, GeoMath.GPS_NOISE_STD_DEV)
        return lat + noise_lat, lon + noise_lon

    @staticmethod
    def calculate_zone(lat: float, lon: float, base_lat: float, base_lon: float) -> int:
        dist = GeoMath.haversine(lat, lon, base_lat, base_lon)
        
        if dist < 1.5: return 1
        if dist < 3.0: return 2
        if dist < 5.0: return 3
        if dist < 8.0: return 4
        return 5

    @staticmethod
    def find_nearest_pizzeria(current_lat: float, current_lon: float, pizzerias_df: pd.DataFrame) -> Tuple[str, float, float]:
        lats = np.radians(pizzerias_df['lat'].values)
        lons = np.radians(pizzerias_df['lon'].values)
        
        curr_lat_rad = math.radians(current_lat)
        curr_lon_rad = math.radians(current_lon)

        dlat = lats - curr_lat_rad
        dlon = lons - curr_lon_rad

        a = np.sin(dlat / 2)**2 + np.cos(curr_lat_rad) * np.cos(lats) * np.sin(dlon / 2)**2
        c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
        
        distances = GeoMath.EARTH_RADIUS_KM * c
        
        min_idx = np.argmin(distances)
        
        nearest_row = pizzerias_df.iloc[min_idx]
        
        return nearest_row['pizzeria_id'], nearest_row['lat'], nearest_row['lon']

class CourierAgent:
    def __init__(self, profile: pd.Series, all_pizzerias: pd.DataFrame):
        self.id = profile['courier_id']
        self.vehicle = profile['vehicle_type'].upper()
        self.accident_risk = float(profile.get('accident_risk_factor', 0.001))
        self.max_range = float(profile.get('vehicle_range_km', 20.0))
        self.all_pizzerias = all_pizzerias
        
        self.base_pos = (float(profile['lat']), float(profile['lon']))
        self.current_pos = self.base_pos
        self.current_pizzeria_id = profile['pizzeria_id']
        
        self.status = "IDLE"
        self.order: Optional[Dict] = None
        
        self.total_trip_dist = 0.0
        self.traveled_dist_km = 0.0
        self.current_speed_ms = 0.0 
        self.service_time_ticks_left = 0
        
        if "BIKE" in self.vehicle:
            self.speed_range_ms = CONF.speed_bike_ms
        else:
            self.speed_range_ms = CONF.speed_scooter_ms

    def assign_order(self, order_data: Dict, customer_pos: Tuple[float, float], timestamp_obj: datetime) -> Dict:
            self.status = "PREPARING"
            self.current_pos = self.base_pos
            self.traveled_dist_km = 0.0
            self.current_speed_ms = (self.speed_range_ms[0] + self.speed_range_ms[1]) / 2.0
            
            prep_time = random.randint(15, 45)
            
            self.order = order_data
            self.order.update({
                'prep_ticks': int(prep_time / CONF.sim_step_seconds),
                'preparation_timestamp': (timestamp_obj + timedelta(seconds=prep_time)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                'deadline_ts': timestamp_obj + timedelta(seconds=CONF.sla_seconds)
            })
            
            self.total_trip_dist = GeoMath.haversine(*self.base_pos, *customer_pos)
            
            return self._generate_telemetry(timestamp_obj)
    
    def tick(self, current_dt: datetime) -> Optional[Dict]:
            if self.status in ("IDLE", "CRITICAL_FAILURE"):
                return None

            if self.status != "RETURNING" and random.random() < (self.accident_risk * 0.005):
                self.status = "CRITICAL_FAILURE"
                return self._generate_telemetry(current_dt)

            current_iso = current_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            if self.status == "PREPARING":
                self.order['prep_ticks'] -= 1
                if self.order['prep_ticks'] <= 0:
                    self.status = "IN_REALIZATION"
                    self.order['pickup_timestamp'] = current_iso
                return self._generate_telemetry(current_dt)

            if self.status == "IN_REALIZATION":
                return self._move_courier(current_dt, next_status="AT_CUSTOMER")

            if self.status == "AT_CUSTOMER":
                self.service_time_ticks_left -= 1
                self.current_speed_ms = 0.0
                
                if self.service_time_ticks_left <= 0:
                    self.status = "DELIVERED"
                    self.order['delivery_timestamp'] = current_iso
                    
                    new_pid, new_lat, new_lon = GeoMath.find_nearest_pizzeria(*self.current_pos, self.all_pizzerias)
                    self.current_pizzeria_id = new_pid
                    self.base_pos = (new_lat, new_lon)
                    self.total_trip_dist = GeoMath.haversine(*self.current_pos, *self.base_pos)
                    self.traveled_dist_km = 0.0
                    
                return self._generate_telemetry(current_dt)

            if self.status == "DELIVERED":
                self.status = "RETURNING"
                return self._generate_telemetry(current_dt)

            if self.status == "RETURNING":
                return self._move_courier(current_dt, next_status="IDLE")
                
            return None

    def _move_courier(self, current_dt: datetime, next_status: str) -> Optional[Dict]:
            min_speed, max_speed = self.speed_range_ms
            self.current_speed_ms = max(min_speed, min(max_speed, self.current_speed_ms + random.uniform(-1.5, 1.5)))
            
            step_dist_km = (self.current_speed_ms * CONF.sim_step_seconds) / 1000.0
            
            target_pos = (self.order['target_lat'], self.order['target_lon']) if self.status == "IN_REALIZATION" else self.base_pos
            dist_to_target = GeoMath.haversine(*self.current_pos, *target_pos)

            if dist_to_target > step_dist_km:
                ratio = step_dist_km / dist_to_target
                new_lat = self.current_pos[0] + (target_pos[0] - self.current_pos[0]) * ratio
                new_lon = self.current_pos[1] + (target_pos[1] - self.current_pos[1]) * ratio
                
                self.current_pos = (new_lat, new_lon)
                self.traveled_dist_km += step_dist_km
                return self._generate_telemetry(current_dt)

            self.current_pos = target_pos
            self.traveled_dist_km += dist_to_target
            self.status = next_status

            if next_status == "AT_CUSTOMER":
                service_seconds = random.randint(30, 60) if random.random() > 0.8 else random.randint(15, 30)
                self.service_time_ticks_left = int(service_seconds / CONF.sim_step_seconds)
                return self._generate_telemetry(current_dt)

            if next_status == "IDLE":
                self.order['return_timestamp'] = current_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                final_telemetry = self._generate_telemetry(current_dt)
                
                self.order = None
                self.traveled_dist_km = 0.0
                self.current_speed_ms = 0.0
                return final_telemetry
                
            return self._generate_telemetry(current_dt)

    def _generate_telemetry(self, current_dt: datetime) -> Dict:
            jitter_pos = GeoMath.apply_gps_jitter(*self.current_pos)
            dist_from_base = GeoMath.haversine(*jitter_pos, *self.base_pos)
            
            speed_kmh = round(self.current_speed_ms * 3.6, 1)
            if self.status in ("IDLE", "PREPARING", "AT_CUSTOMER", "CRITICAL_FAILURE"):
                speed_kmh = 0.0

            time_left_sec = 0.0
            distance_left_km = 0.0
            realization = 0.0
            is_delayed = False

            if self.order:
                deadline = self.order['deadline_ts']
                
                if self.status in ("DELIVERED", "RETURNING"):
                    delivery_ts = datetime.strptime(self.order['delivery_timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
                    time_left_sec = (deadline - delivery_ts).total_seconds()
                    is_delayed = delivery_ts > deadline
                    realization = 100.0
                else:
                    time_left_sec = (deadline - current_dt).total_seconds()
                    is_delayed = current_dt > deadline

                    if self.status == "IN_REALIZATION":
                        target_pos = (self.order['target_lat'], self.order['target_lon'])
                        distance_left_km = round(GeoMath.haversine(*self.current_pos, *target_pos), 3)
                        if self.total_trip_dist > 0:
                            realization = max(0.0, min(99.0, (1 - distance_left_km / self.total_trip_dist) * 100))
                    elif self.status == "PREPARING":
                        distance_left_km = round(self.total_trip_dist, 3)
                    elif self.status == "AT_CUSTOMER":
                        realization = 99.0

            return {
                "event_type": "telemetry",
                "event_id": uuid.uuid4().hex,
                "status_key": self.status,
                "courier_id": self.id,
                "pizzeria_id": self.current_pizzeria_id,
                "timestamp": current_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "lat": round(jitter_pos[0], 6),
                "lon": round(jitter_pos[1], 6),
                "speed": speed_kmh,
                "realization_percent": round(realization, 2),
                "zone_key": GeoMath.calculate_zone(*self.current_pos, *self.base_pos),
                "is_outside_geofence": dist_from_base > self.max_range,
                "is_delayed_delivery": is_delayed,
                "time_left_sec": round(time_left_sec, 2),
                "distance_left_km": distance_left_km,
                "total_distance": round(self.traveled_dist_km, 3), 
                "order_id": self.order.get('order_id') if self.order else None,
                "customer_id": self.order.get('customer_id') if self.order else None,
                "order_timestamp": self.order.get('order_timestamp') if self.order else None,
                "preparation_timestamp": self.order.get('preparation_timestamp') if self.order else None,
                "pickup_timestamp": self.order.get('pickup_timestamp') if self.order else None,
                "delivery_timestamp": self.order.get('delivery_timestamp') if self.order else None,
                "return_timestamp": self.order.get('return_timestamp') if self.order else None
            }

class DataManager:
    def __init__(self):
        self.buffers: Dict[str, List[Dict]] = {"telemetry": [], "orders": []}
        self.last_flush = time.time()

    def buffer_telemetry(self, record: Dict):
        if record:
            self.buffers["telemetry"].append(record)

    def buffer_order(self, record: Dict):
        if record:
            self.buffers["orders"].append(record)

    def flush_if_needed(self, force: bool = False):
        has_data = any(self.buffers.values())
        time_elapsed = (time.time() - self.last_flush) >= CONF.flush_interval

        if has_data and (force or time_elapsed):
            self._write_all()
            self.last_flush = time.time()

    def _write_all(self):
        dt_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        batch_suffix = f"{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"

        for dtype, buffer in self.buffers.items():
            if not buffer:
                continue

            try:
                path = BASE_BRONZE_PATH / dtype / f"dt={dt_str}"
                path.mkdir(parents=True, exist_ok=True)
                
                file_path = path / f"{dtype}_{batch_suffix}.json"
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(json.dumps(r) for r in buffer) + '\n')
                
                buffer.clear()
            except Exception as e:
                logger.error(f"Write failed for {dtype}: {e}")
class SimulationEngine:
    SEARCH_RADIUS = 0.025
    ORDERS_PER_TICK = 3
    CHANCE_MODIFIER = 0.50

    def __init__(self):
        self.data_mgr = DataManager()
        self.agents: List[CourierAgent] = []
        self.customers_df = pd.DataFrame()
        self.pizzerias_df = pd.DataFrame()
        self.running = True
        self.sim_clock = datetime.now(timezone.utc)
        self._setup_signals()

    def _setup_signals(self):
        signal.signal(signal.SIGINT, self._handle_exit)
        signal.signal(signal.SIGTERM, self._handle_exit)

    def load_resources(self):
        try:
            base = BASE_BRONZE_PATH
            self.customers_df = pd.read_csv(base / 'customers' / 'customers.csv')
            self.pizzerias_df = pd.read_csv(base / 'pizzerias' / 'pizzerias.csv')
            
            couriers = pd.read_csv(base / 'couriers' / 'couriers.csv')
            couriers['pizzeria_id'] = couriers['pizzeria_id'].astype(str)
            self.pizzerias_df['pizzeria_id'] = self.pizzerias_df['pizzeria_id'].astype(str)
            
            merged = couriers.merge(self.pizzerias_df, on='pizzeria_id')
            
            self.agents = [CourierAgent(row, self.pizzerias_df) for _, row in merged.iterrows()]
            logger.info(f"Initialized {len(self.agents)} agents.")
        except Exception as e:
            logger.critical(f"Resource loading error: {e}")
            sys.exit(1)

    def _find_nearby_customer(self, agent: CourierAgent) -> Optional[pd.Series]:
        lat, lon = agent.base_pos
        lat_min, lat_max = lat - self.SEARCH_RADIUS, lat + self.SEARCH_RADIUS
        lon_min, lon_max = lon - self.SEARCH_RADIUS, lon + self.SEARCH_RADIUS

        local_cust = self.customers_df[
            (self.customers_df['lat'].between(lat_min, lat_max)) & 
            (self.customers_df['lon'].between(lon_min, lon_max))
        ]

        if local_cust.empty:
            return None
        return local_cust.sample(1).iloc[0]

    def _create_order_payload(self, cust_row: pd.Series, agent: CourierAgent, now_iso: str) -> Tuple[Dict, Dict]:
        order_id = f"ORD-{random.randint(100000, 999999)}"
        gross_revenue = round(random.uniform(40, 150), 2)
        delivery_charge = round(random.uniform(8, 15), 2)

        order_meta = {
            "order_id": order_id,
            "customer_id": cust_row['customer_id'],
            "target_lat": cust_row['lat'],
            "target_lon": cust_row['lon'],
            "order_timestamp": now_iso,
            "gross_revenue": gross_revenue,
            "delivery_charge": delivery_charge
        }

        order_event = {
            "event_type": "order_created",
            "order_id": order_id,
            "customer_id": cust_row['customer_id'],
            "gross_revenue": gross_revenue,
            "delivery_charge": delivery_charge,
            "pizzeria_lat": agent.base_pos[0],
            "pizzeria_lon": agent.base_pos[1],
            "order_timestamp": now_iso
        }

        return order_meta, order_event

    def _attempt_order_creation(self, now_iso: str):
        demand_prob = ZonePhysics.get_demand_factor(self.sim_clock.hour)
        chance = demand_prob * self.CHANCE_MODIFIER

        for _ in range(self.ORDERS_PER_TICK):
            idle_agents = [a for a in self.agents if a.status == "IDLE"]
            if not idle_agents:
                break

            if random.random() >= chance:
                continue

            agent = random.choice(idle_agents)
            customer = self._find_nearby_customer(agent)

            if customer is None:
                continue

            order_meta, order_event = self._create_order_payload(customer, agent, now_iso)
            
            self.data_mgr.buffer_order(order_event)
            initial_telem = agent.assign_order(order_meta, (customer['lat'], customer['lon']), self.sim_clock)
            self.data_mgr.buffer_telemetry(initial_telem)

    def start_live_simulation(self):
        logger.info(f"Live simulation started (Step: {CONF.sim_step_seconds}s).")
        
        while self.running:
            self.sim_clock = datetime.now(timezone.utc)
            now_iso = self.sim_clock.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            self._attempt_order_creation(now_iso)

            for agent in self.agents:
                telemetry = agent.tick(self.sim_clock)
                self.data_mgr.buffer_telemetry(telemetry)

            self.data_mgr.flush_if_needed()

            self.sim_clock += timedelta(seconds=CONF.sim_step_seconds)
            time.sleep(CONF.real_sleep_seconds)

    def _handle_exit(self, signum, frame):
        logger.info("Shutdown signal received.")
        self.running = False
        self.data_mgr.flush_if_needed(force=True)
        sys.exit(0)

if __name__ == "__main__":

    sim = SimulationEngine()

    sim.load_resources()

    sim.start_live_simulation()