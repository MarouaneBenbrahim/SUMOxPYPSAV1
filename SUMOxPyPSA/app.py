#!/usr/bin/env python3
"""
SUMOxPyPSA 
Manhattan Grid Simulation with Ultra-Realistic Power Network Visualization and Smart EV Routing
"""

from flask import Flask, render_template
from flask_socketio import SocketIO, emit
import traci
import time
import threading
import os
import random
import math
import json
import numpy as np
from datetime import datetime
from config import *
from sumo_config import SUMO_COMMON_CONFIG, CITY_CONFIGS as SUMO_CITY_CONFIGS

# Import the ultra-realistic power network
from manhattan_power_network import ManhattanPowerNetworkRealistic
from traffic_power_integration import TrafficPowerCoupler

app = Flask(__name__, static_url_path='/static', static_folder='static')
app.config['SECRET_KEY'] = 'SUMOxPyPSA2024'
socketio = SocketIO(app, async_mode='threading', cors_allowed_origins="*")

# SUMO Configuration
SUMO_BINARY = os.path.join(SUMO_PATH, "bin/sumo")

# Global state
simulation_running = False
simulation_thread = None
stop_event = threading.Event()

# Power network
power_network = None
power_coupler = None

# Real-time metrics
metrics = {
    'vehicles': {'total': 0, 'evs': 0, 'charging': 0, 'moving': 0, 'stopped': 0},
    'power': {'total_mw': 0, 'ev_mw': 0, 'traffic_mw': 0, 'peak_mw': 0},
    'traffic_lights': {'total': 0, 'green': 0, 'yellow': 0, 'red': 0},
    'grid': {'efficiency': 0, 'load_factor': 0, 'renewable_percent': 0, 'violations': 0}
}

class ManhattanTrafficController:
    """Professional Manhattan traffic light controller with realistic patterns"""
    
    def __init__(self):
        self.lights = {}
        self.manhattan_bounds = {
            'lat_min': 40.700,
            'lat_max': 40.800,
            'lon_min': -74.020,
            'lon_max': -73.930
        }
        self.cycle_time = 0
        self.avenue_sync_offset = 0
        self.traffic_light_states = {}  # Store states for power network
        
    def initialize_manhattan_lights(self):
        """Initialize traffic lights with Manhattan-specific patterns"""
        try:
            all_tl_ids = traci.trafficlight.getIDList()
            manhattan_lights = []
            
            for tl_id in all_tl_ids:
                try:
                    controlled_lanes = traci.trafficlight.getControlledLanes(tl_id)
                    if controlled_lanes:
                        lane_shape = traci.lane.getShape(controlled_lanes[0])
                        if lane_shape:
                            pos = lane_shape[-1]
                            gps = traci.simulation.convertGeo(*pos)
                            
                            if (self.manhattan_bounds['lat_min'] <= gps[1] <= self.manhattan_bounds['lat_max'] and
                                self.manhattan_bounds['lon_min'] <= gps[0] <= self.manhattan_bounds['lon_max']):
                                manhattan_lights.append((tl_id, gps))
                except:
                    continue
            
            print(f"🚦 Found {len(manhattan_lights)} traffic lights in Manhattan")
            
            for i, (tl_id, gps) in enumerate(manhattan_lights):
                is_avenue = (i % 3 == 0)
                pattern = 'AVENUE' if is_avenue else 'STREET'
                
                if is_avenue:
                    offset = int((gps[1] - 40.700) * 1000) % 60
                else:
                    offset = random.randint(0, 30)
                
                self.lights[tl_id] = {
                    'pattern': pattern,
                    'phase': 0,
                    'timer': offset,
                    'green_time': 35 if is_avenue else 25,
                    'yellow_time': 3,
                    'all_red_time': 2,
                    'position': gps,
                    'state_history': []
                }
                
                initial_state = self._generate_manhattan_state(tl_id, 0)
                traci.trafficlight.setRedYellowGreenState(tl_id, initial_state)
                self.traffic_light_states[tl_id] = initial_state
            
            return True
            
        except Exception as e:
            print(f"Error initializing Manhattan lights: {e}")
            return False
    
    def _generate_manhattan_state(self, tl_id, phase):
        """Generate state based on Manhattan traffic patterns"""
        try:
            current_state = traci.trafficlight.getRedYellowGreenState(tl_id)
            num_signals = len(current_state)
            
            light_info = self.lights.get(tl_id, {})
            pattern = light_info.get('pattern', 'STREET')
            
            if pattern == 'AVENUE':
                if phase == 0:
                    state = 'GG' + 'r' * (num_signals - 2) if num_signals > 2 else 'GG'
                elif phase == 1:
                    state = 'yy' + 'r' * (num_signals - 2) if num_signals > 2 else 'yy'
                elif phase == 2:
                    state = 'r' * num_signals
                elif phase == 3:
                    state = 'r' * 2 + 'G' * (num_signals - 2) if num_signals > 2 else 'GG'
                elif phase == 4:
                    state = 'r' * 2 + 'y' * (num_signals - 2) if num_signals > 2 else 'yy'
                else:
                    state = 'r' * num_signals
            else:
                if phase == 0:
                    state = 'G' * (num_signals // 2) + 'r' * (num_signals - num_signals // 2)
                elif phase == 1:
                    state = 'y' * (num_signals // 2) + 'r' * (num_signals - num_signals // 2)
                elif phase == 2:
                    state = 'r' * num_signals
                elif phase == 3:
                    state = 'r' * (num_signals // 2) + 'G' * (num_signals - num_signals // 2)
                elif phase == 4:
                    state = 'r' * (num_signals // 2) + 'y' * (num_signals - num_signals // 2)
                else:
                    state = 'r' * num_signals
            
            if len(state) != num_signals:
                state = state[:num_signals] if len(state) > num_signals else state + 'r' * (num_signals - len(state))
            
            return state
            
        except:
            return 'rrrr'
    
    def update_cycle(self):
        """Update all traffic lights with Manhattan logic"""
        self.cycle_time += 1
        
        green_count = yellow_count = red_count = 0
        
        for tl_id, light_data in self.lights.items():
            try:
                light_data['timer'] += 1
                
                if light_data['phase'] in [0, 3]:
                    duration = light_data['green_time']
                elif light_data['phase'] in [1, 4]:
                    duration = light_data['yellow_time']
                else:
                    duration = light_data['all_red_time']
                
                if light_data['timer'] >= duration:
                    light_data['phase'] = (light_data['phase'] + 1) % 6
                    light_data['timer'] = 0
                    
                    new_state = self._generate_manhattan_state(tl_id, light_data['phase'])
                    traci.trafficlight.setRedYellowGreenState(tl_id, new_state)
                    self.traffic_light_states[tl_id] = new_state
                    
                    light_data['state_history'].append(new_state)
                    if len(light_data['state_history']) > 10:
                        light_data['state_history'].pop(0)
                
                current_state = traci.trafficlight.getRedYellowGreenState(tl_id)
                if 'G' in current_state or 'g' in current_state:
                    green_count += 1
                elif 'y' in current_state or 'Y' in current_state:
                    yellow_count += 1
                else:
                    red_count += 1
                    
            except:
                continue
        
        metrics['traffic_lights']['green'] = green_count
        metrics['traffic_lights']['yellow'] = yellow_count
        metrics['traffic_lights']['red'] = red_count
    
    def get_traffic_light_states(self):
        """Get current traffic light states for power network"""
        return self.traffic_light_states

class ManhattanEVNetwork:
    """EV charging network with smart routing"""
    
    def __init__(self):
        self.stations = []
        self.charging_sessions = {}
        self.charging_vehicles = {}  # Track which vehicles are charging at which station
        self.total_energy_delivered = 0
        self.peak_demand = 0
        self.ev_share_percent = 30
        self.ev_charging_bias_percent = 30
        self.ev_vehicles = {}  # Track EV vehicles
        
    def create_manhattan_grid_stations(self, traffic_light_positions):
        """Create EV stations WITHIN the traffic light grid area"""
        if traffic_light_positions and len(traffic_light_positions) > 0:
            sorted_lights = sorted(traffic_light_positions, key=lambda x: (x[1], x[0]))
            
            num_stations = min(12, len(sorted_lights) // 5)
            step = len(sorted_lights) // num_stations if num_stations > 0 else 1
            
            station_names = [
                'Times Square Supercharger', 'Penn Station Hub', 'Grand Central Station',
                'Columbus Circle', 'Lincoln Center Station', 'Union Square Station',
                'Washington Square', 'Financial District', 'UN Plaza Station',
                'Chelsea Market', 'Hudson Yards', 'Central Park South'
            ]
            
            self.stations = []
            for i in range(0, min(len(sorted_lights), num_stations * step), step):
                if len(self.stations) >= 12:
                    break
                    
                light_pos = sorted_lights[i]
                station_lat = light_pos[1] + random.uniform(-0.0005, 0.0005)
                station_lon = light_pos[0] + random.uniform(-0.0005, 0.0005)
                
                station_lat = max(40.700, min(40.800, station_lat))
                station_lon = max(-74.020, min(-73.930, station_lon))
                
                self.stations.append({
                    'id': f'ev_station_{len(self.stations)}',
                    'lat': station_lat,
                    'lon': station_lon,
                    'name': station_names[len(self.stations)] if len(self.stations) < len(station_names) else f'Station {len(self.stations)}',
                    'power': random.choice([150, 250, 350]),
                    'capacity': random.randint(6, 12),
                    'street': f'Near intersection {i}',
                    'vehicles_charging': []
                })
        else:
            # Fallback stations
            self.stations = []
            lat_step = (40.800 - 40.700) / 4
            lon_step = (-73.930 - (-74.020)) / 3
            
            station_names = [
                'Times Square Supercharger', 'Penn Station Hub', 'Grand Central Station',
                'Columbus Circle', 'Lincoln Center Station', 'Union Square Station',
                'Washington Square', 'Financial District', 'UN Plaza Station',
                'Chelsea Market', 'Hudson Yards', 'Central Park South'
            ]
            
            idx = 0
            for lat_i in range(4):
                for lon_i in range(3):
                    if idx >= 12:
                        break
                    
                    station_lat = 40.710 + lat_i * lat_step
                    station_lon = -74.010 + lon_i * lon_step
                    
                    self.stations.append({
                        'id': f'ev_station_{idx}',
                        'lat': station_lat,
                        'lon': station_lon,
                        'name': station_names[idx] if idx < len(station_names) else f'Station {idx}',
                        'power': random.choice([150, 250, 350]),
                        'capacity': random.randint(6, 12),
                        'street': f'Grid location {lat_i}-{lon_i}',
                        'vehicles_charging': []
                    })
                    idx += 1
        
        for station in self.stations:
            self.charging_sessions[station['id']] = {}
        
        print(f"⚡ Created {len(self.stations)} EV stations within Manhattan traffic grid")
        return self.stations
    
    def route_ev_to_station(self, vehicle_id, vehicle_pos):
        """Route an EV to the nearest available charging station"""
        try:
            # Find nearest available station
            best_station = None
            min_distance = float('inf')
            
            for station in self.stations:
                # Check if station has capacity
                if len(station['vehicles_charging']) < station['capacity']:
                    # Calculate distance
                    distance = math.sqrt((vehicle_pos[0] - station['lon'])**2 + 
                                       (vehicle_pos[1] - station['lat'])**2)
                    
                    if distance < min_distance:
                        min_distance = distance
                        best_station = station
            
            if best_station and min_distance < 0.01:  # Within reasonable distance
                # Convert GPS to SUMO coordinates
                sumo_pos = traci.simulation.convertGeo(best_station['lon'], best_station['lat'], True)
                
                # Get nearest edge to station
                edges = traci.edge.getIDList()
                if edges:
                    # Set new route to charging station
                    nearest_edge = traci.simulation.convertRoad(sumo_pos[0], sumo_pos[1])[0]
                    if nearest_edge:
                        current_edge = traci.vehicle.getRoadID(vehicle_id)
                        if current_edge and current_edge != nearest_edge:
                            try:
                                # Calculate route to charging station
                                route = traci.simulation.findRoute(current_edge, nearest_edge)
                                if route and route.edges:
                                    traci.vehicle.setRoute(vehicle_id, list(route.edges))
                                    return best_station
                            except:
                                pass
        except Exception as e:
            pass
        
        return None
    
    def process_ev_charging(self, vehicles):
        """Process EV charging with smart routing"""
        self.charging_vehicles = {}  # Reset each update
        total_evs = 0
        charging_count = 0
        
        try:
            share = int(self.ev_share_percent)
        except Exception:
            share = 30
        share = max(0, min(100, share))
        
        try:
            bias = int(self.ev_charging_bias_percent)
        except Exception:
            bias = 30
        bias = max(0, min(100, bias))
        
        # Dynamic capture radius
        base_radius = 0.002
        max_extra = 0.006
        capture_radius = base_radius + (bias / 100.0) * max_extra
        
        current_tick = int(time.time())
        
        for vehicle in vehicles:
            vid = vehicle['id']
            is_ev = (hash(vid) % 100) < share
            vehicle['is_ev'] = bool(is_ev)
            
            if not is_ev:
                continue
            
            total_evs += 1
            
            # Check if EV needs charging
            if vid not in self.ev_vehicles:
                self.ev_vehicles[vid] = {
                    'battery': random.uniform(20, 80),  # Battery percentage
                    'charging': False,
                    'target_station': None
                }
            
            ev_data = self.ev_vehicles[vid]
            speed = float(vehicle.get('speed', 0) or 0)
            
            # Decide if vehicle needs to charge
            needs_charging = ev_data['battery'] < 30 or (ev_data['battery'] < 50 and random.random() < bias/100)
            
            if needs_charging and not ev_data['charging']:
                # Route to nearest station
                station = self.route_ev_to_station(vid, (vehicle['x'], vehicle['y']))
                if station:
                    ev_data['target_station'] = station['id']
            
            # Check if at charging station
            for station in self.stations:
                lat_diff = abs(vehicle['y'] - station['lat'])
                lon_diff = abs(vehicle['x'] - station['lon'])
                
                if lat_diff < capture_radius and lon_diff < capture_radius:
                    if station['id'] not in self.charging_vehicles:
                        self.charging_vehicles[station['id']] = []
                    
                    if len(self.charging_vehicles[station['id']]) < station['capacity']:
                        if speed < 2.0:  # Vehicle is stopped/slow
                            self.charging_vehicles[station['id']].append(vid)
                            charging_count += 1
                            ev_data['charging'] = True
                            vehicle['charging'] = True
                            
                            # Update battery
                            ev_data['battery'] = min(100, ev_data['battery'] + 0.5)
                            
                            if vid not in self.charging_sessions[station['id']]:
                                self.charging_sessions[station['id']][vid] = {
                                    'start': time.time(),
                                    'energy_kwh': 0
                                }
                            
                            session = self.charging_sessions[station['id']][vid]
                            duration_hours = (time.time() - session['start']) / 3600
                            session['energy_kwh'] = min(station['power'] * duration_hours, 100)
                            
                            # If fully charged, leave
                            if ev_data['battery'] >= 95:
                                ev_data['charging'] = False
                                ev_data['target_station'] = None
                                vehicle['charging'] = False
                            
                            break
        
        # Update station occupancy
        for station in self.stations:
            station['vehicles_charging'] = self.charging_vehicles.get(station['id'], [])
        
        return total_evs, charging_count, self.charging_vehicles

class PowerGridManager:
    """Advanced power grid management for Manhattan with ultra-realistic network"""
    
    def __init__(self):
        self.network = None
        self.history = []
        self.peak_demand = 0
        self.total_energy = 0
        
    def initialize_nyc_grid(self):
        """Initialize NYC power grid with ultra-realistic network"""
        print("⚡ Initializing Ultra-Realistic Manhattan Power Grid...")
        self.network = ManhattanPowerNetworkRealistic()
        self.network.build_network()
        
        print(f"✅ NYC Power Grid initialized with {len(self.network.buses)} buses")
        print(f"   {len(self.network.lines)} lines, {len(self.network.transformers)} transformers")
        print(f"   {len(self.network.generators)} generators")
        return self.network
    
    def get_power_network_data(self):
        """Get comprehensive power network data for visualization"""
        if not self.network:
            return None
        
        # Use the new comprehensive network data method
        return self.network.get_network_data()
    
    def calculate_real_time_load(self, traffic_data, ev_data, traffic_light_states):
        """Calculate real-time power load using realistic network"""
        # Update traffic loads in the network
        self.network.update_traffic_loads(
            traffic_data['vehicle_count'],
            traffic_light_states,
            ev_data.get('charging_vehicles', {})
        )
        
        # Run power flow simulation
        self.network.simulate_power_flow()
        
        # Get status
        status = self.network.get_status()
        
        # Update history
        self.history.append(status['total_load_mw'])
        if len(self.history) > 100:
            self.history.pop(0)
        
        # Update peak demand
        if status['total_load_mw'] > self.peak_demand:
            self.peak_demand = status['total_load_mw']
        
        self.total_energy += status['total_load_mw'] / 3600
        
        # Return formatted data for frontend
        return {
            'total_load_mw': status['total_load_mw'],
            'base_load_mw': status['total_load_mw'] - status['traffic_light_load_mw'] - status['street_light_load_mw'] - status['ev_charging_load_mw'],
            'traffic_infrastructure_mw': status['traffic_light_load_mw'] + status['street_light_load_mw'],
            'ev_charging_mw': status['ev_charging_load_mw'],
            'traffic_systems_mw': status['traffic_light_load_mw'],
            'line_utilization': status['line_utilization'],
            'load_factor': (status['total_load_mw'] / self.peak_demand * 100) if self.peak_demand > 0 else 0,
            'renewable_percent': status['renewable_percent'],
            'peak_demand_mw': self.peak_demand,
            'total_energy_mwh': self.total_energy,
            'trend': self._calculate_trend(),
            'violations': status.get('violations', {'thermal': 0, 'voltage': 0})
        }
    
    def _calculate_trend(self):
        """Calculate load trend"""
        if len(self.history) < 10:
            return 'stable'
        
        recent = sum(self.history[-5:]) / 5
        older = sum(self.history[-10:-5]) / 5
        
        if recent > older * 1.05:
            return 'increasing'
        elif recent < older * 0.95:
            return 'decreasing'
        else:
            return 'stable'

# Initialize systems
traffic_controller = ManhattanTrafficController()
ev_network = ManhattanEVNetwork()
power_grid = PowerGridManager()

def create_manhattan_sumocfg(city):
    """Create SUMO config optimized for Manhattan"""
    city_dir = CITY_CONFIGS[city]["working_dir"]
    city_sumo_config = SUMO_CITY_CONFIGS[city.upper()]
    
    temp_path = os.path.join(city_dir, "manhattan.sumocfg")
    
    with open(temp_path, 'w') as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<configuration>\n')
        
        f.write('    <input>\n')
        f.write(f'        <net-file value="{os.path.basename(city_sumo_config["net-file"])}"/>\n')
        f.write(f'        <route-files value="{os.path.basename(city_sumo_config["route-files"])}"/>\n')
        
        poly_file = "osm.poly.xml.gz"
        if os.path.exists(os.path.join(city_dir, poly_file)):
            f.write(f'        <additional-files value="{poly_file}"/>\n')
        
        f.write('    </input>\n')
        
        f.write('    <processing>\n')
        f.write('        <ignore-route-errors value="true"/>\n')
        f.write('        <time-to-teleport value="300"/>\n')
        f.write('        <max-depart-delay value="900"/>\n')
        f.write('        <routing-algorithm value="dijkstra"/>\n')
        f.write('        <device.rerouting.probability value="0.5"/>\n')
        f.write('        <device.rerouting.period value="60"/>\n')
        f.write('        <scale value="0.3"/>\n')
        f.write('        <lateral-resolution value="0.8"/>\n')
        f.write('    </processing>\n')
        
        f.write('    <time>\n')
        f.write('        <begin value="0"/>\n')
        f.write('        <step-length value="0.1"/>\n')
        f.write('    </time>\n')
        
        f.write('</configuration>\n')
    
    return temp_path

def get_manhattan_vehicles():
    """Get vehicles in Manhattan area only"""
    manhattan_vehicles = []
    
    for vid in traci.vehicle.getIDList():
        try:
            pos = traci.vehicle.getPosition(vid)
            gps = traci.simulation.convertGeo(*pos)
            
            if (40.700 <= gps[1] <= 40.800 and -74.020 <= gps[0] <= -73.930):
                manhattan_vehicles.append({
                    'id': vid,
                    'x': gps[0],
                    'y': gps[1],
                    'angle': traci.vehicle.getAngle(vid),
                    'speed': traci.vehicle.getSpeed(vid),
                    'type': traci.vehicle.getTypeID(vid),
                    'is_ev': False,
                    'charging': False
                })
        except:
            continue
    
    return manhattan_vehicles

def get_manhattan_traffic_lights():
    """Get traffic lights in Manhattan with proper states"""
    lights = []
    
    for tl_id in traffic_controller.lights:
        try:
            light_data = traffic_controller.lights[tl_id]
            state = traci.trafficlight.getRedYellowGreenState(tl_id)
            
            if 'G' in state or 'g' in state:
                color = 'green'
            elif 'y' in state or 'Y' in state:
                color = 'yellow'
            else:
                color = 'red'
            
            lights.append({
                'id': tl_id,
                'x': light_data['position'][0],
                'y': light_data['position'][1],
                'state': state,
                'color': color,
                'pattern': light_data['pattern']
            })
        except:
            continue
    
    return lights

def prepare_ev_station_data(charging_vehicles):
    """Prepare EV station data for frontend"""
    station_data = []
    total_power_mw = 0
    ev_charging_data = {}  # For power network update
    
    for station in ev_network.stations:
        vehicles_at_station = charging_vehicles.get(station['id'], [])
        num_charging = len(vehicles_at_station)
        utilization = (num_charging / station['capacity']) * 100 if station['capacity'] > 0 else 0
        power_output_mw = (num_charging * station['power']) / 1000
        
        total_power_mw += power_output_mw
        
        # Prepare data for power network
        ev_charging_data[station['id']] = vehicles_at_station
        
        station_data.append({
            'id': station['id'],
            'lat': station['lat'],
            'lon': station['lon'],
            'name': station['name'],
            'street': station['street'],
            'power': station['power'],
            'capacity': station['capacity'],
            'evs_charging': num_charging,
            'utilization': utilization,
            'power_output_mw': power_output_mw,
            'status': 'busy' if utilization > 80 else 'available',
            'vehicles_charging': vehicles_at_station
        })
    
    return station_data, total_power_mw, ev_charging_data

def manhattan_simulation():
    """Main Manhattan simulation loop with ultra-realistic power network"""
    global simulation_running, metrics
    
    print("🏙️ Starting Manhattan Grid Simulation with Ultra-Realistic Power Network")
    print("📍 Area: 40.70°N to 40.80°N, -74.02°W to -73.93°W")
    print("⚡ Power Network: 138kV/27kV/13.8kV/4.16kV Multi-Level Grid")
    
    city_config = CITY_CONFIGS["newyork"]
    working_dir = city_config["working_dir"]
    
    original_dir = os.getcwd()
    temp_cfg = None
    
    try:
        os.chdir(working_dir)
        temp_cfg = create_manhattan_sumocfg("newyork")
        
        simulation_running = True
        stop_event.clear()
        
        sumo_cmd = [SUMO_BINARY, "-c", os.path.basename(temp_cfg)]
        traci.start(sumo_cmd)
        print("✅ SUMO started successfully")
        
        traffic_controller.initialize_manhattan_lights()
        
        traffic_light_positions = []
        for tl_id, light_data in traffic_controller.lights.items():
            traffic_light_positions.append(light_data['position'])
        
        ev_network.create_manhattan_grid_stations(traffic_light_positions)
        power_grid.initialize_nyc_grid()
        
        step_counter = 0
        last_update_time = time.time()
        
        while traci.simulation.getMinExpectedNumber() > 0 and not stop_event.is_set():
            traci.simulationStep()
            step_counter += 1
            
            if step_counter % 10 == 0:
                traffic_controller.update_cycle()
            
            if step_counter % 5 == 0:
                current_time = time.time()
                
                if current_time - last_update_time >= 0.1:
                    vehicles = get_manhattan_vehicles()
                    metrics['vehicles']['total'] = len(vehicles)
                    
                    total_evs, charging_evs, charging_vehicles = ev_network.process_ev_charging(vehicles)
                    metrics['vehicles']['evs'] = total_evs
                    metrics['vehicles']['charging'] = charging_evs
                    
                    traffic_lights = get_manhattan_traffic_lights()
                    metrics['traffic_lights']['total'] = len(traffic_lights)
                    
                    ev_stations, total_ev_power_mw, ev_charging_data = prepare_ev_station_data(charging_vehicles)
                    
                    # Get traffic light states for power network
                    traffic_light_states = traffic_controller.get_traffic_light_states()
                    
                    traffic_data = {
                        'lights_count': len(traffic_lights),
                        'vehicle_count': len(vehicles)
                    }
                    ev_data = {
                        'total_power_mw': total_ev_power_mw,
                        'charging_vehicles': ev_charging_data
                    }
                    
                    # Calculate power with ultra-realistic network
                    power_data = power_grid.calculate_real_time_load(traffic_data, ev_data, traffic_light_states)
                    
                    metrics['power']['total_mw'] = power_data['total_load_mw']
                    metrics['power']['ev_mw'] = power_data['ev_charging_mw']
                    metrics['power']['traffic_mw'] = power_data['traffic_infrastructure_mw']
                    metrics['grid']['load_factor'] = power_data['load_factor']
                    metrics['grid']['renewable_percent'] = power_data['renewable_percent']
                    metrics['grid']['violations'] = power_data['violations']['thermal'] + power_data['violations']['voltage']
                    
                    # Get comprehensive power network data for visualization
                    power_network_data = power_grid.get_power_network_data()
                    
                    if step_counter % 100 == 0:
                        print(f"\n📊 Step {step_counter} | Time: {traci.simulation.getTime():.1f}s")
                        print(f"  🚗 Vehicles: {len(vehicles)} in Manhattan ({total_evs} EVs)")
                        print(f"  ⚡ Charging: {charging_evs} EVs at stations")
                        print(f"  🚦 Lights: {metrics['traffic_lights']['green']}G/{metrics['traffic_lights']['yellow']}Y/{metrics['traffic_lights']['red']}R")
                        print(f"  💡 Power: {power_data['total_load_mw']:.1f} MW (EV: {power_data['ev_charging_mw']:.3f} MW)")
                        print(f"  📊 Grid: {len(power_network_data['buses'])} buses, {len(power_network_data['lines'])} lines")
                        print(f"  ⚠️ Violations: {power_data['violations']['thermal']} thermal, {power_data['violations']['voltage']} voltage")
                    
                    socketio.emit('update', {
                        'vehicles': vehicles,
                        'traffic_lights': traffic_lights,
                        'ev_stations': ev_stations,
                        'power': power_data,
                        'power_network': power_network_data,  # Comprehensive network data
                        'metrics': metrics,
                        'simulation_time': traci.simulation.getTime(),
                        'timestamp': datetime.now().isoformat()
                    })
                    
                    last_update_time = current_time
            
            time.sleep(SIMULATION_SPEED)
        
        traci.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if temp_cfg and os.path.exists(temp_cfg):
            os.unlink(temp_cfg)
        os.chdir(original_dir)
        simulation_running = False

@socketio.on('connect')
def handle_connect():
    print("✅ Client connected")
    emit('system_ready', {
        'message': 'Connected to Manhattan Grid System with Ultra-Realistic Power Network',
        'features': [
            'Traffic Simulation', 
            'EV Charging Network', 
            'Ultra-Realistic Power Grid (138kV/27kV/13.8kV/4.16kV)',
            'Real-time Metrics',
            'Power Flow Analysis',
            'Violation Detection'
        ]
    })

@socketio.on('start_simulation')
def handle_start():
    global simulation_thread
    
    if not simulation_running:
        print("🚀 Starting Manhattan simulation with ultra-realistic power network...")
        simulation_thread = threading.Thread(target=manhattan_simulation)
        simulation_thread.start()

@socketio.on('restart_simulation')
def handle_restart():
    global simulation_thread
    
    print("🔄 Restarting simulation...")
    
    if simulation_running:
        stop_event.set()
        if simulation_thread:
            simulation_thread.join(timeout=2)
    
    for key in metrics:
        if isinstance(metrics[key], dict):
            for subkey in metrics[key]:
                metrics[key][subkey] = 0
    
    simulation_thread = threading.Thread(target=manhattan_simulation)
    simulation_thread.start()

@socketio.on('set_ev_percentage')
def handle_set_ev_percentage(data):
    try:
        percent = int(data.get('percent', 30))
        percent = max(0, min(100, percent))
        ev_network.ev_share_percent = percent
        print(f"🔧 EV share set to {percent}%")
        emit('ev_percentage_updated', {'percent': percent})
    except Exception as e:
        print(f"Error setting EV percentage: {e}")

@socketio.on('set_ev_charging_bias')
def handle_set_ev_charging_bias(data):
    try:
        percent = int(data.get('percent', 30))
        percent = max(0, min(100, percent))
        ev_network.ev_charging_bias_percent = percent
        print(f"🔧 EV charging propensity set to {percent}%")
        emit('ev_charging_bias_updated', {'percent': percent})
    except Exception as e:
        print(f"Error setting EV charging bias: {e}")

@socketio.on('request_power_update')
def handle_request_power_update():
    """Send latest power network data on request"""
    if power_grid and power_grid.network:
        power_network_data = power_grid.get_power_network_data()
        emit('power_network_update', {'power_network': power_network_data})

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == "__main__":
    print("=" * 80)
    print("🏙️  SUMOxPyPSA MANHATTAN GRID SYSTEM")
    print("⚡ ULTRA-REALISTIC POWER NETWORK")
    print("=" * 80)
    print("📍 Focus: Manhattan (40.70-40.80°N, -74.02--73.93°W)")
    print("🚦 Traffic: Realistic Manhattan grid patterns")
    print("⚡ Power: Ultra-realistic multi-voltage NYC grid")
    print("   - 138kV Transmission Network")
    print("   - 27kV Subtransmission Network")
    print("   - 13.8kV Primary Distribution")
    print("   - 4.16kV Secondary Networks")
    print("🔌 EV Network: 12+ charging stations with smart routing")
    print("📊 Features: Real-time power flow, violation detection, renewable integration")
    print("=" * 80)
    print(f"🌐 Server: http://{HOST}:{PORT}")
    print("=" * 80)
    
    socketio.run(app, debug=True, host=HOST, port=PORT)