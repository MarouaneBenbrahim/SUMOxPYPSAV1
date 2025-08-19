#!/usr/bin/env python3
"""
SUMOxPyPSA 
Perfect NYC Manhattan Grid Simulation with Real-Time Power Integration
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

# Import power components
from pypsa_network_builder import NYCPowerNetworkSimple
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
    'grid': {'efficiency': 0, 'load_factor': 0, 'renewable_percent': 0}
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
        self.avenue_sync_offset = 0  # For green wave on avenues
        
    def initialize_manhattan_lights(self):
        """Initialize traffic lights with Manhattan-specific patterns"""
        try:
            all_tl_ids = traci.trafficlight.getIDList()
            manhattan_lights = []
            
            # Filter for Manhattan area lights only
            for tl_id in all_tl_ids:
                try:
                    # Get position
                    controlled_lanes = traci.trafficlight.getControlledLanes(tl_id)
                    if controlled_lanes:
                        lane_shape = traci.lane.getShape(controlled_lanes[0])
                        if lane_shape:
                            pos = lane_shape[-1]
                            gps = traci.simulation.convertGeo(*pos)
                            
                            # Check if in Manhattan
                            if (self.manhattan_bounds['lat_min'] <= gps[1] <= self.manhattan_bounds['lat_max'] and
                                self.manhattan_bounds['lon_min'] <= gps[0] <= self.manhattan_bounds['lon_max']):
                                manhattan_lights.append((tl_id, gps))
                except:
                    continue
            
            print(f"🚦 Found {len(manhattan_lights)} traffic lights in Manhattan")
            
            # Initialize each light with proper pattern
            for i, (tl_id, gps) in enumerate(manhattan_lights):
                # Determine if on avenue (north-south) or street (east-west)
                is_avenue = (i % 3 == 0)  # Simplified - every 3rd light is avenue
                
                # Set pattern based on location
                pattern = 'AVENUE' if is_avenue else 'STREET'
                
                # Calculate offset for green wave
                if is_avenue:
                    # Progressive timing for avenues (green wave)
                    offset = int((gps[1] - 40.700) * 1000) % 60
                else:
                    # Streets get different timing
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
                
                # Set initial state
                initial_state = self._generate_manhattan_state(tl_id, 0)
                traci.trafficlight.setRedYellowGreenState(tl_id, initial_state)
            
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
                # Avenue pattern - prioritize north-south
                if phase == 0:  # NS Green
                    state = 'GG' + 'r' * (num_signals - 2) if num_signals > 2 else 'GG'
                elif phase == 1:  # NS Yellow
                    state = 'yy' + 'r' * (num_signals - 2) if num_signals > 2 else 'yy'
                elif phase == 2:  # All red
                    state = 'r' * num_signals
                elif phase == 3:  # EW Green
                    state = 'r' * 2 + 'G' * (num_signals - 2) if num_signals > 2 else 'GG'
                elif phase == 4:  # EW Yellow
                    state = 'r' * 2 + 'y' * (num_signals - 2) if num_signals > 2 else 'yy'
                else:  # All red
                    state = 'r' * num_signals
            else:
                # Street pattern - balanced timing
                if phase == 0:  # First direction green
                    state = 'G' * (num_signals // 2) + 'r' * (num_signals - num_signals // 2)
                elif phase == 1:  # First direction yellow
                    state = 'y' * (num_signals // 2) + 'r' * (num_signals - num_signals // 2)
                elif phase == 2:  # All red
                    state = 'r' * num_signals
                elif phase == 3:  # Second direction green
                    state = 'r' * (num_signals // 2) + 'G' * (num_signals - num_signals // 2)
                elif phase == 4:  # Second direction yellow
                    state = 'r' * (num_signals // 2) + 'y' * (num_signals - num_signals // 2)
                else:  # All red
                    state = 'r' * num_signals
            
            # Ensure state length matches
            if len(state) != num_signals:
                state = state[:num_signals] if len(state) > num_signals else state + 'r' * (num_signals - len(state))
            
            return state
            
        except:
            return 'rrrr'  # Safe default
    
    def update_cycle(self):
        """Update all traffic lights with Manhattan logic"""
        self.cycle_time += 1
        
        green_count = yellow_count = red_count = 0
        
        for tl_id, light_data in self.lights.items():
            try:
                light_data['timer'] += 1
                
                # Determine current phase duration
                if light_data['phase'] in [0, 3]:  # Green phases
                    duration = light_data['green_time']
                elif light_data['phase'] in [1, 4]:  # Yellow phases
                    duration = light_data['yellow_time']
                else:  # Red phases
                    duration = light_data['all_red_time']
                
                # Check for phase change
                if light_data['timer'] >= duration:
                    # Advance phase
                    light_data['phase'] = (light_data['phase'] + 1) % 6
                    light_data['timer'] = 0
                    
                    # Generate new state
                    new_state = self._generate_manhattan_state(tl_id, light_data['phase'])
                    traci.trafficlight.setRedYellowGreenState(tl_id, new_state)
                    
                    # Track state
                    light_data['state_history'].append(new_state)
                    if len(light_data['state_history']) > 10:
                        light_data['state_history'].pop(0)
                
                # Count states
                current_state = traci.trafficlight.getRedYellowGreenState(tl_id)
                if 'G' in current_state or 'g' in current_state:
                    green_count += 1
                elif 'y' in current_state or 'Y' in current_state:
                    yellow_count += 1
                else:
                    red_count += 1
                    
            except:
                continue
        
        # Update metrics
        metrics['traffic_lights']['green'] = green_count
        metrics['traffic_lights']['yellow'] = yellow_count
        metrics['traffic_lights']['red'] = red_count

class ManhattanEVNetwork:
    """EV charging network for Manhattan with realistic placement"""
    
    def __init__(self):
        self.stations = []
        self.charging_sessions = {}
        self.total_energy_delivered = 0
        self.peak_demand = 0
        
    def create_manhattan_grid_stations(self):
        """Create EV stations on Manhattan street grid"""
        # Major stations at key intersections
        self.stations = [
            # Midtown stations
            {'id': 'ev_times_square', 'lat': 40.7580, 'lon': -73.9855,
             'name': 'Times Square Supercharger', 'power': 350, 'capacity': 12,
             'street': '42nd St & Broadway'},
            
            {'id': 'ev_penn_station', 'lat': 40.7505, 'lon': -73.9934,
             'name': 'Penn Station Hub', 'power': 250, 'capacity': 10,
             'street': '34th St & 8th Ave'},
            
            {'id': 'ev_grand_central', 'lat': 40.7527, 'lon': -73.9772,
             'name': 'Grand Central Station', 'power': 250, 'capacity': 10,
             'street': '42nd St & Park Ave'},
            
            # Upper Manhattan
            {'id': 'ev_columbus_circle', 'lat': 40.7680, 'lon': -73.9819,
             'name': 'Columbus Circle', 'power': 150, 'capacity': 8,
             'street': '59th St & Columbus Circle'},
            
            {'id': 'ev_lincoln_center', 'lat': 40.7725, 'lon': -73.9835,
             'name': 'Lincoln Center Station', 'power': 150, 'capacity': 6,
             'street': '65th St & Broadway'},
            
            # Lower Manhattan
            {'id': 'ev_union_square', 'lat': 40.7359, 'lon': -73.9911,
             'name': 'Union Square Station', 'power': 200, 'capacity': 8,
             'street': '14th St & Broadway'},
            
            {'id': 'ev_washington_square', 'lat': 40.7308, 'lon': -73.9973,
             'name': 'Washington Square', 'power': 150, 'capacity': 6,
             'street': 'Washington Sq & 5th Ave'},
            
            {'id': 'ev_wall_street', 'lat': 40.7074, 'lon': -74.0113,
             'name': 'Financial District', 'power': 250, 'capacity': 10,
             'street': 'Wall St & Broadway'},
            
            # East Side
            {'id': 'ev_un_plaza', 'lat': 40.7489, 'lon': -73.9680,
             'name': 'UN Plaza Station', 'power': 150, 'capacity': 6,
             'street': '42nd St & 1st Ave'},
            
            # West Side
            {'id': 'ev_chelsea_market', 'lat': 40.7424, 'lon': -74.0060,
             'name': 'Chelsea Market', 'power': 200, 'capacity': 8,
             'street': '15th St & 9th Ave'},
            
            {'id': 'ev_hudson_yards', 'lat': 40.7538, 'lon': -74.0020,
             'name': 'Hudson Yards', 'power': 350, 'capacity': 15,
             'street': '34th St & 11th Ave'},
            
            # Central Park adjacent
            {'id': 'ev_central_park_south', 'lat': 40.7644, 'lon': -73.9735,
             'name': 'Central Park South', 'power': 150, 'capacity': 6,
             'street': '59th St & 5th Ave'}
        ]
        
        # Initialize tracking
        for station in self.stations:
            self.charging_sessions[station['id']] = {}
        
        print(f"⚡ Created {len(self.stations)} EV stations on Manhattan grid")
        return self.stations
    
    def process_ev_charging(self, vehicles):
        """Process EV charging with realistic behavior"""
        charging_vehicles = {}
        total_evs = 0
        charging_count = 0
        
        for vehicle in vehicles:
            vid = vehicle['id']
            
            # 30% are EVs
            is_ev = hash(vid) % 100 < 30
            
            if is_ev:
                total_evs += 1
                vehicle['is_ev'] = True
                
                # Check proximity to stations
                for station in self.stations:
                    # Manhattan block distance (more realistic for grid)
                    lat_blocks = abs(vehicle['y'] - station['lat']) * 20  # ~20 blocks per degree
                    lon_blocks = abs(vehicle['x'] - station['lon']) * 13  # ~13 blocks per degree
                    
                    # Within 2 blocks and slow/stopped
                    if lat_blocks < 2 and lon_blocks < 2 and vehicle.get('speed', 0) < 2.0:
                        if station['id'] not in charging_vehicles:
                            charging_vehicles[station['id']] = []
                        
                        if len(charging_vehicles[station['id']]) < station['capacity']:
                            charging_vehicles[station['id']].append(vid)
                            charging_count += 1
                            
                            # Track session
                            if vid not in self.charging_sessions[station['id']]:
                                self.charging_sessions[station['id']][vid] = {
                                    'start': time.time(),
                                    'energy_kwh': 0
                                }
                            
                            # Update energy
                            session = self.charging_sessions[station['id']][vid]
                            duration_hours = (time.time() - session['start']) / 3600
                            session['energy_kwh'] = min(station['power'] * duration_hours, 100)  # Max 100 kWh
                            
                            break
            else:
                vehicle['is_ev'] = False
        
        return total_evs, charging_count, charging_vehicles

class PowerGridManager:
    """Advanced power grid management for NYC"""
    
    def __init__(self):
        self.network = None
        self.history = []
        self.peak_demand = 0
        self.total_energy = 0
        
    def initialize_nyc_grid(self):
        """Initialize NYC power grid"""
        print("⚡ Initializing NYC Power Grid...")
        self.network = NYCPowerNetworkSimple()
        self.network.build_network()
        
        # Set realistic capacities
        self.network.lines['DL_Manhattan_Traffic']['capacity_mw'] = 350
        self.network.lines['DL_Brooklyn_Traffic']['capacity_mw'] = 250
        self.network.lines['DL_Queens_Traffic']['capacity_mw'] = 300
        
        print("✅ NYC Power Grid initialized")
        return self.network
    
    def calculate_real_time_load(self, traffic_data, ev_data):
        """Calculate real-time power load"""
        current_time = time.time()
        hour = (int(current_time) % 86400) // 3600
        
        # NYC base load pattern (MW)
        base_patterns = {
            'night': 1800,     # 0-5
            'morning': 2400,   # 6-9
            'midday': 2200,    # 10-16
            'evening': 2600,   # 17-21
            'late': 2000       # 22-23
        }
        
        if hour < 6:
            base_load = base_patterns['night']
        elif hour < 10:
            base_load = base_patterns['morning']
        elif hour < 17:
            base_load = base_patterns['midday']
        elif hour < 22:
            base_load = base_patterns['evening']
        else:
            base_load = base_patterns['late']
        
        # Add variations
        seasonal_factor = 1.0 + 0.1 * math.sin(time.time() / 86400)  # Daily variation
        weather_factor = random.uniform(0.95, 1.05)  # Weather impact
        
        base_load *= seasonal_factor * weather_factor
        
        # Traffic infrastructure load
        traffic_lights_mw = traffic_data['lights_count'] * 0.003  # 3kW per intersection
        street_lights_mw = 20.0 if hour < 6 or hour > 18 else 0  # Street lighting
        traffic_systems_mw = traffic_data['vehicle_count'] * 0.0001  # Traffic management
        
        # EV charging load
        ev_charging_mw = ev_data['total_power_mw']
        
        # Calculate total
        total_load = base_load + traffic_lights_mw + street_lights_mw + traffic_systems_mw + ev_charging_mw
        
        # Update metrics
        if total_load > self.peak_demand:
            self.peak_demand = total_load
        
        self.total_energy += total_load / 3600  # MWh
        
        # Line utilization
        line_utilization = {
            'DL_Manhattan_Traffic': min(95, (ev_charging_mw / 3.5) * 100),
            'DL_Brooklyn_Traffic': min(85, (traffic_lights_mw / 2.5) * 100),
            'DL_Queens_Traffic': min(80, (street_lights_mw / 3.0) * 100)
        }
        
        # Grid metrics
        load_factor = (total_load / self.peak_demand * 100) if self.peak_demand > 0 else 0
        renewable_percent = 15.0 if 10 <= hour <= 16 else 5.0  # Solar during day
        
        return {
            'total_load_mw': round(total_load, 1),
            'base_load_mw': round(base_load, 1),
            'traffic_infrastructure_mw': round(traffic_lights_mw + street_lights_mw, 1),
            'ev_charging_mw': round(ev_charging_mw, 3),
            'traffic_systems_mw': round(traffic_systems_mw, 2),
            'line_utilization': line_utilization,
            'load_factor': round(load_factor, 1),
            'renewable_percent': renewable_percent,
            'peak_demand_mw': round(self.peak_demand, 1),
            'total_energy_mwh': round(self.total_energy, 2),
            'trend': self._calculate_trend()
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
        
        # Add polygon file if exists
        poly_file = "osm.poly.xml.gz"
        if os.path.exists(os.path.join(city_dir, poly_file)):
            f.write(f'        <additional-files value="{poly_file}"/>\n')
        
        f.write('    </input>\n')
        
        f.write('    <processing>\n')
        f.write('        <ignore-route-errors value="true"/>\n')
        f.write('        <time-to-teleport value="300"/>\n')
        f.write('        <max-depart-delay value="900"/>\n')
        f.write('        <routing-algorithm value="dijkstra"/>\n')
        f.write('        <device.rerouting.probability value="1"/>\n')
        f.write('        <device.rerouting.period value="60"/>\n')
        f.write('        <scale value="0.5"/>\n')  # Moderate traffic
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
            
            # Check if in Manhattan bounds
            if (40.700 <= gps[1] <= 40.800 and -74.020 <= gps[0] <= -73.930):
                manhattan_vehicles.append({
                    'id': vid,
                    'x': gps[0],
                    'y': gps[1],
                    'angle': traci.vehicle.getAngle(vid),
                    'speed': traci.vehicle.getSpeed(vid),
                    'is_ev': False  # Will be set by EV system
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
            
            # Determine color
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
    
    for station in ev_network.stations:
        vehicles_at_station = charging_vehicles.get(station['id'], [])
        num_charging = len(vehicles_at_station)
        utilization = (num_charging / station['capacity']) * 100
        power_output_mw = (num_charging * station['power']) / 1000
        
        total_power_mw += power_output_mw
        
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
            'status': 'busy' if utilization > 80 else 'available'
        })
    
    return station_data, total_power_mw

def manhattan_simulation():
    """Main Manhattan simulation loop"""
    global simulation_running, metrics
    
    print("🏙️ Starting Manhattan Grid Simulation")
    print("📍 Area: 40.70°N to 40.80°N, -74.02°W to -73.93°W")
    
    city_config = CITY_CONFIGS["newyork"]
    working_dir = city_config["working_dir"]
    
    original_dir = os.getcwd()
    temp_cfg = None
    
    try:
        os.chdir(working_dir)
        temp_cfg = create_manhattan_sumocfg("newyork")
        
        simulation_running = True
        stop_event.clear()
        
        # Start SUMO
        sumo_cmd = [SUMO_BINARY, "-c", os.path.basename(temp_cfg)]
        traci.start(sumo_cmd)
        print("✅ SUMO started successfully")
        
        # Initialize systems
        traffic_controller.initialize_manhattan_lights()
        ev_network.create_manhattan_grid_stations()
        power_grid.initialize_nyc_grid()
        
        step_counter = 0
        
        while traci.simulation.getMinExpectedNumber() > 0 and not stop_event.is_set():
            traci.simulationStep()
            step_counter += 1
            
            # Update traffic lights every 10 steps
            if step_counter % 10 == 0:
                traffic_controller.update_cycle()
            
            # Main update every 2 steps
            if step_counter % 2 == 0:
                # Get Manhattan vehicles only
                vehicles = get_manhattan_vehicles()
                metrics['vehicles']['total'] = len(vehicles)
                
                # Process EV charging
                total_evs, charging_evs, charging_vehicles = ev_network.process_ev_charging(vehicles)
                metrics['vehicles']['evs'] = total_evs
                metrics['vehicles']['charging'] = charging_evs
                
                # Get traffic lights
                traffic_lights = get_manhattan_traffic_lights()
                metrics['traffic_lights']['total'] = len(traffic_lights)
                
                # Prepare EV station data
                ev_stations, total_ev_power_mw = prepare_ev_station_data(charging_vehicles)
                
                # Calculate power
                traffic_data = {
                    'lights_count': len(traffic_lights),
                    'vehicle_count': len(vehicles)
                }
                ev_data = {'total_power_mw': total_ev_power_mw}
                
                power_data = power_grid.calculate_real_time_load(traffic_data, ev_data)
                
                # Update global metrics
                metrics['power']['total_mw'] = power_data['total_load_mw']
                metrics['power']['ev_mw'] = power_data['ev_charging_mw']
                metrics['power']['traffic_mw'] = power_data['traffic_infrastructure_mw']
                metrics['grid']['load_factor'] = power_data['load_factor']
                metrics['grid']['renewable_percent'] = power_data['renewable_percent']
                
                # Debug output
                if step_counter % 50 == 0:
                    print(f"\n📊 Step {step_counter} | Time: {traci.simulation.getTime():.1f}s")
                    print(f"  🚗 Vehicles: {len(vehicles)} in Manhattan ({total_evs} EVs)")
                    print(f"  ⚡ Charging: {charging_evs} EVs at stations")
                    print(f"  🚦 Lights: {metrics['traffic_lights']['green']}G/{metrics['traffic_lights']['yellow']}Y/{metrics['traffic_lights']['red']}R")
                    print(f"  💡 Power: {power_data['total_load_mw']} MW (EV: {power_data['ev_charging_mw']} MW)")
                    print(f"  📈 Grid: {power_data['load_factor']:.1f}% load factor")
                
                # Send to frontend
                socketio.emit('update', {
                    'vehicles': vehicles,
                    'traffic_lights': traffic_lights,
                    'ev_stations': ev_stations,
                    'power': power_data,
                    'metrics': metrics,
                    'simulation_time': traci.simulation.getTime(),
                    'timestamp': datetime.now().isoformat()
                })
            
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
        'message': 'Connected to Manhattan Grid System',
        'features': ['Traffic Simulation', 'EV Charging', 'Power Grid', 'Real-time Metrics']
    })

@socketio.on('start_simulation')
def handle_start():
    global simulation_thread
    
    if not simulation_running:
        print("🚀 Starting Manhattan simulation...")
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
    
    # Reset metrics
    metrics['vehicles'] = {'total': 0, 'evs': 0, 'charging': 0}
    metrics['power'] = {'total_mw': 0, 'ev_mw': 0, 'traffic_mw': 0}
    
    simulation_thread = threading.Thread(target=manhattan_simulation)
    simulation_thread.start()

@app.route('/')
def index():
    return render_template('index_manhattan.html')

if __name__ == "__main__":
    print("=" * 80)
    print("🏙️  SUMOxPyPSA MANHATTAN GRID SYSTEM")
    print("=" * 80)
    print("📍 Focus: Manhattan (40.70-40.80°N, -74.02--73.93°W)")
    print("🚦 Traffic: Realistic Manhattan grid patterns")
    print("⚡ Power: Real-time NYC power grid simulation")
    print("🔌 EV Network: 12 charging stations on grid")
    print("=" * 80)
    print(f"🌐 Server: http://{HOST}:{PORT}")
    print("=" * 80)
    
    socketio.run(app, debug=True, host=HOST, port=PORT)