"""
Vehicle Generator - Creates realistic traffic flows with regular cars and EVs
Generates routes, manages vehicle insertion, and handles EV charging behavior
"""

import os
import sys
import json
import pandas as pd
import numpy as np
import random
import xml.etree.ElementTree as ET
from xml.dom import minidom
from pathlib import Path
from datetime import datetime

class VehicleGenerator:
    def __init__(self):
        """Initialize vehicle generator"""
        # Load configuration
        with open('configs/config.json', 'r') as f:
            self.config = json.load(f)
        
        # Load network data
        self.nodes_df = pd.read_csv('data/traffic/nodes.csv')
        self.edges_df = pd.read_csv('data/traffic/edges.csv')
        self.ev_stations = pd.read_csv('data/power/ev_stations.csv')
        
        print("\n" + "="*70)
        print(" VEHICLE GENERATOR ".center(70, "="))
        print("="*70)
        print(f"\n🚗 Generating vehicles for Manhattan simulation")
        print(f"📍 Area: {self.config['simulation']['area']['description']}")
        print("-"*70)
        
        # Vehicle parameters from config
        self.vehicle_config = self.config['vehicles']
        self.total_vehicles = self.vehicle_config['total_vehicles']
        self.ev_percentage = self.vehicle_config['ev_percentage']
        
        # Initialize random seed for reproducibility
        np.random.seed(self.config['simulation'].get('seed', 42))
        random.seed(self.config['simulation'].get('seed', 42))
        
    def generate_all(self, scenario='normal'):
        """Generate all vehicle-related files for a scenario"""
        print(f"\n🎬 Generating vehicles for scenario: {scenario}")
        
        # Load scenario config
        scenario_config = self.config['scenarios'].get(scenario, self.config['scenarios']['normal'])
        
        # Step 1: Generate routes
        print("\n[1/5] 🛣️  Generating route network...")
        routes = self.generate_routes()
        
        # Step 2: Generate OD matrix
        print("\n[2/5] 📊 Creating Origin-Destination matrix...")
        od_matrix = self.generate_od_matrix()
        
        # Step 3: Generate vehicle flows
        print("\n[3/5] 🚗 Creating vehicle flows...")
        vehicles = self.generate_vehicle_flows(routes, od_matrix, scenario_config)
        
        # Step 4: Add EV charging behavior
        print("\n[4/5] ⚡ Adding EV charging logic...")
        self.add_ev_charging_behavior(vehicles)
        
        # Step 5: Write route file
        print("\n[5/5] 💾 Writing route files...")
        self.write_route_file(vehicles, routes, scenario)
        
        print("\n" + "="*70)
        print(f"✅ VEHICLE GENERATION COMPLETE FOR '{scenario.upper()}'!")
        print("="*70)
        
        self.print_statistics(vehicles)
        
        return True
    
    def generate_routes(self):
        """Generate possible routes through the network"""
        routes = []
        
        # Create edge lookup
        edge_dict = {}
        for _, edge in self.edges_df.iterrows():
            from_node = int(edge['from_node'])
            to_node = int(edge['to_node'])
            
            if from_node not in edge_dict:
                edge_dict[from_node] = []
            edge_dict[from_node].append(to_node)
        
        # Generate routes of different lengths
        route_id = 0
        
        # Short routes (2-4 edges)
        for start_node in random.sample(list(edge_dict.keys()), min(20, len(edge_dict))):
            route = self.build_route(start_node, edge_dict, length=random.randint(2, 4))
            if route:
                routes.append({
                    'route_id': f'route_{route_id}',
                    'edges': route,
                    'type': 'short',
                    'distance': len(route) * 100
                })
                route_id += 1
        
        # Medium routes (5-8 edges)
        for start_node in random.sample(list(edge_dict.keys()), min(30, len(edge_dict))):
            route = self.build_route(start_node, edge_dict, length=random.randint(5, 8))
            if route:
                routes.append({
                    'route_id': f'route_{route_id}',
                    'edges': route,
                    'type': 'medium',
                    'distance': len(route) * 100
                })
                route_id += 1
        
        # Long routes (9-15 edges)
        for start_node in random.sample(list(edge_dict.keys()), min(20, len(edge_dict))):
            route = self.build_route(start_node, edge_dict, length=random.randint(9, 15))
            if route:
                routes.append({
                    'route_id': f'route_{route_id}',
                    'edges': route,
                    'type': 'long',
                    'distance': len(route) * 100
                })
                route_id += 1
        
        print(f"   ✅ Generated {len(routes)} routes")
        print(f"      • Short routes: {sum(1 for r in routes if r['type'] == 'short')}")
        print(f"      • Medium routes: {sum(1 for r in routes if r['type'] == 'medium')}")
        print(f"      • Long routes: {sum(1 for r in routes if r['type'] == 'long')}")
        
        return routes
    
    def build_route(self, start_node, edge_dict, length):
        """Build a single route from start node"""
        route = []
        current = start_node
        visited = set()
        
        for _ in range(length):
            if current not in edge_dict or current in visited:
                break
            
            visited.add(current)
            
            # Get possible next nodes
            next_nodes = [n for n in edge_dict.get(current, []) if n not in visited]
            if not next_nodes:
                break
            
            # Choose next node
            next_node = random.choice(next_nodes)
            
            # Add edge to route
            edge_id = self.get_edge_id(current, next_node)
            if edge_id:
                route.append(edge_id)
                current = next_node
        
        return route if len(route) >= 2 else None
    
    def get_edge_id(self, from_node, to_node):
        """Get edge ID for node pair"""
        edge = self.edges_df[
            (self.edges_df['from_node'] == from_node) & 
            (self.edges_df['to_node'] == to_node)
        ]
        
        if not edge.empty:
            return f"e{int(edge.iloc[0]['edge_id'])}"
        return None
    
    def generate_od_matrix(self):
        """Generate Origin-Destination demand matrix"""
        # Define zones (simplified - divide area into quadrants)
        zones = {
            'NW': {'nodes': [], 'demand_factor': 1.2},  # Times Square area - high demand
            'NE': {'nodes': [], 'demand_factor': 1.0},  # Grand Central area
            'SW': {'nodes': [], 'demand_factor': 0.8},  # Port Authority area
            'SE': {'nodes': [], 'demand_factor': 0.9}   # Lower Midtown
        }
        
        # Assign nodes to zones
        center_lat = (self.config['simulation']['area']['north'] + 
                     self.config['simulation']['area']['south']) / 2
        center_lon = (self.config['simulation']['area']['east'] + 
                     self.config['simulation']['area']['west']) / 2
        
        for _, node in self.nodes_df.iterrows():
            if node['lat'] >= center_lat:
                if node['lon'] <= center_lon:
                    zones['NW']['nodes'].append(node['node_id'])
                else:
                    zones['NE']['nodes'].append(node['node_id'])
            else:
                if node['lon'] <= center_lon:
                    zones['SW']['nodes'].append(node['node_id'])
                else:
                    zones['SE']['nodes'].append(node['node_id'])
        
        # Create OD matrix
        od_matrix = {}
        for origin_zone, origin_data in zones.items():
            for dest_zone, dest_data in zones.items():
                # Higher demand for cross-zone trips
                if origin_zone != dest_zone:
                    demand = origin_data['demand_factor'] * dest_data['demand_factor'] * 100
                else:
                    demand = origin_data['demand_factor'] * 50  # Lower intra-zone demand
                
                od_matrix[f"{origin_zone}_{dest_zone}"] = {
                    'origin_nodes': origin_data['nodes'],
                    'dest_nodes': dest_data['nodes'],
                    'demand': int(demand)
                }
        
        total_demand = sum(od['demand'] for od in od_matrix.values())
        print(f"   ✅ Generated OD matrix")
        print(f"      • Zones: 4 (NW, NE, SW, SE)")
        print(f"      • OD pairs: {len(od_matrix)}")
        print(f"      • Total demand: {total_demand} trips")
        
        return od_matrix
    
    def generate_vehicle_flows(self, routes, od_matrix, scenario_config):
        """Generate vehicle flows based on OD matrix and scenario"""
        vehicles = []
        vehicle_id = 0
        
        # Time distribution (vehicles per hour)
        time_distribution = self.get_time_distribution(scenario_config)
        
        # Generate vehicles for each OD pair
        for od_pair, od_data in od_matrix.items():
            num_vehicles = int(od_data['demand'] * scenario_config.get('vehicle_factor', 1.0))
            
            for _ in range(num_vehicles):
                # Select vehicle type
                rand_val = random.random()
                if rand_val < self.ev_percentage:
                    vtype = 'ev'
                    battery_capacity = random.choice([40, 60, 100]) * 1000  # Wh
                    initial_soc = random.uniform(0.3, 0.9)
                elif rand_val < self.ev_percentage + self.vehicle_config['bus_percentage']:
                    vtype = 'bus'
                    battery_capacity = 0
                    initial_soc = 0
                elif rand_val < self.ev_percentage + self.vehicle_config['bus_percentage'] + self.vehicle_config['taxi_percentage']:
                    vtype = 'taxi'
                    battery_capacity = 0
                    initial_soc = 0
                else:
                    vtype = 'car'
                    battery_capacity = 0
                    initial_soc = 0
                
                # Select route
                suitable_routes = [r for r in routes if len(r['edges']) >= 2]
                if not suitable_routes:
                    continue
                    
                route = random.choice(suitable_routes)
                
                # Determine departure time
                depart_time = self.sample_departure_time(time_distribution)
                
                vehicles.append({
                    'vehicle_id': f'veh_{vehicle_id}',
                    'type': vtype,
                    'route_id': route['route_id'],
                    'depart': depart_time,
                    'origin_zone': od_pair.split('_')[0],
                    'dest_zone': od_pair.split('_')[1],
                    'battery_capacity': battery_capacity,
                    'initial_soc': initial_soc,
                    'needs_charging': False
                })
                
                vehicle_id += 1
        
        print(f"   ✅ Generated {len(vehicles)} vehicles")
        print(f"      • Regular cars: {sum(1 for v in vehicles if v['type'] == 'car')}")
        print(f"      • EVs: {sum(1 for v in vehicles if v['type'] == 'ev')}")
        print(f"      • Buses: {sum(1 for v in vehicles if v['type'] == 'bus')}")
        print(f"      • Taxis: {sum(1 for v in vehicles if v['type'] == 'taxi')}")
        
        return vehicles
    
    def get_time_distribution(self, scenario_config):
        """Get time distribution for vehicle departures"""
        scenario_name = scenario_config.get('name', 'Normal Day')
        
        if 'Rush Hour' in scenario_name:
            # Peak at rush hour
            return {
                'peak_time': 8 * 3600,  # 8 AM
                'std_dev': 1800,  # 30 minutes
                'base_rate': 0.2
            }
        elif 'Evening' in scenario_name:
            # Peak in evening
            return {
                'peak_time': 18 * 3600,  # 6 PM
                'std_dev': 2400,  # 40 minutes
                'base_rate': 0.3
            }
        elif 'Night' in scenario_name:
            # Low uniform distribution
            return {
                'peak_time': 2 * 3600,  # 2 AM
                'std_dev': 7200,  # 2 hours
                'base_rate': 0.1
            }
        else:
            # Normal distribution throughout simulation
            return {
                'peak_time': 1800,  # Middle of simulation
                'std_dev': 900,  # 15 minutes
                'base_rate': 0.5
            }
    
    def sample_departure_time(self, time_distribution):
        """Sample a departure time from distribution"""
        # Use normal distribution around peak time
        time = np.random.normal(
            time_distribution['peak_time'],
            time_distribution['std_dev']
        )
        
        # Ensure within simulation bounds
        time = max(0, min(time, self.config['simulation']['duration_seconds'] - 300))
        
        return int(time)
    
    def add_ev_charging_behavior(self, vehicles):
        """Add charging behavior for EVs"""
        ev_vehicles = [v for v in vehicles if v['type'] == 'ev']
        
        charging_threshold = self.config['ev_charging']['charge_threshold']
        
        for ev in ev_vehicles:
            # Determine if EV needs charging based on SOC
            if ev['initial_soc'] < charging_threshold:
                ev['needs_charging'] = True
                
                # Find nearest charging station
                # (Simplified - in real implementation would use actual positions)
                station = random.choice(self.ev_stations.to_dict('records'))
                ev['charging_station'] = station['station_id']
                ev['charging_duration'] = self.calculate_charging_time(
                    ev['battery_capacity'],
                    ev['initial_soc'],
                    station['power_kw']
                )
        
        num_charging = sum(1 for v in vehicles if v.get('needs_charging', False))
        print(f"   ✅ Added EV charging behavior")
        print(f"      • EVs needing charge: {num_charging}/{len(ev_vehicles)}")
        print(f"      • Average SOC: {np.mean([v['initial_soc'] for v in ev_vehicles]):.2%}")
    
    def calculate_charging_time(self, battery_capacity, initial_soc, charger_power):
        """Calculate charging time in seconds"""
        target_soc = self.config['ev_charging']['target_soc']
        energy_needed = battery_capacity * (target_soc - initial_soc) / 1000  # kWh
        
        # Account for charging efficiency
        if charger_power >= 150:  # DC Fast
            efficiency = self.config['ev_charging']['dc_fast']['efficiency']
        else:  # Level 2
            efficiency = self.config['ev_charging']['level_2']['efficiency']
        
        charging_time_hours = energy_needed / (charger_power * efficiency)
        return int(charging_time_hours * 3600)  # Convert to seconds
    
    def write_route_file(self, vehicles, routes, scenario):
        """Write SUMO route file"""
        root = ET.Element('routes')
        
        # Add vehicle types (already defined in network_builder)
        # Reference the types file
        
        # Add routes
        for route in routes:
            route_elem = ET.SubElement(root, 'route')
            route_elem.set('id', route['route_id'])
            route_elem.set('edges', ' '.join(route['edges']))
        
        # Add vehicles
        vehicles_sorted = sorted(vehicles, key=lambda x: x['depart'])
        
        for vehicle in vehicles_sorted:
            if vehicle['type'] in ['car', 'taxi', 'bus']:
                # Regular vehicle
                veh_elem = ET.SubElement(root, 'vehicle')
                veh_elem.set('id', vehicle['vehicle_id'])
                veh_elem.set('type', vehicle['type'])
                veh_elem.set('route', vehicle['route_id'])
                veh_elem.set('depart', str(vehicle['depart']))
                
            elif vehicle['type'] == 'ev':
                # Electric vehicle with battery
                veh_elem = ET.SubElement(root, 'vehicle')
                veh_elem.set('id', vehicle['vehicle_id'])
                veh_elem.set('type', 'ev')
                veh_elem.set('route', vehicle['route_id'])
                veh_elem.set('depart', str(vehicle['depart']))
                
                # Add battery parameters
                param1 = ET.SubElement(veh_elem, 'param')
                param1.set('key', 'has.battery.device')
                param1.set('value', 'true')
                
                param2 = ET.SubElement(veh_elem, 'param')
                param2.set('key', 'maximumBatteryCapacity')
                param2.set('value', str(vehicle['battery_capacity']))
                
                param3 = ET.SubElement(veh_elem, 'param')
                param3.set('key', 'actualBatteryCapacity')
                param3.set('value', str(int(vehicle['battery_capacity'] * vehicle['initial_soc'])))
                
                if vehicle.get('needs_charging'):
                    # Add stop for charging
                    stop = ET.SubElement(veh_elem, 'stop')
                    stop.set('parkingArea', f"pa_{vehicle.get('charging_station', 'ev_station_0')}")
                    stop.set('duration', str(vehicle.get('charging_duration', 1800)))
        
        # Write to file
        filename = f'data/sumo/routes_{scenario}.rou.xml'
        self.write_xml(root, filename)
        
        print(f"   ✅ Wrote route file: {filename}")
        print(f"      • Total vehicles: {len(vehicles)}")
        print(f"      • Simulation period: {min(v['depart'] for v in vehicles)}-{max(v['depart'] for v in vehicles)}s")
    
    def write_xml(self, root, filename):
        """Write XML with proper formatting"""
        rough_string = ET.tostring(root, 'utf-8')
        reparsed = minidom.parseString(rough_string)
        pretty_xml = reparsed.toprettyxml(indent="    ")
        
        # Remove extra blank lines
        lines = pretty_xml.split('\n')
        lines = [line for line in lines if line.strip()]
        pretty_xml = '\n'.join(lines)
        
        with open(filename, 'w') as f:
            f.write(pretty_xml)
    
    def print_statistics(self, vehicles):
        """Print vehicle statistics"""
        print("\n📊 VEHICLE STATISTICS:")
        print(f"\n   Vehicle Types:")
        for vtype in ['car', 'ev', 'bus', 'taxi']:
            count = sum(1 for v in vehicles if v['type'] == vtype)
            percentage = (count / len(vehicles)) * 100 if vehicles else 0
            print(f"      • {vtype.upper()}: {count} ({percentage:.1f}%)")
        
        if any(v['type'] == 'ev' for v in vehicles):
            ev_vehicles = [v for v in vehicles if v['type'] == 'ev']
            print(f"\n   EV Statistics:")
            print(f"      • Average initial SOC: {np.mean([v['initial_soc'] for v in ev_vehicles]):.1%}")
            print(f"      • Vehicles needing charge: {sum(1 for v in ev_vehicles if v.get('needs_charging', False))}")
            
            battery_sizes = [v['battery_capacity']/1000 for v in ev_vehicles]
            print(f"      • Battery capacities: {min(battery_sizes):.0f}-{max(battery_sizes):.0f} kWh")
        
        print(f"\n   Temporal Distribution:")
        print(f"      • First departure: {min(v['depart'] for v in vehicles):.0f}s")
        print(f"      • Last departure: {max(v['depart'] for v in vehicles):.0f}s")
        print(f"      • Peak hour: {np.median([v['depart'] for v in vehicles])/3600:.1f}h")

def generate_all_scenarios():
    """Generate vehicles for all scenarios"""
    generator = VehicleGenerator()
    
    scenarios = ['normal', 'rush_hour', 'night']
    
    for scenario in scenarios:
        print(f"\n{'='*70}")
        print(f" GENERATING SCENARIO: {scenario.upper()} ".center(70, '='))
        print(f"{'='*70}")
        
        generator.generate_all(scenario)
        
    print("\n" + "="*70)
    print(" ALL SCENARIOS GENERATED ".center(70, "="))
    print("="*70)
    print("\n✅ Vehicle generation complete for all scenarios!")
    print("\nGenerated files:")
    for scenario in scenarios:
        print(f"   • data/sumo/routes_{scenario}.rou.xml")

if __name__ == "__main__":
    generate_all_scenarios()
    input("\nPress Enter to continue...")