"""
Manhattan Data Fetcher - Downloads and prepares all data for simulation
This will fetch real Manhattan data and create all necessary infrastructure files
"""

import os
import sys
import json
import osmnx as ox
import pandas as pd
import numpy as np
import requests
from pathlib import Path
import warnings
from datetime import datetime
import geopandas as gpd
from shapely.geometry import Point
warnings.filterwarnings('ignore')

class ManhattanDataFetcher:
    def __init__(self):
        """Initialize data fetcher with configuration"""
        # Load configuration
        with open('configs/config.json', 'r') as f:
            self.config = json.load(f)
        
        self.bounds = self.config['simulation']['area']
        self.start_time = datetime.now()
        
        print("\n" + "="*70)
        print(" MANHATTAN DATA FETCHER ".center(70, "="))
        print("="*70)
        print(f"\n📍 Target Area: {self.bounds['description']}")
        print(f"📐 Coordinates: ({self.bounds['south']:.4f}, {self.bounds['west']:.4f}) to ({self.bounds['north']:.4f}, {self.bounds['east']:.4f})")
        print(f"🕐 Started: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("\n" + "-"*70)
        
    def fetch_all(self):
        """Main function to fetch all required data"""
        try:
            print("\n📥 STARTING DATA COLLECTION...\n")
            
            # Step 1: Road Network
            print("[1/7] 🛣️  Fetching road network from OpenStreetMap...")
            self.fetch_road_network()
            
            # Step 2: Traffic Signals
            print("\n[2/7] 🚦 Creating traffic signals...")
            self.create_traffic_signals()
            
            # Step 3: EV Stations
            print("\n[3/7] ⚡ Setting up EV charging stations...")
            self.create_ev_stations()
            
            # Step 4: Power Grid
            print("\n[4/7] 🔌 Building power grid infrastructure...")
            self.create_power_grid()
            
            # Step 5: Buildings
            print("\n[5/7] 🏢 Generating building loads...")
            self.generate_buildings()
            
            # Step 6: Scenario Data
            print("\n[6/7] 📊 Creating scenario templates...")
            self.create_scenarios()
            
            # Step 7: Validation
            print("\n[7/7] ✔️  Validating all data...")
            self.validate_data()
            
            # Summary
            self.print_summary()
            
        except Exception as e:
            print(f"\n❌ ERROR: {e}")
            print("Attempting to continue with partial data...")
            
    def fetch_road_network(self):
        """Download road network from OpenStreetMap"""
        try:
            # Configure OSMnx
            ox.settings.use_cache = True
            ox.settings.log_console = False
            
            # Download the network
            print("   ⏳ Downloading from OpenStreetMap...")
            G = ox.graph_from_bbox(
                north=self.bounds['north'],
                south=self.bounds['south'],
                east=self.bounds['east'],
                west=self.bounds['west'],
                network_type='drive',
                simplify=True,
                clean_periphery=True
            )
            
            # Get basic stats
            stats = ox.basic_stats(G)
            n_nodes = len(G.nodes())
            n_edges = len(G.edges())
            
            # Convert to GeoDataFrames
            nodes_gdf, edges_gdf = ox.graph_to_gdfs(G)
            
            # Save files
            print("   💾 Saving network files...")
            
            # Save for SUMO conversion
            ox.save_graphml(G, filepath='data/traffic/manhattan.graphml')
            
            # Save as CSV for easy access
            nodes_df = pd.DataFrame({
                'node_id': nodes_gdf.index,
                'x': nodes_gdf.geometry.x,
                'y': nodes_gdf.geometry.y,
                'lat': nodes_gdf.geometry.y,
                'lon': nodes_gdf.geometry.x
            })
            nodes_df.to_csv('data/traffic/nodes.csv', index=False)
            
            # Process edges
            edges_df = edges_gdf.reset_index()
            edges_simple = pd.DataFrame({
                'edge_id': range(len(edges_df)),
                'from_node': edges_df['u'],
                'to_node': edges_df['v'],
                'length': edges_df['length'] if 'length' in edges_df.columns else 100,
                'speed_limit': 13.89,  # 50 km/h default
                'lanes': 2
            })
            edges_simple.to_csv('data/traffic/edges.csv', index=False)
            
            print(f"   ✅ Network downloaded successfully!")
            print(f"      • Nodes (intersections): {n_nodes}")
            print(f"      • Edges (road segments): {n_edges}")
            print(f"      • Total road length: {stats.get('edge_length_total', n_edges*100)/1000:.2f} km")
            
        except Exception as e:
            print(f"   ⚠️ OSM download failed: {e}")
            print("   🔄 Creating synthetic grid network...")
            self.create_synthetic_network()
            
    def create_synthetic_network(self):
        """Create a synthetic Manhattan grid if OSM fails"""
        nodes = []
        edges = []
        
        # Create 11x11 grid
        grid_size = 11
        lat_step = (self.bounds['north'] - self.bounds['south']) / (grid_size - 1)
        lon_step = (self.bounds['east'] - self.bounds['west']) / (grid_size - 1)
        
        # Create nodes
        node_id = 0
        node_map = {}
        for i in range(grid_size):
            for j in range(grid_size):
                lat = self.bounds['south'] + lat_step * i
                lon = self.bounds['west'] + lon_step * j
                nodes.append({
                    'node_id': node_id,
                    'x': lon,
                    'y': lat,
                    'lat': lat,
                    'lon': lon
                })
                node_map[(i, j)] = node_id
                node_id += 1
        
        # Create edges (horizontal and vertical streets)
        edge_id = 0
        for i in range(grid_size):
            for j in range(grid_size):
                current = node_map[(i, j)]
                
                # Horizontal edge (avenue)
                if j < grid_size - 1:
                    edges.append({
                        'edge_id': edge_id,
                        'from_node': current,
                        'to_node': node_map[(i, j + 1)],
                        'length': 100,
                        'speed_limit': 13.89,
                        'lanes': 3 if i % 2 == 0 else 2  # Major avenues have more lanes
                    })
                    edge_id += 1
                
                # Vertical edge (street)
                if i < grid_size - 1:
                    edges.append({
                        'edge_id': edge_id,
                        'from_node': current,
                        'to_node': node_map[(i + 1, j)],
                        'length': 100,
                        'speed_limit': 11.11,  # 40 km/h for cross streets
                        'lanes': 2
                    })
                    edge_id += 1
        
        # Save files
        pd.DataFrame(nodes).to_csv('data/traffic/nodes.csv', index=False)
        pd.DataFrame(edges).to_csv('data/traffic/edges.csv', index=False)
        
        print(f"   ✅ Synthetic network created!")
        print(f"      • Nodes: {len(nodes)}")
        print(f"      • Edges: {len(edges)}")
        
    def create_traffic_signals(self):
        """Create traffic signals at major intersections"""
        nodes_df = pd.read_csv('data/traffic/nodes.csv')
        
        signals = []
        signal_id = 0
        
        # Place signals at every 3rd intersection (simulating major intersections)
        for idx, node in nodes_df.iterrows():
            if idx % 3 == 0:  # Major intersection
                signals.append({
                    'signal_id': f'sig_{signal_id}',
                    'node_id': node['node_id'],
                    'lat': node['lat'],
                    'lon': node['lon'],
                    'type': 'actuated',
                    'phases': 4,
                    'cycle_time': 120,
                    'yellow_time': 3,
                    'all_red_time': 2,
                    'power_w': 100,  # LED signals
                    'has_backup': True
                })
                signal_id += 1
        
        df = pd.DataFrame(signals)
        df.to_csv('data/traffic/signals.csv', index=False)
        
        print(f"   ✅ Created {len(signals)} traffic signals")
        print(f"      • Actuated signals: {len(signals)}")
        print(f"      • Total power consumption: {len(signals) * 100 / 1000:.1f} kW")
        
    def create_ev_stations(self):
        """Create realistic EV charging stations"""
        stations = []
        
        # Real-world inspired locations in Midtown
        station_configs = [
            {'name': 'Times Square Supercharger', 'lat': 40.7580, 'lon': -73.9855, 
             'type': 'SUPERCHARGER', 'power_kw': 250, 'num_chargers': 8},
            {'name': 'Bryant Park Fast Charge', 'lat': 40.7536, 'lon': -73.9832,
             'type': 'DC_FAST', 'power_kw': 150, 'num_chargers': 6},
            {'name': 'Grand Central Station', 'lat': 40.7527, 'lon': -73.9772,
             'type': 'LEVEL_2', 'power_kw': 7.2, 'num_chargers': 20},
            {'name': 'Port Authority Terminal', 'lat': 40.7571, 'lon': -73.9897,
             'type': 'DC_FAST', 'power_kw': 150, 'num_chargers': 4},
            {'name': 'Rockefeller Center', 'lat': 40.7587, 'lon': -73.9787,
             'type': 'DC_FAST', 'power_kw': 100, 'num_chargers': 6},
            {'name': '5th Ave Parking', 'lat': 40.7560, 'lon': -73.9740,
             'type': 'LEVEL_2', 'power_kw': 7.2, 'num_chargers': 15},
            {'name': '8th Ave Garage', 'lat': 40.7550, 'lon': -73.9870,
             'type': 'LEVEL_2', 'power_kw': 7.2, 'num_chargers': 10},
            {'name': 'Madison Square', 'lat': 40.7540, 'lon': -73.9800,
             'type': 'DC_FAST', 'power_kw': 50, 'num_chargers': 8}
        ]
        
        for i, config in enumerate(station_configs):
            # Check if location is within bounds
            if (self.bounds['south'] <= config['lat'] <= self.bounds['north'] and
                self.bounds['west'] <= config['lon'] <= self.bounds['east']):
                
                config['station_id'] = f'ev_station_{i}'
                config['total_power_kw'] = config['power_kw'] * config['num_chargers']
                config['queue_capacity'] = config['num_chargers'] * 2
                config['price_per_kwh'] = 0.35 if config['type'] == 'SUPERCHARGER' else 0.25
                stations.append(config)
        
        df = pd.DataFrame(stations)
        df.to_csv('data/power/ev_stations.csv', index=False)
        
        total_chargers = sum(s['num_chargers'] for s in stations)
        total_power = sum(s['total_power_kw'] for s in stations)
        
        print(f"   ✅ Created {len(stations)} EV charging stations")
        print(f"      • Total chargers: {total_chargers}")
        print(f"      • Total capacity: {total_power/1000:.1f} MW")
        print(f"      • Superchargers: {sum(1 for s in stations if s['type'] == 'SUPERCHARGER')}")
        print(f"      • DC Fast: {sum(1 for s in stations if s['type'] == 'DC_FAST')}")
        print(f"      • Level 2: {sum(1 for s in stations if s['type'] == 'LEVEL_2')}")
        
    def create_power_grid(self):
        """Create realistic power grid infrastructure"""
        # Substations (based on real Con Edison infrastructure)
        substations = [
            {'name': 'TimesSquare_Sub', 'substation_id': 'SUB_1',
             'lat': 40.7580, 'lon': -73.9855, 'voltage_kv': 138, 'capacity_mva': 100},
            {'name': 'GrandCentral_Sub', 'substation_id': 'SUB_2',
             'lat': 40.7527, 'lon': -73.9772, 'voltage_kv': 138, 'capacity_mva': 150},
            {'name': 'PortAuth_Sub', 'substation_id': 'SUB_3',
             'lat': 40.7571, 'lon': -73.9897, 'voltage_kv': 138, 'capacity_mva': 120}
        ]
        
        # Distribution transformers (spread across the area)
        transformers = []
        np.random.seed(42)
        for i in range(25):
            lat = np.random.uniform(self.bounds['south'], self.bounds['north'])
            lon = np.random.uniform(self.bounds['west'], self.bounds['east'])
            transformers.append({
                'transformer_id': f'TR_{i}',
                'lat': lat,
                'lon': lon,
                'rating_kva': np.random.choice([500, 750, 1000, 1500, 2000]),
                'primary_voltage_kv': 13.8,
                'secondary_voltage_v': 480,
                'loading_percent': np.random.uniform(40, 70)
            })
        
        # Power lines connecting substations and transformers
        lines = []
        line_id = 0
        
        # Transmission lines between substations
        lines.append({'line_id': f'TL_{line_id}', 'from': 'SUB_1', 'to': 'SUB_2',
                     'voltage_kv': 138, 'length_km': 0.8, 'capacity_mva': 100,
                     'resistance_ohm_per_km': 0.05, 'reactance_ohm_per_km': 0.15})
        line_id += 1
        
        lines.append({'line_id': f'TL_{line_id}', 'from': 'SUB_2', 'to': 'SUB_3',
                     'voltage_kv': 138, 'length_km': 0.7, 'capacity_mva': 100,
                     'resistance_ohm_per_km': 0.05, 'reactance_ohm_per_km': 0.15})
        line_id += 1
        
        lines.append({'line_id': f'TL_{line_id}', 'from': 'SUB_3', 'to': 'SUB_1',
                     'voltage_kv': 138, 'length_km': 0.5, 'capacity_mva': 100,
                     'resistance_ohm_per_km': 0.05, 'reactance_ohm_per_km': 0.15})
        line_id += 1
        
        # Distribution lines from substations to transformers
        for transformer in transformers[:10]:
            closest_sub = min(substations, 
                            key=lambda s: abs(s['lat']-transformer['lat']) + abs(s['lon']-transformer['lon']))
            lines.append({
                'line_id': f'DL_{line_id}',
                'from': closest_sub['substation_id'],
                'to': transformer['transformer_id'],
                'voltage_kv': 13.8,
                'length_km': np.random.uniform(0.1, 0.5),
                'capacity_mva': 10,
                'resistance_ohm_per_km': 0.1,
                'reactance_ohm_per_km': 0.08
            })
            line_id += 1
        
        # Save files
        pd.DataFrame(substations).to_csv('data/power/substations.csv', index=False)
        pd.DataFrame(transformers).to_csv('data/power/transformers.csv', index=False)
        pd.DataFrame(lines).to_csv('data/power/lines.csv', index=False)
        
        total_capacity = sum(s['capacity_mva'] for s in substations)
        
        print(f"   ✅ Power grid infrastructure created")
        print(f"      • Substations: {len(substations)} ({total_capacity} MVA total)")
        print(f"      • Transformers: {len(transformers)}")
        print(f"      • Power lines: {len(lines)}")
        
    def generate_buildings(self):
        """Generate realistic building loads"""
        buildings = []
        np.random.seed(42)
        
        # Building types distribution for Midtown
        building_types = {
            'office': 0.40,      # 40% office buildings
            'retail': 0.25,      # 25% retail
            'hotel': 0.15,       # 15% hotels
            'residential': 0.15, # 15% residential
            'mixed': 0.05        # 5% mixed use
        }
        
        # Generate 150 buildings
        for i in range(150):
            lat = np.random.uniform(self.bounds['south'], self.bounds['north'])
            lon = np.random.uniform(self.bounds['west'], self.bounds['east'])
            
            # Select building type based on distribution
            btype = np.random.choice(list(building_types.keys()), 
                                    p=list(building_types.values()))
            
            # Building characteristics based on type
            if btype == 'office':
                floors = np.random.randint(20, 60)
                area_per_floor = np.random.uniform(1000, 3000)
                base_load_w_per_m2 = 100
            elif btype == 'retail':
                floors = np.random.randint(1, 5)
                area_per_floor = np.random.uniform(500, 2000)
                base_load_w_per_m2 = 150
            elif btype == 'hotel':
                floors = np.random.randint(15, 40)
                area_per_floor = np.random.uniform(800, 1500)
                base_load_w_per_m2 = 80
            elif btype == 'residential':
                floors = np.random.randint(10, 30)
                area_per_floor = np.random.uniform(600, 1200)
                base_load_w_per_m2 = 40
            else:  # mixed
                floors = np.random.randint(10, 25)
                area_per_floor = np.random.uniform(800, 1800)
                base_load_w_per_m2 = 75
            
            total_area = floors * area_per_floor
            base_load_kw = (total_area * base_load_w_per_m2) / 1000
            
            buildings.append({
                'building_id': f'BLD_{i}',
                'lat': lat,
                'lon': lon,
                'type': btype,
                'floors': floors,
                'area_m2': total_area,
                'base_load_kw': base_load_kw,
                'peak_load_kw': base_load_kw * 1.5,
                'has_backup_power': floors > 20,  # Tall buildings have backup
                'solar_pv_kw': 50 if floors > 10 and np.random.random() > 0.7 else 0
            })
        
        df = pd.DataFrame(buildings)
        df.to_csv('data/power/buildings.csv', index=False)
        
        total_base_load = df['base_load_kw'].sum() / 1000
        total_peak_load = df['peak_load_kw'].sum() / 1000
        buildings_with_solar = (df['solar_pv_kw'] > 0).sum()
        
        print(f"   ✅ Generated {len(buildings)} buildings")
        print(f"      • Base load: {total_base_load:.1f} MW")
        print(f"      • Peak load: {total_peak_load:.1f} MW")
        print(f"      • Buildings with solar: {buildings_with_solar}")
        print(f"      • Buildings with backup power: {df['has_backup_power'].sum()}")
        
    def create_scenarios(self):
        """Create scenario configuration files"""
        scenarios = {
            'normal_day': {
                'name': 'Normal Day',
                'description': 'Typical weekday traffic and power patterns',
                'start_time': '14:00',
                'vehicle_multiplier': 1.0,
                'power_multiplier': 1.0,
                'ev_charge_probability': 0.3,
                'incidents': []
            },
            'rush_hour': {
                'name': 'Morning Rush Hour',
                'description': 'Peak morning traffic with high power demand',
                'start_time': '08:00',
                'vehicle_multiplier': 2.0,
                'power_multiplier': 1.3,
                'ev_charge_probability': 0.1,
                'incidents': []
            },
            'blackout': {
                'name': 'Substation Failure',
                'description': 'Grand Central substation fails causing cascading blackout',
                'start_time': '15:00',
                'vehicle_multiplier': 1.0,
                'power_multiplier': 1.0,
                'ev_charge_probability': 0.5,
                'incidents': [
                    {
                        'time': 300,
                        'type': 'power_failure',
                        'location': 'GrandCentral_Sub',
                        'duration': 1800
                    }
                ]
            },
            'traffic_incident': {
                'name': 'Times Square Incident',
                'description': 'Major traffic incident blocks Times Square',
                'start_time': '12:00',
                'vehicle_multiplier': 1.2,
                'power_multiplier': 1.0,
                'ev_charge_probability': 0.3,
                'incidents': [
                    {
                        'time': 600,
                        'type': 'road_closure',
                        'location': {'lat': 40.7580, 'lon': -73.9855},
                        'radius': 200,
                        'duration': 2400
                    }
                ]
            },
            'heat_wave': {
                'name': 'Extreme Heat Wave',
                'description': 'High temperature causes increased AC load and EV range reduction',
                'start_time': '14:00',
                'vehicle_multiplier': 0.8,
                'power_multiplier': 1.5,
                'ev_charge_probability': 0.5,
                'temperature_c': 38,
                'ev_range_reduction': 0.15,
                'incidents': []
            }
        }
        
        # Save each scenario
        for scenario_id, config in scenarios.items():
            filepath = f'data/scenarios/{scenario_id}.json'
            with open(filepath, 'w') as f:
                json.dump(config, f, indent=2)
        
        print(f"   ✅ Created {len(scenarios)} scenarios")
        for sid, config in scenarios.items():
            print(f"      • {config['name']}: {config['description'][:40]}...")
        
    def validate_data(self):
        """Validate all created data files"""
        required_files = [
            'data/traffic/nodes.csv',
            'data/traffic/edges.csv',
            'data/traffic/signals.csv',
            'data/power/ev_stations.csv',
            'data/power/substations.csv',
            'data/power/transformers.csv',
            'data/power/lines.csv',
            'data/power/buildings.csv'
        ]
        
        all_valid = True
        for filepath in required_files:
            if Path(filepath).exists():
                df = pd.read_csv(filepath)
                print(f"   ✅ {filepath}: {len(df)} records")
            else:
                print(f"   ❌ {filepath}: MISSING")
                all_valid = False
        
        if all_valid:
            print("\n   🎉 All data files validated successfully!")
        else:
            print("\n   ⚠️ Some files are missing - check errors above")
        
        return all_valid
    
    def print_summary(self):
        """Print summary of all fetched data"""
        duration = (datetime.now() - self.start_time).total_seconds()
        
        print("\n" + "="*70)
        print(" DATA FETCHING COMPLETE ".center(70, "="))
        print("="*70)
        
        # Load and summarize data
        try:
            nodes = pd.read_csv('data/traffic/nodes.csv')
            edges = pd.read_csv('data/traffic/edges.csv')
            signals = pd.read_csv('data/traffic/signals.csv')
            ev_stations = pd.read_csv('data/power/ev_stations.csv')
            buildings = pd.read_csv('data/power/buildings.csv')
            substations = pd.read_csv('data/power/substations.csv')
            
            print(f"\n📊 DATA SUMMARY:")
            print(f"\n   🚦 Traffic Network:")
            print(f"      • Intersections: {len(nodes)}")
            print(f"      • Road segments: {len(edges)}")
            print(f"      • Traffic signals: {len(signals)}")
            
            print(f"\n   ⚡ Power Infrastructure:")
            print(f"      • Substations: {len(substations)}")
            print(f"      • EV stations: {len(ev_stations)}")
            print(f"      • Buildings: {len(buildings)}")
            print(f"      • Total building load: {buildings['base_load_kw'].sum()/1000:.1f} MW")
            
            print(f"\n   📁 Files created in 'data/' directory")
            print(f"   ⏱️ Time taken: {duration:.1f} seconds")
            
        except Exception as e:
            print(f"\n⚠️ Could not generate summary: {e}")
        
        print("\n" + "="*70)
        print("✅ Ready to proceed with SUMO network generation!")
        print("="*70)

if __name__ == "__main__":
    fetcher = ManhattanDataFetcher()
    fetcher.fetch_all()
    print("\n🎯 Next step: Generate SUMO network from this data")
    input("\nPress Enter to continue...")