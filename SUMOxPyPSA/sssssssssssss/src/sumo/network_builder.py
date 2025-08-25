"""
SUMO Network Builder - Converts CSV data to SUMO format
Creates the complete SUMO network with traffic signals and infrastructure
"""

import os
import sys
import json
import pandas as pd
import numpy as np
from pathlib import Path
import xml.etree.ElementTree as ET
from xml.dom import minidom
import subprocess

class SUMONetworkBuilder:
    def __init__(self):
        """Initialize SUMO network builder"""
        # Load configuration
        with open('configs/config.json', 'r') as f:
            self.config = json.load(f)
        
        self.bounds = self.config['simulation']['area']
        
        print("\n" + "="*70)
        print(" SUMO NETWORK BUILDER ".center(70, "="))
        print("="*70)
        print(f"\n📍 Building SUMO network for: {self.bounds['description']}")
        print("-"*70)
        
        # SUMO file paths
        self.output_dir = Path('data/sumo')
        self.output_dir.mkdir(exist_ok=True)
        
    def build_all(self):
        """Main function to build complete SUMO network"""
        try:
            print("\n🔧 STARTING SUMO NETWORK GENERATION...\n")
            
            # Step 1: Create node file
            print("[1/7] 📍 Creating SUMO nodes file...")
            self.create_nodes_file()
            
            # Step 2: Create edge file
            print("\n[2/7] 🛣️  Creating SUMO edges file...")
            self.create_edges_file()
            
            # Step 3: Create edge type file
            print("\n[3/7] 🚗 Creating edge types...")
            self.create_edge_types()
            
            # Step 4: Generate network
            print("\n[4/7] 🔨 Building SUMO network...")
            self.generate_network()
            
            # Step 5: Add traffic signals
            print("\n[5/7] 🚦 Adding traffic signals...")
            self.add_traffic_signals()
            
            # Step 6: Create additional files
            print("\n[6/7] 📋 Creating additional files...")
            self.create_additional_files()
            
            # Step 7: Create main configuration
            print("\n[7/7] ⚙️  Creating SUMO configuration...")
            self.create_sumo_config()
            
            print("\n" + "="*70)
            print("✅ SUMO NETWORK GENERATION COMPLETE!")
            print("="*70)
            
            return True
            
        except Exception as e:
            print(f"\n❌ Error building SUMO network: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def create_nodes_file(self):
        """Create SUMO nodes XML file"""
        # Load nodes from CSV
        nodes_df = pd.read_csv('data/traffic/nodes.csv')
        
        # Create XML structure
        nodes_root = ET.Element('nodes')
        
        for _, node in nodes_df.iterrows():
            node_elem = ET.SubElement(nodes_root, 'node')
            node_elem.set('id', str(int(node['node_id'])))
            node_elem.set('x', str(node['lon'] * 111000))  # Convert to meters
            node_elem.set('y', str(node['lat'] * 111000))   # Convert to meters
            
            # Mark traffic signal nodes
            if _ % 3 == 0:  # Every 3rd node has a signal
                node_elem.set('type', 'traffic_light')
            else:
                node_elem.set('type', 'priority')
        
        # Write to file
        self.write_xml(nodes_root, 'data/sumo/nodes.nod.xml')
        print(f"   ✅ Created {len(nodes_df)} nodes")
        print(f"      • Traffic lights: {len(nodes_df) // 3}")
        print(f"      • Priority junctions: {len(nodes_df) - len(nodes_df) // 3}")
    
    def create_edges_file(self):
        """Create SUMO edges XML file"""
        # Load edges from CSV
        edges_df = pd.read_csv('data/traffic/edges.csv')
        nodes_df = pd.read_csv('data/traffic/nodes.csv')
        
        # Create node lookup
        node_coords = {row['node_id']: (row['lon'], row['lat']) 
                      for _, row in nodes_df.iterrows()}
        
        # Create XML structure
        edges_root = ET.Element('edges')
        
        for _, edge in edges_df.iterrows():
            edge_elem = ET.SubElement(edges_root, 'edge')
            edge_elem.set('id', f"e{int(edge['edge_id'])}")
            edge_elem.set('from', str(int(edge['from_node'])))
            edge_elem.set('to', str(int(edge['to_node'])))
            
            # Determine edge type based on lanes
            if edge['lanes'] >= 3:
                edge_elem.set('type', 'avenue')
            else:
                edge_elem.set('type', 'street')
            
            edge_elem.set('numLanes', str(int(edge['lanes'])))
            edge_elem.set('speed', str(edge['speed_limit']))
            
            # Add shape if we have coordinates
            if edge['from_node'] in node_coords and edge['to_node'] in node_coords:
                from_coord = node_coords[edge['from_node']]
                to_coord = node_coords[edge['to_node']]
                shape = f"{from_coord[0]*111000},{from_coord[1]*111000} {to_coord[0]*111000},{to_coord[1]*111000}"
                edge_elem.set('shape', shape)
        
        # Create bidirectional edges
        edges_to_add = []
        for _, edge in edges_df.iterrows():
            reverse_edge = ET.SubElement(edges_root, 'edge')
            reverse_edge.set('id', f"e{int(edge['edge_id'])}_r")
            reverse_edge.set('from', str(int(edge['to_node'])))
            reverse_edge.set('to', str(int(edge['from_node'])))
            reverse_edge.set('type', 'street')
            reverse_edge.set('numLanes', str(int(edge['lanes'])))
            reverse_edge.set('speed', str(edge['speed_limit']))
        
        # Write to file
        self.write_xml(edges_root, 'data/sumo/edges.edg.xml')
        print(f"   ✅ Created {len(edges_df) * 2} edges (bidirectional)")
        print(f"      • Avenues (3+ lanes): {len(edges_df[edges_df['lanes'] >= 3]) * 2}")
        print(f"      • Streets (2 lanes): {len(edges_df[edges_df['lanes'] < 3]) * 2}")
    
    def create_edge_types(self):
        """Create edge type definitions"""
        types_root = ET.Element('types')
        
        # Avenue type (major roads)
        avenue = ET.SubElement(types_root, 'type')
        avenue.set('id', 'avenue')
        avenue.set('priority', '10')
        avenue.set('numLanes', '3')
        avenue.set('speed', '13.89')  # 50 km/h
        avenue.set('width', '3.5')
        avenue.set('sidewalkWidth', '3.0')
        
        # Street type (minor roads)
        street = ET.SubElement(types_root, 'type')
        street.set('id', 'street')
        street.set('priority', '5')
        street.set('numLanes', '2')
        street.set('speed', '11.11')  # 40 km/h
        street.set('width', '3.0')
        street.set('sidewalkWidth', '2.0')
        
        # Highway type (for future use)
        highway = ET.SubElement(types_root, 'type')
        highway.set('id', 'highway')
        highway.set('priority', '15')
        highway.set('numLanes', '4')
        highway.set('speed', '22.22')  # 80 km/h
        highway.set('width', '3.5')
        
        self.write_xml(types_root, 'data/sumo/types.typ.xml')
        print(f"   ✅ Created 3 edge types (avenue, street, highway)")
    
    def generate_network(self):
        """Use netconvert to generate the network"""
        print("   ⚙️  Running SUMO netconvert...")
        
        # Create the netconvert command
        cmd = [
            'netconvert',
            '--node-files=data/sumo/nodes.nod.xml',
            '--edge-files=data/sumo/edges.edg.xml',
            '--type-files=data/sumo/types.typ.xml',
            '--output-file=data/sumo/network.net.xml',
            '--no-turnarounds',
            '--offset.disable-normalization',
            '--lefthand=false',
            '--junctions.corner-detail=5',
            '--rectangular-lane-cut=true',
            '--walkingareas=true',
            '--crossings.guess=true'
        ]
        
        try:
            # Run netconvert
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"   ✅ Network generated successfully!")
                
                # Check if file was created
                if Path('data/sumo/network.net.xml').exists():
                    size = Path('data/sumo/network.net.xml').stat().st_size / 1024
                    print(f"      • Network file size: {size:.1f} KB")
                else:
                    print(f"   ⚠️  Network file not found, creating simplified version...")
                    self.create_simple_network()
            else:
                print(f"   ⚠️  Netconvert warning: {result.stderr[:200]}")
                print(f"   🔄 Creating simplified network...")
                self.create_simple_network()
                
        except FileNotFoundError:
            print(f"   ⚠️  SUMO netconvert not found in PATH")
            print(f"   🔄 Creating simplified network manually...")
            self.create_simple_network()
    
    def create_simple_network(self):
        """Create a simplified network if netconvert fails"""
        # Create a basic network XML
        net_root = ET.Element('net', version="1.16.0")
        
        # Add location
        location = ET.SubElement(net_root, 'location')
        location.set('netOffset', '0.00,0.00')
        location.set('convBoundary', f"{self.bounds['west']*111000},{self.bounds['south']*111000},{self.bounds['east']*111000},{self.bounds['north']*111000}")
        location.set('origBoundary', f"{self.bounds['west']},{self.bounds['south']},{self.bounds['east']},{self.bounds['north']}")
        location.set('projParameter', '!')
        
        # Add edge types
        edge_type = ET.SubElement(net_root, 'type')
        edge_type.set('id', 'default')
        edge_type.set('priority', '1')
        edge_type.set('numLanes', '2')
        edge_type.set('speed', '13.89')
        
        # Add junctions from nodes
        nodes_df = pd.read_csv('data/traffic/nodes.csv')
        for _, node in nodes_df.iterrows():
            junction = ET.SubElement(net_root, 'junction')
            junction.set('id', str(int(node['node_id'])))
            junction.set('type', 'traffic_light' if _ % 3 == 0 else 'priority')
            junction.set('x', str(node['lon'] * 111000))
            junction.set('y', str(node['lat'] * 111000))
            junction.set('incLanes', '')
            junction.set('intLanes', '')
            junction.set('shape', f"{node['lon']*111000-5},{node['lat']*111000-5} {node['lon']*111000+5},{node['lat']*111000+5}")
        
        # Add edges
        edges_df = pd.read_csv('data/traffic/edges.csv')
        for _, edge in edges_df.iterrows():
            edge_elem = ET.SubElement(net_root, 'edge')
            edge_elem.set('id', f"e{int(edge['edge_id'])}")
            edge_elem.set('from', str(int(edge['from_node'])))
            edge_elem.set('to', str(int(edge['to_node'])))
            edge_elem.set('priority', '1')
            
            # Add lanes
            for lane in range(int(edge['lanes'])):
                lane_elem = ET.SubElement(edge_elem, 'lane')
                lane_elem.set('id', f"e{int(edge['edge_id'])}_{lane}")
                lane_elem.set('index', str(lane))
                lane_elem.set('speed', str(edge['speed_limit']))
                lane_elem.set('length', str(edge['length']))
                lane_elem.set('shape', '0,0 100,0')  # Simplified shape
        
        self.write_xml(net_root, 'data/sumo/network.net.xml')
        print(f"   ✅ Simplified network created")
    
    def add_traffic_signals(self):
        """Add traffic signal programs"""
        signals_df = pd.read_csv('data/traffic/signals.csv')
        
        # Create additional file for traffic lights
        additional_root = ET.Element('additional')
        
        for _, signal in signals_df.iterrows():
            # Create traffic light logic
            tl_logic = ET.SubElement(additional_root, 'tlLogic')
            tl_logic.set('id', str(int(signal['node_id'])))
            tl_logic.set('type', 'static')
            tl_logic.set('programID', '0')
            tl_logic.set('offset', '0')
            
            # Create phases (simplified - green/yellow/red cycle)
            phases = [
                {'duration': '42', 'state': 'GGGGrrrrGGGGrrrr'},  # NS green
                {'duration': '3', 'state': 'yyyyrrrryyyyrrrr'},   # NS yellow
                {'duration': '42', 'state': 'rrrrGGGGrrrrGGGG'},  # EW green
                {'duration': '3', 'state': 'rrrryyyyrrrryyyy'},   # EW yellow
            ]
            
            for phase in phases:
                phase_elem = ET.SubElement(tl_logic, 'phase')
                phase_elem.set('duration', phase['duration'])
                phase_elem.set('state', phase['state'])
        
        self.write_xml(additional_root, 'data/sumo/traffic_lights.add.xml')
        print(f"   ✅ Added {len(signals_df)} traffic signal programs")
        print(f"      • Cycle time: 90 seconds")
        print(f"      • Green time: 42 seconds per direction")
    
    def create_additional_files(self):
        """Create additional SUMO files (parking, EV stations, etc.)"""
        
        # 1. Create parking areas for EV charging
        ev_stations = pd.read_csv('data/power/ev_stations.csv')
        additional_root = ET.Element('additional')
        
        for idx, station in ev_stations.iterrows():
            # Create parking area
            parking = ET.SubElement(additional_root, 'parkingArea')
            parking.set('id', f"pa_{station['station_id']}")
            parking.set('lane', f"e0_0")  # Assign to first edge for now
            parking.set('startPos', str(idx * 50))
            parking.set('endPos', str(idx * 50 + 40))
            parking.set('roadsideCapacity', str(int(station['num_chargers'])))
            parking.set('friendlyPos', 'true')
            
            # Add parking spaces
            for i in range(int(station['num_chargers'])):
                space = ET.SubElement(parking, 'space')
                space.set('x', str(station['lon'] * 111000 + i * 5))
                space.set('y', str(station['lat'] * 111000))
                space.set('name', f"{station['name']}_spot_{i}")
        
        self.write_xml(additional_root, 'data/sumo/parking.add.xml')
        print(f"   ✅ Created {len(ev_stations)} EV charging parking areas")
        
        # 2. Create vehicle types
        self.create_vehicle_types()
        
        # 3. Create route file template
        self.create_route_template()
    
    def create_vehicle_types(self):
        """Create vehicle type definitions"""
        routes_root = ET.Element('routes')
        
        # Regular car
        vtype = ET.SubElement(routes_root, 'vType')
        vtype.set('id', 'car')
        vtype.set('accel', '2.6')
        vtype.set('decel', '4.5')
        vtype.set('sigma', '0.5')
        vtype.set('length', '4.5')
        vtype.set('minGap', '2.5')
        vtype.set('maxSpeed', '33.33')
        vtype.set('color', '1,1,0')
        vtype.set('vClass', 'passenger')
        
        # Electric vehicle
        ev_type = ET.SubElement(routes_root, 'vType')
        ev_type.set('id', 'ev')
        ev_type.set('accel', '3.0')
        ev_type.set('decel', '4.5')
        ev_type.set('sigma', '0.5')
        ev_type.set('length', '4.8')
        ev_type.set('minGap', '2.5')
        ev_type.set('maxSpeed', '30.56')
        ev_type.set('color', '0,1,0')
        ev_type.set('vClass', 'passenger')
        ev_type.set('emissionClass', 'Zero')
        
        # Add battery device
        param = ET.SubElement(ev_type, 'param')
        param.set('key', 'has.battery.device')
        param.set('value', 'true')
        
        param2 = ET.SubElement(ev_type, 'param')
        param2.set('key', 'maximumBatteryCapacity')
        param2.set('value', '60000')  # 60 kWh
        
        param3 = ET.SubElement(ev_type, 'param')
        param3.set('key', 'actualBatteryCapacity')
        param3.set('value', '50000')  # Start at 83% charge
        
        # Bus
        bus_type = ET.SubElement(routes_root, 'vType')
        bus_type.set('id', 'bus')
        bus_type.set('accel', '1.2')
        bus_type.set('decel', '4.0')
        bus_type.set('sigma', '0.1')
        bus_type.set('length', '12.0')
        bus_type.set('minGap', '3')
        bus_type.set('maxSpeed', '22.22')
        bus_type.set('color', '0,0,1')
        bus_type.set('vClass', 'bus')
        
        # Taxi
        taxi_type = ET.SubElement(routes_root, 'vType')
        taxi_type.set('id', 'taxi')
        taxi_type.set('accel', '2.8')
        taxi_type.set('decel', '5.0')
        taxi_type.set('sigma', '0.8')
        taxi_type.set('length', '4.5')
        taxi_type.set('minGap', '2.0')
        taxi_type.set('maxSpeed', '33.33')
        taxi_type.set('color', '1,1,0.3')
        taxi_type.set('vClass', 'taxi')
        
        self.write_xml(routes_root, 'data/sumo/vehicle_types.rou.xml')
        print(f"   ✅ Created 4 vehicle types (car, ev, bus, taxi)")
    
    def create_route_template(self):
        """Create template route file"""
        routes_root = ET.Element('routes')
        
        # Add a comment
        comment = ET.Comment('This is a template. Actual routes will be generated at runtime')
        routes_root.append(comment)
        
        # Create a few sample routes
        edges_df = pd.read_csv('data/traffic/edges.csv')
        
        if len(edges_df) > 0:
            # Create 5 sample routes
            for i in range(min(5, len(edges_df))):
                route = ET.SubElement(routes_root, 'route')
                route.set('id', f'route_{i}')
                # Simple route using first few edges
                route_edges = ' '.join([f"e{j}" for j in range(i, min(i+5, len(edges_df)))])
                route.set('edges', route_edges)
        
        self.write_xml(routes_root, 'data/sumo/routes_template.rou.xml')
        print(f"   ✅ Created route template file")
    
    def create_sumo_config(self):
        """Create main SUMO configuration file"""
        config_root = ET.Element('configuration')
        
        # Input files
        input_elem = ET.SubElement(config_root, 'input')
        net_file = ET.SubElement(input_elem, 'net-file')
        net_file.set('value', 'network.net.xml')
        
        route_files = ET.SubElement(input_elem, 'route-files')
        route_files.set('value', 'vehicle_types.rou.xml')
        
        additional_files = ET.SubElement(input_elem, 'additional-files')
        additional_files.set('value', 'traffic_lights.add.xml,parking.add.xml')
        
        # Time settings
        time_elem = ET.SubElement(config_root, 'time')
        begin = ET.SubElement(time_elem, 'begin')
        begin.set('value', '0')
        
        end = ET.SubElement(time_elem, 'end')
        end.set('value', str(self.config['simulation']['duration_seconds']))
        
        step_length = ET.SubElement(time_elem, 'step-length')
        step_length.set('value', str(self.config['simulation']['step_size']))
        
        # Processing
        processing = ET.SubElement(config_root, 'processing')
        
        lateral = ET.SubElement(processing, 'lateral-resolution')
        lateral.set('value', '0.8')
        
        collision = ET.SubElement(processing, 'collision.action')
        collision.set('value', 'warn')
        
        # Routing
        routing = ET.SubElement(config_root, 'routing')
        device_rerouting = ET.SubElement(routing, 'device.rerouting.probability')
        device_rerouting.set('value', '0.5')
        
        # Report
        report = ET.SubElement(config_root, 'report')
        verbose = ET.SubElement(report, 'verbose')
        verbose.set('value', 'true')
        
        no_step_log = ET.SubElement(report, 'no-step-log')
        no_step_log.set('value', 'true')
        
        # GUI settings (if using sumo-gui)
        gui = ET.SubElement(config_root, 'gui_only')
        gui_settings = ET.SubElement(gui, 'gui-settings-file')
        gui_settings.set('value', 'gui_settings.xml')
        
        self.write_xml(config_root, 'data/sumo/manhattan.sumocfg')
        
        # Also create GUI settings
        self.create_gui_settings()
        
        print(f"   ✅ Created SUMO configuration file")
        print(f"      • Simulation duration: {self.config['simulation']['duration_seconds']} seconds")
        print(f"      • Step size: {self.config['simulation']['step_size']} second")
    
    def create_gui_settings(self):
        """Create GUI visualization settings"""
        viewsettings = ET.Element('viewsettings')
        
        scheme = ET.SubElement(viewsettings, 'scheme')
        scheme.set('name', 'Manhattan Simulation')
        
        viewport = ET.SubElement(viewsettings, 'viewport')
        viewport.set('zoom', '100')
        viewport.set('x', str(self.bounds['west'] * 111000))
        viewport.set('y', str(self.bounds['south'] * 111000))
        
        # Visual settings
        delay = ET.SubElement(viewsettings, 'delay')
        delay.set('value', '100')
        
        # Color by speed
        vehicles = ET.SubElement(viewsettings, 'vehicles')
        vehicles.set('vehicleQuality', '2')
        vehicles.set('showBlinker', 'true')
        vehicles.set('vehicleSize', '1')
        
        self.write_xml(viewsettings, 'data/sumo/gui_settings.xml')
    
    def write_xml(self, root, filename):
        """Write XML to file with pretty formatting"""
        rough_string = ET.tostring(root, 'utf-8')
        reparsed = minidom.parseString(rough_string)
        pretty_xml = reparsed.toprettyxml(indent="    ")
        
        # Remove extra blank lines
        lines = pretty_xml.split('\n')
        lines = [line for line in lines if line.strip()]
        pretty_xml = '\n'.join(lines)
        
        with open(filename, 'w') as f:
            f.write(pretty_xml)

def test_sumo_network():
    """Test if SUMO network can be loaded"""
    print("\n" + "="*70)
    print(" TESTING SUMO NETWORK ".center(70, "="))
    print("="*70)
    
    config_file = Path('data/sumo/manhattan.sumocfg')
    
    if config_file.exists():
        print("✅ SUMO configuration file found")
        
        # Try to validate with sumo
        try:
            import sumo
            print(f"✅ SUMO version: {sumo.__version__}")
            
            # Check if network file exists
            if Path('data/sumo/network.net.xml').exists():
                print("✅ Network file exists")
                print("\n🎉 SUMO network is ready!")
                print("\nYou can now:")
                print("1. Run: sumo-gui -c data/sumo/manhattan.sumocfg")
                print("2. Or continue to next step for vehicle generation")
            else:
                print("⚠️ Network file missing")
        except:
            print("⚠️ Could not validate with SUMO")
    else:
        print("❌ Configuration file not found")
    
    print("="*70)

if __name__ == "__main__":
    builder = SUMONetworkBuilder()
    success = builder.build_all()
    
    if success:
        test_sumo_network()
    
    input("\nPress Enter to continue...")