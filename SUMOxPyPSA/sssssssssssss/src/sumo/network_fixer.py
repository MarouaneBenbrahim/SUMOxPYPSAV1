"""
SUMO Network Fixer - Diagnoses and fixes network visibility issues
"""

import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
import pandas as pd
import subprocess

class SUMONetworkFixer:
    def __init__(self):
        print("\n" + "="*70)
        print(" SUMO NETWORK DIAGNOSTIC & FIXER ".center(70, "="))
        print("="*70)
        
    def diagnose(self):
        """Diagnose network issues"""
        print("\n🔍 DIAGNOSING NETWORK...\n")
        
        issues_found = False
        
        # Check 1: Files exist
        print("[1] Checking files...")
        required_files = [
            'data/sumo/network.net.xml',
            'data/sumo/manhattan.sumocfg',
            'data/sumo/nodes.nod.xml',
            'data/sumo/edges.edg.xml'
        ]
        
        for file in required_files:
            if Path(file).exists():
                size = Path(file).stat().st_size
                if size == 0:
                    print(f"   ❌ {file} is empty!")
                    issues_found = True
                else:
                    print(f"   ✅ {file} ({size/1024:.1f} KB)")
            else:
                print(f"   ❌ {file} missing!")
                issues_found = True
        
        # Check 2: Network content
        if Path('data/sumo/network.net.xml').exists():
            print("\n[2] Checking network content...")
            try:
                tree = ET.parse('data/sumo/network.net.xml')
                root = tree.getroot()
                
                junctions = root.findall('.//junction')
                edges = root.findall('.//edge')
                
                print(f"   • Junctions in network: {len(junctions)}")
                print(f"   • Edges in network: {len(edges)}")
                
                if len(junctions) == 0:
                    print("   ❌ No junctions found!")
                    issues_found = True
                    
                if len(edges) == 0:
                    print("   ❌ No edges found!")
                    issues_found = True
                    
            except Exception as e:
                print(f"   ❌ Could not parse network: {e}")
                issues_found = True
        
        return issues_found
    
    def create_simple_test_network(self):
        """Create a simple test network that definitely works"""
        print("\n🔧 CREATING SIMPLE TEST NETWORK...\n")
        
        # Create simple nodes file
        nodes_content = '''<?xml version="1.0" encoding="UTF-8"?>
<nodes>
    <node id="0" x="0" y="0" type="priority"/>
    <node id="1" x="500" y="0" type="traffic_light"/>
    <node id="2" x="1000" y="0" type="priority"/>
    <node id="3" x="0" y="500" type="priority"/>
    <node id="4" x="500" y="500" type="traffic_light"/>
    <node id="5" x="1000" y="500" type="priority"/>
    <node id="6" x="0" y="1000" type="priority"/>
    <node id="7" x="500" y="1000" type="traffic_light"/>
    <node id="8" x="1000" y="1000" type="priority"/>
</nodes>'''
        
        with open('data/sumo/simple_nodes.nod.xml', 'w') as f:
            f.write(nodes_content)
        
        # Create simple edges file
        edges_content = '''<?xml version="1.0" encoding="UTF-8"?>
<edges>
    <edge id="e0" from="0" to="1" numLanes="2" speed="13.89"/>
    <edge id="e1" from="1" to="2" numLanes="2" speed="13.89"/>
    <edge id="e2" from="3" to="4" numLanes="2" speed="13.89"/>
    <edge id="e3" from="4" to="5" numLanes="2" speed="13.89"/>
    <edge id="e4" from="6" to="7" numLanes="2" speed="13.89"/>
    <edge id="e5" from="7" to="8" numLanes="2" speed="13.89"/>
    <edge id="e6" from="0" to="3" numLanes="2" speed="13.89"/>
    <edge id="e7" from="3" to="6" numLanes="2" speed="13.89"/>
    <edge id="e8" from="1" to="4" numLanes="2" speed="13.89"/>
    <edge id="e9" from="4" to="7" numLanes="2" speed="13.89"/>
    <edge id="e10" from="2" to="5" numLanes="2" speed="13.89"/>
    <edge id="e11" from="5" to="8" numLanes="2" speed="13.89"/>
</edges>'''
        
        with open('data/sumo/simple_edges.edg.xml', 'w') as f:
            f.write(edges_content)
        
        print("   ✅ Created simple node and edge files")
        
        # Run netconvert
        print("   🔄 Running netconvert...")
        cmd = [
            'netconvert',
            '--node-files=data/sumo/simple_nodes.nod.xml',
            '--edge-files=data/sumo/simple_edges.edg.xml',
            '--output-file=data/sumo/simple_network.net.xml',
            '--no-internal-links=false',
            '--no-turnarounds=true'
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                print("   ✅ Simple network generated!")
            else:
                print(f"   ⚠️ Netconvert output: {result.stderr[:200]}")
        except:
            print("   ⚠️ Could not run netconvert")
        
        # Create simple routes
        routes_content = '''<?xml version="1.0" encoding="UTF-8"?>
<routes>
    <vType id="car" length="5" maxSpeed="30" accel="2.6" decel="4.5" sigma="0.5"/>
    <route id="r0" edges="e0 e1"/>
    <route id="r1" edges="e2 e3"/>
    <route id="r2" edges="e6 e7"/>
    
    <flow id="flow0" type="car" route="r0" begin="0" end="3600" number="100"/>
    <flow id="flow1" type="car" route="r1" begin="0" end="3600" number="100"/>
    <flow id="flow2" type="car" route="r2" begin="0" end="3600" number="100"/>
</routes>'''
        
        with open('data/sumo/simple_routes.rou.xml', 'w') as f:
            f.write(routes_content)
        
        # Create simple config
        config_content = '''<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <input>
        <net-file value="simple_network.net.xml"/>
        <route-files value="simple_routes.rou.xml"/>
    </input>
    <time>
        <begin value="0"/>
        <end value="3600"/>
    </time>
    <report>
        <no-step-log value="true"/>
    </report>
</configuration>'''
        
        with open('data/sumo/simple.sumocfg', 'w') as f:
            f.write(config_content)
        
        print("   ✅ Created simple configuration")
        print("\n📊 Simple test network ready!")
        print("\nTry running: sumo-gui -c data\\sumo\\simple.sumocfg")
    
    def rebuild_manhattan_network(self):
        """Rebuild Manhattan network with better coordinates"""
        print("\n🔨 REBUILDING MANHATTAN NETWORK...\n")
        
        # Load original data
        nodes_df = pd.read_csv('data/traffic/nodes.csv')
        edges_df = pd.read_csv('data/traffic/edges.csv')
        
        # Create nodes with better scaling
        nodes_content = '<?xml version="1.0" encoding="UTF-8"?>\n<nodes>\n'
        
        # Scale coordinates to reasonable values
        min_x = nodes_df['lon'].min()
        min_y = nodes_df['lat'].min()
        
        for _, node in nodes_df.iterrows():
            # Convert to meters from origin
            x = (node['lon'] - min_x) * 111000 * 0.87  # cos(40.75 degrees) for longitude
            y = (node['lat'] - min_y) * 111000
            
            node_type = 'traffic_light' if _ % 3 == 0 else 'priority'
            nodes_content += f'    <node id="{int(node["node_id"])}" x="{x:.2f}" y="{y:.2f}" type="{node_type}"/>\n'
        
        nodes_content += '</nodes>'
        
        with open('data/sumo/manhattan_nodes.nod.xml', 'w') as f:
            f.write(nodes_content)
        
        print(f"   ✅ Created {len(nodes_df)} nodes with proper coordinates")
        
        # Create edges
        edges_content = '<?xml version="1.0" encoding="UTF-8"?>\n<edges>\n'
        
        for _, edge in edges_df.iterrows():
            edges_content += f'    <edge id="e{int(edge["edge_id"])}" from="{int(edge["from_node"])}" to="{int(edge["to_node"])}" numLanes="{int(edge["lanes"])}" speed="{edge["speed_limit"]:.2f}"/>\n'
            # Add reverse edge
            edges_content += f'    <edge id="e{int(edge["edge_id"])}_r" from="{int(edge["to_node"])}" to="{int(edge["from_node"])}" numLanes="{int(edge["lanes"])}" speed="{edge["speed_limit"]:.2f}"/>\n'
        
        edges_content += '</edges>'
        
        with open('data/sumo/manhattan_edges.edg.xml', 'w') as f:
            f.write(edges_content)
        
        print(f"   ✅ Created {len(edges_df)*2} edges (bidirectional)")
        
        # Run netconvert with proper settings
        print("   🔄 Running netconvert with Manhattan data...")
        cmd = [
            'netconvert',
            '--node-files=data/sumo/manhattan_nodes.nod.xml',
            '--edge-files=data/sumo/manhattan_edges.edg.xml',
            '--output-file=data/sumo/manhattan_network.net.xml',
            '--no-turnarounds=true',
            '--no-internal-links=false',
            '--junctions.join=true',
            '--tls.guess=true',
            '--tls.default-type=actuated'
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                print("   ✅ Manhattan network rebuilt successfully!")
                
                # Update config to use new network
                self.update_config()
            else:
                print(f"   ⚠️ Warning: {result.stderr[:200]}")
        except Exception as e:
            print(f"   ⚠️ Could not run netconvert: {e}")
    
    def update_config(self):
        """Update SUMO config to use rebuilt network"""
        config_content = '''<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <input>
        <net-file value="manhattan_network.net.xml"/>
        <route-files value="vehicle_types.rou.xml"/>
    </input>
    <time>
        <begin value="0"/>
        <end value="3600"/>
        <step-length value="1"/>
    </time>
    <processing>
        <collision.action value="warn"/>
        <time-to-teleport value="300"/>
    </processing>
    <report>
        <no-step-log value="true"/>
        <verbose value="true"/>
    </report>
    <gui_only>
        <start value="true"/>
        <delay value="100"/>
    </gui_only>
</configuration>'''
        
        with open('data/sumo/manhattan_fixed.sumocfg', 'w') as f:
            f.write(config_content)
        
        print("   ✅ Updated configuration file")

def main():
    fixer = SUMONetworkFixer()
    
    # First diagnose
    has_issues = fixer.diagnose()
    
    if has_issues:
        print("\n⚠️ Issues found with network!")
        
        # Create simple test network
        print("\n[Option 1] Creating simple test network...")
        fixer.create_simple_test_network()
        
        # Rebuild Manhattan network
        print("\n[Option 2] Rebuilding Manhattan network...")
        fixer.rebuild_manhattan_network()
        
        print("\n" + "="*70)
        print("✅ FIXES APPLIED!")
        print("="*70)
        print("\nYou can now try:")
        print("1. Simple test: sumo-gui -c data\\sumo\\simple.sumocfg")
        print("2. Manhattan: sumo-gui -c data\\sumo\\manhattan_fixed.sumocfg")
    else:
        print("\n✅ No major issues found!")
        print("\nIf you still can't see the network, try:")
        print("1. In SUMO-GUI: View -> Center View")
        print("2. Or press 'Ctrl+J' to fit view to network")
        print("3. Or use mouse wheel to zoom out")

if __name__ == "__main__":
    main()
    input("\nPress Enter to continue...")