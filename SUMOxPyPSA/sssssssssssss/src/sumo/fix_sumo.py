"""
Fix SUMO configuration to enable proper coupling
"""

import os
import xml.etree.ElementTree as ET

def fix_sumo_config():
    """Remove duplicate traffic light definitions"""
    
    print("\n🔧 FIXING SUMO CONFIGURATION...")
    
    # 1. Fix the main config file
    config_content = """<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <input>
        <net-file value="network.net.xml"/>
        <route-files value="routes_normal.rou.xml"/>
    </input>
    <time>
        <begin value="0"/>
        <end value="3600"/>
        <step-length value="1"/>
    </time>
    <processing>
        <lateral-resolution value="0.8"/>
        <collision.action value="warn"/>
        <time-to-teleport value="300"/>
    </processing>
    <report>
        <no-step-log value="true"/>
    </report>
</configuration>"""
    
    with open('data/sumo/manhattan_clean.sumocfg', 'w') as f:
        f.write(config_content)
    
    print("✅ Created clean config without additional files")
    
    # 2. Check if we have a valid network
    if not os.path.exists('data/sumo/network.net.xml'):
        print("⚠️  No network file found, creating minimal network...")
        create_minimal_network()
    
    # 3. Check routes
    if not os.path.exists('data/sumo/routes_normal.rou.xml'):
        print("⚠️  No routes found, creating test vehicles...")
        create_test_vehicles()
    
    print("\n✅ SUMO should now work properly!")
    return True

def create_minimal_network():
    """Create a minimal working network"""
    network_content = """<?xml version="1.0" encoding="UTF-8"?>
<net version="1.16" junctionCornerDetail="5" limitTurnSpeed="5.50">
    <location netOffset="0.00,0.00" convBoundary="-8244.00,-4497.00,-7244.00,-3497.00" 
              origBoundary="-73.9897,40.7527,-73.9734,40.7614" projParameter="!"/>
    
    <edge id="e1" from="n1" to="n2" priority="1">
        <lane id="e1_0" index="0" speed="13.89" length="100.00" shape="0,0 100,0"/>
    </edge>
    <edge id="e2" from="n2" to="n3" priority="1">
        <lane id="e2_0" index="0" speed="13.89" length="100.00" shape="100,0 200,0"/>
    </edge>
    <edge id="e3" from="n3" to="n4" priority="1">
        <lane id="e3_0" index="0" speed="13.89" length="100.00" shape="200,0 300,0"/>
    </edge>
    
    <junction id="n1" type="priority" x="0" y="0" incLanes="" intLanes="" shape="0,-1.6 0,1.6"/>
    <junction id="n2" type="priority" x="100" y="0" incLanes="e1_0" intLanes="" shape="100,-1.6 100,1.6"/>
    <junction id="n3" type="priority" x="200" y="0" incLanes="e2_0" intLanes="" shape="200,-1.6 200,1.6"/>
    <junction id="n4" type="priority" x="300" y="0" incLanes="e3_0" intLanes="" shape="300,-1.6 300,1.6"/>
</net>"""
    
    with open('data/sumo/network.net.xml', 'w') as f:
        f.write(network_content)
    print("✅ Created minimal network")

def create_test_vehicles():
    """Create test vehicles with EVs"""
    routes_content = """<?xml version="1.0" encoding="UTF-8"?>
<routes>
    <vType id="car" accel="2.6" decel="4.5" length="5" maxSpeed="30" sigma="0.5" color="1,1,0"/>
    <vType id="ev" accel="3.0" decel="4.5" length="5" maxSpeed="28" sigma="0.5" color="0,1,0">
        <param key="has.battery.device" value="true"/>
        <param key="maximumBatteryCapacity" value="60000"/>
        <param key="actualBatteryCapacity" value="45000"/>
    </vType>
    
    <route id="r1" edges="e1 e2 e3"/>
    
    <flow id="flow_cars" type="car" route="r1" begin="0" end="3600" number="100" departLane="best"/>
    <flow id="flow_evs" type="ev" route="r1" begin="0" end="3600" number="20" departLane="best"/>
</routes>"""
    
    with open('data/sumo/routes_normal.rou.xml', 'w') as f:
        f.write(routes_content)
    print("✅ Created test vehicles including EVs")

if __name__ == "__main__":
    fix_sumo_config()