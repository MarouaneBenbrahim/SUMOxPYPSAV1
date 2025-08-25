#!/usr/bin/env python3
"""
PyPSA Network for Manhattan - All components within Manhattan bounds
Manhattan bounds: 40.700-40.800°N, -74.020--73.930°W
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime

class NYCPowerNetworkSimple:
    def __init__(self):
        """Initialize NYC Power Network - Manhattan Focus"""
        self.name = "Manhattan Power Grid"
        
        # Network components
        self.buses = {}
        self.generators = {}
        self.loads = {}
        self.lines = {}
        self.transformers = {}
        
        # Time series (24 hours)
        self.time_steps = pd.date_range('2025-01-01', periods=24, freq='h')
        self.current_hour = 0
        
        # Traffic-related loads
        self.traffic_light_loads = {}
        self.ev_charging_loads = {}
        self.street_light_loads = {}
        
        # Network state
        self.total_generation = 0
        self.total_load = 0
        self.line_flows = {}
        
    def build_network(self):
        """Build the Manhattan power network"""
        print("Building Manhattan-Focused Power Network...")
        
        self._add_buses()
        self._add_generators()
        self._add_lines()
        self._add_base_loads()
        self._add_traffic_infrastructure()
        
        print("Network built successfully!")
        return self
    
    def _add_buses(self):
        """Add electrical buses (substations) - ALL WITHIN MANHATTAN"""
        # Manhattan bounds: 40.700-40.800°N, -74.020--73.930°W
        
        self.buses = {
            # High voltage buses (138kV) - Main substations within Manhattan
            'Manhattan_Midtown': {
                'lat': 40.7580,  # Times Square area
                'lon': -73.9855,
                'voltage': 138,
                'type': 'transmission'
            },
            'Manhattan_Upper_West': {
                'lat': 40.7850,  # Upper West Side
                'lon': -74.0000,
                'voltage': 138,
                'type': 'transmission'
            },
            'Manhattan_Upper_East': {
                'lat': 40.7750,  # Upper East Side
                'lon': -73.9600,
                'voltage': 138,
                'type': 'transmission'
            },
            'Manhattan_Lower': {
                'lat': 40.7150,  # Lower Manhattan/Financial District
                'lon': -74.0100,
                'voltage': 138,
                'type': 'transmission'
            },
            'Manhattan_Chelsea': {
                'lat': 40.7450,  # Chelsea/Greenwich Village
                'lon': -74.0000,
                'voltage': 138,
                'type': 'transmission'
            },
            
            # Distribution buses (13.8kV) for traffic infrastructure - spread across Manhattan
            'Traffic_Times_Square': {
                'lat': 40.7580,
                'lon': -73.9855,
                'voltage': 13.8,
                'type': 'distribution'
            },
            'Traffic_Central_Park_South': {
                'lat': 40.7650,
                'lon': -73.9750,
                'voltage': 13.8,
                'type': 'distribution'
            },
            'Traffic_Union_Square': {
                'lat': 40.7359,
                'lon': -73.9911,
                'voltage': 13.8,
                'type': 'distribution'
            },
            'Traffic_Wall_Street': {
                'lat': 40.7074,
                'lon': -74.0113,
                'voltage': 13.8,
                'type': 'distribution'
            },
            'Traffic_Columbus_Circle': {
                'lat': 40.7680,
                'lon': -73.9819,
                'voltage': 13.8,
                'type': 'distribution'
            },
            'Traffic_Washington_Heights': {
                'lat': 40.7900,  # Still within bounds
                'lon': -73.9500,
                'voltage': 13.8,
                'type': 'distribution'
            }
        }
        print(f"Added {len(self.buses)} electrical buses within Manhattan")
    
    def _add_generators(self):
        """Add power generators - positioned at Manhattan edges but within bounds"""
        self.generators = {
            # East River Generation Station (East edge of Manhattan)
            'East_River_Station': {
                'bus': 'Manhattan_Upper_East',
                'capacity_mw': 800,
                'type': 'gas',
                'cost_per_mwh': 50,
                'current_output': 0,
                'lat': 40.7700,  # East side, within bounds
                'lon': -73.9400   # Eastern edge of Manhattan
            },
            
            # Hudson River Generation Station (West edge of Manhattan)
            'Hudson_Station': {
                'bus': 'Manhattan_Chelsea',
                'capacity_mw': 600,
                'type': 'gas',
                'cost_per_mwh': 55,
                'current_output': 0,
                'lat': 40.7500,  # West side, within bounds
                'lon': -74.0150  # Western edge of Manhattan
            },
            
            # Downtown Generation (Southern Manhattan)
            'Downtown_Station': {
                'bus': 'Manhattan_Lower',
                'capacity_mw': 500,
                'type': 'gas',
                'cost_per_mwh': 60,
                'current_output': 0,
                'lat': 40.7050,  # Southern Manhattan
                'lon': -74.0150
            },
            
            # Rooftop Solar (Distributed across Manhattan)
            'Manhattan_Solar_Midtown': {
                'bus': 'Manhattan_Midtown',
                'capacity_mw': 50,
                'type': 'solar',
                'cost_per_mwh': 0,
                'current_output': 0,
                'lat': 40.7600,
                'lon': -73.9800
            },
            
            'Manhattan_Solar_Upper': {
                'bus': 'Manhattan_Upper_West',
                'capacity_mw': 30,
                'type': 'solar',
                'cost_per_mwh': 0,
                'current_output': 0,
                'lat': 40.7800,
                'lon': -73.9700
            },
            
            # Battery Storage (for grid stability)
            'Battery_Storage_Chelsea': {
                'bus': 'Manhattan_Chelsea',
                'capacity_mw': 100,
                'type': 'battery',
                'cost_per_mwh': 20,
                'current_output': 0,
                'lat': 40.7480,
                'lon': -73.9950
            }
        }
        print(f"Added {len(self.generators)} generators within Manhattan")
    
    def _add_lines(self):
        """Add transmission lines - all within Manhattan grid"""
        self.lines = {
            # Main transmission lines (138kV) - North-South backbone
            'TL_Upper_Midtown': {
                'from': 'Manhattan_Upper_West',
                'to': 'Manhattan_Midtown',
                'capacity_mw': 500,
                'resistance': 0.01,
                'current_flow': 0
            },
            'TL_Upper_East_Midtown': {
                'from': 'Manhattan_Upper_East',
                'to': 'Manhattan_Midtown',
                'capacity_mw': 500,
                'resistance': 0.01,
                'current_flow': 0
            },
            'TL_Midtown_Chelsea': {
                'from': 'Manhattan_Midtown',
                'to': 'Manhattan_Chelsea',
                'capacity_mw': 600,
                'resistance': 0.008,
                'current_flow': 0
            },
            'TL_Chelsea_Lower': {
                'from': 'Manhattan_Chelsea',
                'to': 'Manhattan_Lower',
                'capacity_mw': 500,
                'resistance': 0.01,
                'current_flow': 0
            },
            
            # Cross-town connections (East-West)
            'TL_Upper_Crosstown': {
                'from': 'Manhattan_Upper_West',
                'to': 'Manhattan_Upper_East',
                'capacity_mw': 400,
                'resistance': 0.012,
                'current_flow': 0
            },
            
            # Distribution lines to traffic infrastructure (13.8kV)
            'DL_Times_Square': {
                'from': 'Manhattan_Midtown',
                'to': 'Traffic_Times_Square',
                'capacity_mw': 100,
                'resistance': 0.02,
                'current_flow': 0
            },
            'DL_Central_Park': {
                'from': 'Manhattan_Upper_East',
                'to': 'Traffic_Central_Park_South',
                'capacity_mw': 80,
                'resistance': 0.025,
                'current_flow': 0
            },
            'DL_Union_Square': {
                'from': 'Manhattan_Chelsea',
                'to': 'Traffic_Union_Square',
                'capacity_mw': 90,
                'resistance': 0.022,
                'current_flow': 0
            },
            'DL_Wall_Street': {
                'from': 'Manhattan_Lower',
                'to': 'Traffic_Wall_Street',
                'capacity_mw': 100,
                'resistance': 0.02,
                'current_flow': 0
            },
            'DL_Columbus_Circle': {
                'from': 'Manhattan_Upper_West',
                'to': 'Traffic_Columbus_Circle',
                'capacity_mw': 80,
                'resistance': 0.025,
                'current_flow': 0
            },
            'DL_Washington_Heights': {
                'from': 'Manhattan_Upper_East',
                'to': 'Traffic_Washington_Heights',
                'capacity_mw': 70,
                'resistance': 0.03,
                'current_flow': 0
            }
        }
        print(f"Added {len(self.lines)} transmission lines within Manhattan")
    
    def _add_base_loads(self):
        """Add base electrical loads for Manhattan districts"""
        self.loads = {
            'Load_Midtown_Commercial': {
                'bus': 'Manhattan_Midtown',
                'base_mw': 400,
                'current_mw': 400,
                'type': 'commercial'
            },
            'Load_Upper_West_Residential': {
                'bus': 'Manhattan_Upper_West',
                'base_mw': 250,
                'current_mw': 250,
                'type': 'residential'
            },
            'Load_Upper_East_Residential': {
                'bus': 'Manhattan_Upper_East',
                'base_mw': 280,
                'current_mw': 280,
                'type': 'residential'
            },
            'Load_Financial_District': {
                'bus': 'Manhattan_Lower',
                'base_mw': 350,
                'current_mw': 350,
                'type': 'commercial'
            },
            'Load_Chelsea_Mixed': {
                'bus': 'Manhattan_Chelsea',
                'base_mw': 200,
                'current_mw': 200,
                'type': 'mixed'
            }
        }
        print(f"Added {len(self.loads)} base loads")
    
    def _add_traffic_infrastructure(self):
        """Add traffic-related electrical loads"""
        # Traffic lights distributed across Manhattan
        self.traffic_light_loads = {
            'TL_Times_Square': {
                'bus': 'Traffic_Times_Square',
                'base_mw': 0.8,
                'current_mw': 0.8
            },
            'TL_Central_Park': {
                'bus': 'Traffic_Central_Park_South',
                'base_mw': 0.6,
                'current_mw': 0.6
            },
            'TL_Union_Square': {
                'bus': 'Traffic_Union_Square',
                'base_mw': 0.5,
                'current_mw': 0.5
            },
            'TL_Wall_Street': {
                'bus': 'Traffic_Wall_Street',
                'base_mw': 0.7,
                'current_mw': 0.7
            },
            'TL_Columbus_Circle': {
                'bus': 'Traffic_Columbus_Circle',
                'base_mw': 0.4,
                'current_mw': 0.4
            },
            'TL_Washington_Heights': {
                'bus': 'Traffic_Washington_Heights',
                'base_mw': 0.3,
                'current_mw': 0.3
            }
        }
        
        # Street lights
        self.street_light_loads = {
            'SL_Midtown': {
                'bus': 'Traffic_Times_Square',
                'base_mw': 2.0,
                'current_mw': 2.0
            },
            'SL_Upper_Manhattan': {
                'bus': 'Traffic_Central_Park_South',
                'base_mw': 1.5,
                'current_mw': 1.5
            },
            'SL_Lower_Manhattan': {
                'bus': 'Traffic_Wall_Street',
                'base_mw': 1.8,
                'current_mw': 1.8
            }
        }
        
        # EV charging stations distributed across Manhattan
        self.ev_charging_loads = {
            'EV_Times_Square': {
                'bus': 'Traffic_Times_Square',
                'capacity_mw': 2.0,
                'current_mw': 0
            },
            'EV_Central_Park': {
                'bus': 'Traffic_Central_Park_South',
                'capacity_mw': 1.5,
                'current_mw': 0
            },
            'EV_Union_Square': {
                'bus': 'Traffic_Union_Square',
                'capacity_mw': 1.8,
                'current_mw': 0
            },
            'EV_Wall_Street': {
                'bus': 'Traffic_Wall_Street',
                'capacity_mw': 2.2,
                'current_mw': 0
            },
            'EV_Columbus_Circle': {
                'bus': 'Traffic_Columbus_Circle',
                'capacity_mw': 1.6,
                'current_mw': 0
            }
        }
        
        print(f"Added {len(self.traffic_light_loads)} traffic light load zones")
        print(f"Added {len(self.street_light_loads)} street light load zones")
        print(f"Added {len(self.ev_charging_loads)} EV charging zones")
    
    def update_traffic_loads(self, vehicle_count, traffic_light_states):
        """Update loads based on SUMO traffic data"""
        # Update EV charging based on vehicle density
        ev_ratio = 0.005
        charging_power_per_ev = 0.05  # 50kW average
        
        total_ev_charging = vehicle_count * ev_ratio * charging_power_per_ev
        
        # Distribute EV charging load across Manhattan zones
        zones = ['EV_Times_Square', 'EV_Central_Park', 'EV_Union_Square', 
                 'EV_Wall_Street', 'EV_Columbus_Circle']
        
        for i, zone in enumerate(zones):
            if zone in self.ev_charging_loads:
                # Distribute proportionally with some variation
                zone_factor = (1.0 + 0.2 * (i - 2)) / len(zones)
                self.ev_charging_loads[zone]['current_mw'] = min(
                    total_ev_charging * zone_factor,
                    self.ev_charging_loads[zone]['capacity_mw']
                )
        
        # Update traffic light loads based on states
        green_ratio = sum(1 for state in traffic_light_states.values() if 'g' in state.lower()) / max(len(traffic_light_states), 1)
        power_factor = 1.0 + (1.0 - green_ratio) * 0.1
        
        for tl_load in self.traffic_light_loads.values():
            tl_load['current_mw'] = tl_load['base_mw'] * power_factor
    
    def simulate_power_flow(self):
        """Simple power flow simulation"""
        # Calculate total load
        self.total_load = 0
        
        # Base loads with time-of-day variation
        hour = self.current_hour
        if 0 <= hour < 6:
            factor = 0.6
        elif 6 <= hour < 9:
            factor = 0.8
        elif 9 <= hour < 17:
            factor = 0.9
        elif 17 <= hour < 21:
            factor = 1.0
        else:
            factor = 0.7
        
        for load in self.loads.values():
            load['current_mw'] = load['base_mw'] * factor
            self.total_load += load['current_mw']
        
        # Traffic infrastructure loads
        for tl_load in self.traffic_light_loads.values():
            self.total_load += tl_load['current_mw']
        
        # Street lights (on at night)
        for sl_load in self.street_light_loads.values():
            if hour < 6 or hour > 18:
                sl_load['current_mw'] = sl_load['base_mw']
            else:
                sl_load['current_mw'] = 0
            self.total_load += sl_load['current_mw']
        
        # EV charging loads
        for ev_load in self.ev_charging_loads.values():
            self.total_load += ev_load['current_mw']
        
        # Dispatch generators (simple merit order)
        self.total_generation = 0
        remaining_load = self.total_load
        
        # Solar first (if daytime)
        if 6 <= hour <= 18:
            for gen_name, gen in self.generators.items():
                if gen['type'] == 'solar':
                    solar_output = gen['capacity_mw'] * np.sin((hour - 6) * np.pi / 12)
                    gen['current_output'] = min(solar_output, remaining_load)
                    self.total_generation += gen['current_output']
                    remaining_load -= gen['current_output']
        
        # Then dispatch gas plants by cost
        gas_plants = sorted(
            [(name, gen) for name, gen in self.generators.items() if gen['type'] == 'gas'],
            key=lambda x: x[1]['cost_per_mwh']
        )
        
        for gen_name, gen in gas_plants:
            if remaining_load > 0:
                gen['current_output'] = min(gen['capacity_mw'], remaining_load)
                self.total_generation += gen['current_output']
                remaining_load -= gen['current_output']
            else:
                gen['current_output'] = 0
        
        # Update line flows (simplified)
        for line_name, line in self.lines.items():
            # Approximate flow based on connected bus loads
            if 'DL_' in line_name:  # Distribution lines
                line['current_flow'] = min(self.total_load * 0.05, line['capacity_mw'])
            else:  # Transmission lines
                line['current_flow'] = min(self.total_load * 0.15, line['capacity_mw'])
    
    def get_status(self):
        """Get current network status"""
        return {
            'timestamp': self.time_steps[self.current_hour].strftime('%Y-%m-%d %H:%M'),
            'total_generation_mw': round(self.total_generation, 2),
            'total_load_mw': round(self.total_load, 2),
            'balance_mw': round(self.total_generation - self.total_load, 2),
            'traffic_light_load_mw': round(sum(tl['current_mw'] for tl in self.traffic_light_loads.values()), 2),
            'street_light_load_mw': round(sum(sl['current_mw'] for sl in self.street_light_loads.values()), 2),
            'ev_charging_load_mw': round(sum(ev['current_mw'] for ev in self.ev_charging_loads.values()), 2),
            'generators': {name: round(gen['current_output'], 2) for name, gen in self.generators.items()},
            'line_utilization': {name: round((line['current_flow'] / line['capacity_mw']) * 100, 1) 
                               for name, line in self.lines.items()}
        }
    
    def advance_time(self):
        """Advance to next hour"""
        self.current_hour = (self.current_hour + 1) % 24
    
    def save_state(self, filepath="manhattan_power_state.json"):
        """Save current state to JSON"""
        state = {
            'buses': self.buses,
            'generators': self.generators,
            'loads': self.loads,
            'lines': self.lines,
            'traffic_light_loads': self.traffic_light_loads,
            'street_light_loads': self.street_light_loads,
            'ev_charging_loads': self.ev_charging_loads,
            'current_hour': self.current_hour,
            'total_generation': self.total_generation,
            'total_load': self.total_load
        }
        
        with open(filepath, 'w') as f:
            json.dump(state, f, indent=2, default=str)
        
        print(f"State saved to {filepath}")

def test_network():
    """Test the Manhattan-focused network"""
    print("=" * 60)
    print("Testing Manhattan Power Network")
    print("All components within: 40.700-40.800°N, -74.020--73.930°W")
    print("=" * 60)
    
    # Create and build network
    network = NYCPowerNetworkSimple()
    network.build_network()
    
    # Verify all components are within Manhattan bounds
    print("\n" + "=" * 60)
    print("Verifying component locations:")
    print("-" * 60)
    
    manhattan_bounds = {
        'lat_min': 40.700,
        'lat_max': 40.800,
        'lon_min': -74.020,
        'lon_max': -73.930
    }
    
    # Check buses
    print("\nBuses (Substations):")
    for bus_id, bus in network.buses.items():
        in_bounds = (manhattan_bounds['lat_min'] <= bus['lat'] <= manhattan_bounds['lat_max'] and
                    manhattan_bounds['lon_min'] <= bus['lon'] <= manhattan_bounds['lon_max'])
        status = "✓" if in_bounds else "✗"
        print(f"  {status} {bus_id}: ({bus['lat']:.3f}, {bus['lon']:.3f}) - {bus['voltage']}kV")
    
    # Check generators
    print("\nGenerators:")
    for gen_id, gen in network.generators.items():
        in_bounds = (manhattan_bounds['lat_min'] <= gen['lat'] <= manhattan_bounds['lat_max'] and
                    manhattan_bounds['lon_min'] <= gen['lon'] <= manhattan_bounds['lon_max'])
        status = "✓" if in_bounds else "✗"
        print(f"  {status} {gen_id}: ({gen['lat']:.3f}, {gen['lon']:.3f}) - {gen['capacity_mw']}MW {gen['type']}")
    
    # Display network summary
    print("\n" + "=" * 60)
    print("Network Components Summary:")
    print(f"• Buses: {len(network.buses)} (all within Manhattan)")
    print(f"• Generators: {len(network.generators)} (Total: {sum(g['capacity_mw'] for g in network.generators.values())} MW)")
    print(f"• Base Loads: {len(network.loads)}")
    print(f"• Transmission Lines: {len(network.lines)}")
    print(f"• Traffic Light Zones: {len(network.traffic_light_loads)}")
    print(f"• EV Charging Zones: {len(network.ev_charging_loads)}")
    
    # Simulate
    print("\n" + "=" * 60)
    print("24-Hour Simulation:")
    print("-" * 60)
    
    for hour in range(0, 24, 6):
        network.current_hour = hour
        network.simulate_power_flow()
        status = network.get_status()
        
        print(f"\nTime: {status['timestamp']}")
        print(f"Generation: {status['total_generation_mw']} MW")
        print(f"Total Load: {status['total_load_mw']} MW")
        print(f"Traffic Infrastructure: {status['traffic_light_load_mw'] + status['street_light_load_mw']} MW")
        print(f"EV Charging: {status['ev_charging_load_mw']} MW")
    
    # Save state
    network.save_state()
    
    print("\n✅ Manhattan Power Network ready for integration!")
    print("All components positioned within Manhattan traffic grid area")
    print("=" * 60)
    
    return network

if __name__ == "__main__":
    test_network()