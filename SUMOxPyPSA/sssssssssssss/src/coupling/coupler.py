"""
Coupling Interface - Bi-directional coupling between SUMO traffic and PyPSA power grid
Handles real-time data exchange and interdependency modeling
"""

import os
import sys
import json
import pandas as pd
import numpy as np
import pypsa
import traci
from pathlib import Path
from datetime import datetime, timedelta
import threading
import queue
import time
import warnings
warnings.filterwarnings('ignore')

class SUMOPyPSACoupler:
    def __init__(self):
        """Initialize the coupling interface"""
        # Load configuration
        with open('configs/config.json', 'r') as f:
            self.config = json.load(f)
        
        print("\n" + "="*70)
        print(" SUMO-PYPSA COUPLING INTERFACE ".center(70, "="))
        print("="*70)
        print("\n🔗 Initializing bi-directional coupling system")
        print(f"📍 Area: {self.config['simulation']['area']['description']}")
        print("-"*70)
        
        # Coupling parameters
        self.coupling_config = self.config['coupling']
        self.sync_interval = self.coupling_config['sync_interval']
        
        # Data structures for coupling
        self.traffic_state = {
            'vehicles': {},
            'signals': {},
            'ev_charging': {},
            'congestion': {}
        }
        
        self.power_state = {
            'bus_voltages': {},
            'line_flows': {},
            'loads': {},
            'outages': [],
            'frequency': 60.0
        }
        
        # Load infrastructure data
        self.ev_stations = pd.read_csv('data/power/ev_stations.csv')
        self.buildings = pd.read_csv('data/power/buildings.csv')
        self.signals = pd.read_csv('data/traffic/signals.csv')
        
        # Communication queues
        self.traffic_to_power_queue = queue.Queue()
        self.power_to_traffic_queue = queue.Queue()
        
        # Metrics tracking
        self.metrics = {
            'simulation_time': 0,
            'total_vehicles': 0,
            'evs_charging': 0,
            'total_load_mw': 0,
            'blackout_areas': 0,
            'avg_speed_kmh': 0,
            'signals_affected': 0
        }
        
        # Initialize components
        self.sumo_connected = False
        self.pypsa_network = None
        
    def initialize(self, scenario='normal'):
        """Initialize both simulators"""
        print("\n🚀 INITIALIZING COUPLED SIMULATION...\n")
        
        # Step 1: Initialize SUMO
        print("[1/3] 🚗 Initializing SUMO...")
        self.init_sumo(scenario)
        
        # Step 2: Initialize PyPSA
        print("\n[2/3] ⚡ Initializing PyPSA...")
        self.init_pypsa()
        
        # Step 3: Establish coupling
        print("\n[3/3] 🔗 Establishing coupling...")
        self.establish_coupling()
        
        print("\n" + "="*70)
        print("✅ COUPLING INTERFACE INITIALIZED!")
        print("="*70)
        
        return True
    
    def init_sumo(self, scenario):
        """Initialize SUMO simulation"""
        try:
            # SUMO configuration
            sumo_binary = "sumo"  # Use "sumo-gui" for visualization
            sumo_config = "data/sumo/manhattan_clean.sumocfg"
            
            # Check if files exist
            if not Path(sumo_config).exists():
                # Use simple config if main one doesn't exist
                sumo_config = "data/sumo/simple.sumocfg"
                if not Path(sumo_config).exists():
                    print("   ⚠️  No SUMO config found, creating minimal setup...")
                    self.create_minimal_sumo_config()
                    sumo_config = "data/sumo/minimal.sumocfg"
            
            # Add route file for scenario
            route_file = f"data/sumo/routes_{scenario}.rou.xml"
            if not Path(route_file).exists():
                print(f"   ⚠️  Route file for '{scenario}' not found, using template...")
                route_file = "data/sumo/vehicle_types.rou.xml"
            
            # Start SUMO with basic options only
            sumo_cmd = [sumo_binary, "-c", sumo_config]
            
            # Add additional options (only those that work with your version)
            if Path(route_file).exists():
                sumo_cmd.extend(["--route-files", route_file])
            
            sumo_cmd.extend([
                "--step-length", str(self.config['sumo']['step_length']),
                "--no-step-log", "true"
                # Removed --warning-output as it's not supported in all versions
            ])
            
            print(f"   Starting SUMO with command: {' '.join(sumo_cmd[:3])}...")
            
            # Try to start SUMO
            try:
                traci.start(sumo_cmd)
                self.sumo_connected = True
                
                # Get initial statistics
                self.sumo_net_boundary = traci.simulation.getNetBoundary()
                
                print(f"   ✅ SUMO initialized")
                print(f"      • Net boundary: {self.sumo_net_boundary}")
                print(f"      • Simulation will run for {self.config['simulation']['duration_seconds']}s")
                
            except Exception as sumo_error:
                print(f"   ⚠️  SUMO start failed: {sumo_error}")
                print(f"   🔄 Running without SUMO (power-only mode)...")
                self.sumo_connected = False
                
        except Exception as e:
            print(f"   ❌ Failed to initialize SUMO: {e}")
            self.sumo_connected = False
            print(f"   🔄 Continuing with power simulation only...")
    
    def run_without_sumo(self):
        """Run simulation with only PyPSA (no traffic)"""
        # Simulate synthetic traffic data
        time_hour = (self.metrics['simulation_time'] % 86400) / 3600  # Hour of day
        
        # Traffic pattern based on time of day
        if 7 <= time_hour <= 9 or 17 <= time_hour <= 19:  # Rush hours
            base_vehicles = 200
            base_speed = 20
            evs_charging = 10
        elif 22 <= time_hour or time_hour <= 5:  # Night
            base_vehicles = 30
            base_speed = 40
            evs_charging = 15
        else:  # Normal hours
            base_vehicles = 100
            base_speed = 30
            evs_charging = 5
        
        # Add some randomness
        self.metrics['total_vehicles'] = base_vehicles + np.random.randint(-10, 10)
        self.metrics['evs_charging'] = evs_charging + np.random.randint(-2, 3)
        self.metrics['avg_speed_kmh'] = base_speed + np.random.uniform(-5, 5)
        
        # Simulate signal states
        self.traffic_state['signals'] = {f'sig_{i}': 'GGrrGGrr' for i in range(41)}
    
    def init_pypsa(self):
        """Initialize PyPSA network"""
        try:
            # Load saved network
            network_file = 'data/power/manhattan_grid.nc'
            
            if Path(network_file).exists():
                self.pypsa_network = pypsa.Network()
                self.pypsa_network.import_from_netcdf(network_file)
                print(f"   ✅ Loaded existing PyPSA network")
            else:
                print(f"   ⚠️  Network file not found, creating simple network...")
                self.pypsa_network = self.create_simple_power_network()
            
            # Fix the load scaling issue
            self.fix_load_scaling()
            
            # Run initial power flow
            print(f"   Running initial power flow...")
            try:
                self.pypsa_network.lpf()
                print(f"   ✅ Power flow solved")
            except:
                print(f"   ⚠️  Power flow failed, continuing anyway...")
            
            print(f"   ✅ PyPSA initialized")
            print(f"      • Buses: {len(self.pypsa_network.buses)}")
            print(f"      • Total generation: {self.pypsa_network.generators.p_nom.sum():.1f} MW")
            print(f"      • Total load: {self.pypsa_network.loads.p_set.sum():.1f} MW")
            
        except Exception as e:
            print(f"   ❌ Failed to initialize PyPSA: {e}")
            self.pypsa_network = None
    
    def fix_load_scaling(self):
        """Fix the load scaling issue in PyPSA network"""
        if self.pypsa_network is None:
            return
        
        # Reset loads to reasonable values
        total_building_load = 100  # MW for the area
        
        # Scale all loads proportionally
        for load in self.pypsa_network.loads.index:
            if 'building' in load:
                self.pypsa_network.loads.at[load, 'p_set'] = total_building_load / 15  # Distribute across groups
            elif 'ev' in load:
                self.pypsa_network.loads.at[load, 'p_set'] = 0.5  # 500 kW per station initially
            elif 'signal' in load:
                self.pypsa_network.loads.at[load, 'p_set'] = 0.004  # 4 kW for signals
        
        # Clear time series if they exist and are problematic
        if not self.pypsa_network.loads_t.p_set.empty:
            # Scale down time series
            self.pypsa_network.loads_t.p_set = self.pypsa_network.loads_t.p_set / 100
    
    def establish_coupling(self):
        """Establish coupling between simulators"""
        # Map SUMO edges to power buses
        self.edge_to_bus_mapping = {}
        self.signal_to_load_mapping = {}
        self.ev_station_mapping = {}
        
        # Create mappings
        print(f"   📍 Mapping infrastructure...")
        
        # Map traffic signals to power loads
        for _, signal in self.signals.iterrows():
            self.signal_to_load_mapping[str(signal['node_id'])] = 'traffic_signals'
        
        # Map EV stations
        for _, station in self.ev_stations.iterrows():
            self.ev_station_mapping[station['station_id']] = f"ev_{station['station_id']}"
        
        print(f"   ✅ Coupling established")
        print(f"      • Mapped {len(self.signal_to_load_mapping)} signals to power loads")
        print(f"      • Mapped {len(self.ev_station_mapping)} EV charging stations")
    
    def step_simulation(self, time_step):
        """Execute one simulation step with coupling"""
        self.metrics['simulation_time'] = time_step
        
        # Step 1: Get traffic state from SUMO or use synthetic
        if self.sumo_connected:
            self.update_traffic_state()
        else:
            self.run_without_sumo()
        
        # Step 2: Calculate power demands from traffic
        power_demands = self.calculate_power_demands()
        
        # Step 3: Update PyPSA loads
        if self.pypsa_network:
            self.update_power_loads(power_demands)
            
            # Step 4: Solve power flow
            self.solve_power_flow()
            
            # Step 5: Check for power failures
            failures = self.check_power_failures()
            
            # Step 6: Apply power failures to traffic (if SUMO connected)
            if failures and self.sumo_connected:
                self.apply_power_to_traffic(failures)
        
        # Step 7: Step SUMO forward (if connected)
        if self.sumo_connected:
            try:
                traci.simulationStep()
            except:
                self.sumo_connected = False
                print(f"   ⚠️  Lost SUMO connection, continuing with power only...")
        
        # Update metrics
        self.update_metrics()
        
        return self.metrics
    
    def update_traffic_state(self):
        """Get current traffic state from SUMO"""
        try:
            # Get vehicle data
            vehicle_ids = traci.vehicle.getIDList()
            self.metrics['total_vehicles'] = len(vehicle_ids)
            
            # Track EVs and their battery state
            evs_charging = 0
            total_speed = 0
            
            for veh_id in vehicle_ids:
                veh_type = traci.vehicle.getTypeID(veh_id)
                position = traci.vehicle.getPosition(veh_id)
                speed = traci.vehicle.getSpeed(veh_id)
                
                total_speed += speed
                
                if veh_type == 'ev':
                    # Check if stopped at charging station
                    if speed < 0.1:  # Nearly stopped
                        evs_charging += 1
                    
                    # Get battery level (if available)
                    try:
                        battery = traci.vehicle.getParameter(veh_id, "device.battery.actualBatteryCapacity")
                        self.traffic_state['vehicles'][veh_id] = {
                            'type': 'ev',
                            'battery': float(battery),
                            'charging': speed < 0.1
                        }
                    except:
                        pass
            
            self.metrics['evs_charging'] = evs_charging
            self.metrics['avg_speed_kmh'] = (total_speed / max(len(vehicle_ids), 1)) * 3.6
            
            # Get traffic light states
            tls_ids = traci.trafficlight.getIDList()
            for tls_id in tls_ids:
                state = traci.trafficlight.getRedYellowGreenState(tls_id)
                self.traffic_state['signals'][tls_id] = state
                
        except Exception as e:
            # If error, disconnect SUMO and continue
            self.sumo_connected = False
            print(f"   ⚠️  Lost SUMO connection: {e}")
    
    def calculate_power_demands(self):
        """Calculate power demands from traffic state"""
        demands = {}
        
        # Traffic signal demand
        signal_demand = len(self.traffic_state.get('signals', {})) * 0.0001  # 100W per signal in MW
        demands['traffic_signals'] = signal_demand
        
        # EV charging demand
        ev_charging_demand = self.metrics['evs_charging'] * 0.05  # 50 kW per charging EV in MW
        for station_id in self.ev_station_mapping:
            demands[f"ev_{station_id}"] = ev_charging_demand / max(len(self.ev_station_mapping), 1)
        
        # Building demand based on traffic (simplified)
        # More traffic = more commercial activity = higher load
        traffic_factor = min(self.metrics['total_vehicles'] / 500, 2.0)  # Scale factor
        base_building_load = 100  # MW
        
        # Time-based variation
        time_hour = (self.metrics['simulation_time'] % 86400) / 3600
        if 8 <= time_hour <= 18:  # Business hours
            time_factor = 1.2
        elif 18 <= time_hour <= 22:  # Evening
            time_factor = 1.0
        else:  # Night
            time_factor = 0.5
        
        demands['buildings'] = base_building_load * traffic_factor * time_factor
        
        return demands
    
    def update_power_loads(self, demands):
        """Update PyPSA network loads"""
        if not self.pypsa_network:
            return
        
        for load_name, demand in demands.items():
            if load_name == 'buildings':
                # Distribute across building loads
                building_loads = [l for l in self.pypsa_network.loads.index if 'building' in l]
                for b_load in building_loads:
                    self.pypsa_network.loads.at[b_load, 'p_set'] = demand / max(len(building_loads), 1)
            elif load_name in self.pypsa_network.loads.index:
                self.pypsa_network.loads.at[load_name, 'p_set'] = demand
        
        self.metrics['total_load_mw'] = self.pypsa_network.loads.p_set.sum()
    
    def solve_power_flow(self):
        """Solve power flow in PyPSA"""
        try:
            self.pypsa_network.lpf()
            
            # Check line flows
            if not self.pypsa_network.lines_t.p0.empty:
                snapshot = self.pypsa_network.snapshots[0]
                line_flows = self.pypsa_network.lines_t.p0.loc[snapshot]
                
                # Check for overloads
                line_loading = abs(line_flows) / self.pypsa_network.lines.s_nom
                self.power_state['line_flows'] = line_loading.to_dict()
                
        except Exception as e:
            # Continue even if power flow fails
            pass
    
    def check_power_failures(self):
        """Check for power system failures"""
        failures = []
        
        if not self.power_state['line_flows']:
            return failures
        
        # Check for line overloads
        overloaded = 0
        for line, loading in self.power_state['line_flows'].items():
            if loading > 1.2:  # 120% overload threshold
                overloaded += 1
                if overloaded <= 3:  # Only report first few
                    failures.append({
                        'type': 'line_overload',
                        'component': line,
                        'severity': loading
                    })
        
        # Check for cascading failures
        if overloaded > 3:
            failures.append({
                'type': 'blackout',
                'area': 'partial',
                'affected_signals': min(len(self.signal_to_load_mapping) // 2, 10)
            })
            self.metrics['blackout_areas'] = 1
        
        return failures
    
    def apply_power_to_traffic(self, failures):
        """Apply power system failures to traffic"""
        if not self.sumo_connected:
            return
        
        affected_signals = 0
        
        for failure in failures:
            if failure['type'] == 'blackout':
                # Turn off traffic lights in affected area
                try:
                    tls_ids = list(traci.trafficlight.getIDList())[:failure.get('affected_signals', 5)]
                    
                    for tls_id in tls_ids:
                        try:
                            # Set to yellow blinking (all yellow)
                            program = traci.trafficlight.getAllProgramLogics(tls_id)[0]
                            yellow_state = 'y' * len(program.phases[0].state)
                            traci.trafficlight.setRedYellowGreenState(tls_id, yellow_state)
                            affected_signals += 1
                        except:
                            pass
                except:
                    pass
        
        self.metrics['signals_affected'] = affected_signals
    
    def update_metrics(self):
        """Update simulation metrics"""
        # Calculate additional metrics
        if self.pypsa_network:
            self.metrics['generation_mw'] = self.pypsa_network.generators.p_nom.sum()
            self.metrics['load_factor'] = self.metrics['total_load_mw'] / max(self.metrics['generation_mw'], 1)
        
        # Print metrics every 10 steps
        if self.metrics['simulation_time'] % 10 == 0:
            self.print_metrics()
    
    def print_metrics(self):
        """Print current metrics"""
        mode = "Coupled" if self.sumo_connected else "Power-Only"
        print(f"\n[t={self.metrics['simulation_time']}s] {mode} Mode Metrics:")
        print(f"  🚗 Traffic: {self.metrics['total_vehicles']} vehicles, "
              f"{self.metrics['avg_speed_kmh']:.1f} km/h avg")
        print(f"  ⚡ Power: {self.metrics['total_load_mw']:.1f} MW load, "
              f"{self.metrics['evs_charging']} EVs charging")
        if self.metrics['signals_affected'] > 0:
            print(f"  ⚠️  Failures: {self.metrics['signals_affected']} signals affected")
    
    def create_minimal_sumo_config(self):
        """Create minimal SUMO config if none exists"""
        config = """<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <input>
        <net-file value="network.net.xml"/>
    </input>
    <time>
        <begin value="0"/>
        <end value="3600"/>
    </time>
</configuration>"""
        
        with open('data/sumo/minimal.sumocfg', 'w') as f:
            f.write(config)
    
    def create_simple_power_network(self):
        """Create simple power network if file doesn't exist"""
        network = pypsa.Network()
        network.set_snapshots(pd.date_range('2024-01-01', periods=1, freq='H'))
        
        # Add basic components
        network.add("Bus", "main_bus", v_nom=13.8)
        network.add("Generator", "main_gen", bus="main_bus", p_nom=100)
        network.add("Load", "main_load", bus="main_bus", p_set=50)
        
        return network
    
    def run_coupled_simulation(self, duration=None, scenario='normal'):
        """Run the complete coupled simulation"""
        print("\n" + "="*70)
        print(" STARTING COUPLED SIMULATION ".center(70, "="))
        print("="*70)
        print(f"\n🎬 Scenario: {scenario}")
        print(f"⏱️  Duration: {duration or self.config['simulation']['duration_seconds']} seconds")
        print("-"*70)
        
        # Initialize
        if not self.initialize(scenario):
            print("❌ Failed to initialize coupling")
            return False
        
        # Determine mode
        if self.sumo_connected:
            print("\n✅ Running in COUPLED MODE (Traffic + Power)")
        else:
            print("\n⚠️  Running in POWER-ONLY MODE (No traffic simulation)")
        
        # Run simulation
        sim_duration = duration or self.config['simulation']['duration_seconds']
        
        try:
            for step in range(sim_duration):
                # Step simulation
                metrics = self.step_simulation(step)
                
                # Progress indicator
                if step % 100 == 0:
                    progress = (step / sim_duration) * 100
                    print(f"\n[{progress:.0f}%] Simulation progress...")
                
        except KeyboardInterrupt:
            print("\n⚠️  Simulation interrupted by user")
        except Exception as e:
            print(f"\n❌ Simulation error: {e}")
            
        finally:
            # Cleanup
            if self.sumo_connected:
                try:
                    traci.close()
                except:
                    pass
            
            print("\n" + "="*70)
            print(" SIMULATION COMPLETE ".center(70, "="))
            print("="*70)
            self.print_final_summary()
        
        return True
    
    def print_final_summary(self):
        """Print final simulation summary"""
        print("\n📊 FINAL SIMULATION SUMMARY:")
        
        mode = "Coupled" if self.sumo_connected else "Power-Only"
        print(f"\n   Simulation Mode: {mode}")
        
        print(f"\n   Traffic Metrics:")
        print(f"      • Total vehicles simulated: {self.metrics['total_vehicles']}")
        print(f"      • Average speed: {self.metrics['avg_speed_kmh']:.1f} km/h")
        print(f"      • EVs charged: {self.metrics['evs_charging']}")
        
        print(f"\n   Power Metrics:")
        print(f"      • Peak load: {self.metrics['total_load_mw']:.1f} MW")
        print(f"      • Load factor: {self.metrics.get('load_factor', 0):.1%}")
        print(f"      • Blackout events: {self.metrics['blackout_areas']}")
        
        print(f"\n   Coupling Metrics:")
        print(f"      • Signals affected by outages: {self.metrics['signals_affected']}")
        print(f"      • Simulation time: {self.metrics['simulation_time']} seconds")


def test_coupling():
    """Test the coupling interface"""
    coupler = SUMOPyPSACoupler()
    
    # Run short test (10 seconds)
    print("\n🧪 Running coupling test (10 second simulation)...")
    coupler.run_coupled_simulation(duration=10, scenario='normal')


if __name__ == "__main__":
    test_coupling()
    input("\nPress Enter to continue...")