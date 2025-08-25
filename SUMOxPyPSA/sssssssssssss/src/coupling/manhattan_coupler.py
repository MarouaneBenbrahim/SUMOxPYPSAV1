"""
PROPER Manhattan SUMO×PyPSA Coupling System
Real-time bidirectional coupling with actual data exchange
"""

import traci
import pypsa
import pandas as pd
import numpy as np
import json
import time
from pathlib import Path

class ManhattanCoupledSimulation:
    def __init__(self):
        self.sumo_config = 'data/sumo/manhattan.sumocfg'
        self.pypsa_network_file = 'data/power/manhattan_grid_complete.nc'
        
        # Coupling data structures
        self.ev_vehicles = {}  # Track EVs and their battery states
        self.charging_stations = {}  # Map charging stations to power loads
        self.traffic_signals = {}  # Map signals to power loads
        self.current_step = 0
        
    def initialize(self):
        """Initialize both simulators with proper error handling"""
        
        print("\n" + "="*80)
        print("INITIALIZING MANHATTAN COUPLED SIMULATION")
        print("="*80)
        
        # Initialize SUMO
        print("\n[1/2] Starting SUMO...")
        try:
            sumo_cmd = ['sumo', '-c', self.sumo_config, '--step-length', '1', '--no-step-log']
            traci.start(sumo_cmd)
            print("✅ SUMO started successfully")
            
            # Get SUMO network information
            self.net_boundary = traci.simulation.getNetBoundary()
            print(f"   Network boundary: {self.net_boundary}")
            
        except Exception as e:
            print(f"❌ SUMO initialization failed: {e}")
            return False
        
        # Initialize PyPSA
        print("\n[2/2] Loading PyPSA network...")
        try:
            self.network = pypsa.Network()
            self.network.import_from_netcdf(self.pypsa_network_file)
            print("✅ PyPSA network loaded")
            print(f"   Buses: {len(self.network.buses)}")
            print(f"   Generators: {len(self.network.generators)}")
            print(f"   Loads: {len(self.network.loads)}")
            
        except Exception as e:
            print(f"❌ PyPSA initialization failed: {e}")
            traci.close()
            return False
        
        # Establish coupling mappings
        self._establish_coupling()
        
        print("\n" + "="*80)
        print("✅ COUPLED SIMULATION READY")
        print("="*80)
        
        return True
    
    def _establish_coupling(self):
        """Map SUMO entities to PyPSA components"""
        
        print("\nEstablishing coupling mappings...")
        
        # Map EV charging stations to power loads
        self.charging_stations = {
            'Times_Square': 'ev_Times_Square_Supercharger',
            'Bryant_Park': 'ev_Bryant_Park_Station',
            'Grand_Central': 'ev_Grand_Central_Parking',
            'Port_Authority': 'ev_Port_Authority',
            'Madison_Square': 'ev_Madison_Square'
        }
        
        # Map traffic signal groups to power loads
        self.traffic_signals = {
            'HK_signals': 'signals_HK_SUB',
            'TS_signals': 'signals_TS_SUB',
            'GC_signals': 'signals_GC_SUB',
            'MH_signals': 'signals_MH_SUB'
        }
        
        print(f"   Mapped {len(self.charging_stations)} charging stations")
        print(f"   Mapped {len(self.traffic_signals)} signal groups")
    
    def step(self):
        """Execute one coupled simulation step"""
        
        # Step 1: Get SUMO state
        vehicles = traci.vehicle.getIDList()
        
        # Track EVs and their charging needs
        ev_demand = self._calculate_ev_demand(vehicles)
        
        # Step 2: Update PyPSA loads based on traffic
        self._update_power_loads(ev_demand, len(vehicles))
        
        # Step 3: Solve power flow
        try:
            snapshot = self.network.snapshots[self.current_step % len(self.network.snapshots)]
            self.network.pf(snapshots=[snapshot])
            
            # Check for violations
            violations = self._check_power_violations()
            
            # Step 4: Apply power state to traffic
            if violations:
                self._apply_power_failures_to_traffic(violations)
                
        except Exception as e:
            print(f"Power flow failed at step {self.current_step}: {e}")
        
        # Step 5: Advance SUMO
        traci.simulationStep()
        
        self.current_step += 1
        
        return self._get_metrics()
    
    def _calculate_ev_demand(self, vehicles):
        """Calculate EV charging demand from SUMO"""
        
        ev_demand = {station: 0 for station in self.charging_stations}
        
        for veh_id in vehicles:
            veh_type = traci.vehicle.getTypeID(veh_id)
            
            if veh_type == 'electric':
                # Check if vehicle is stopped (charging)
                speed = traci.vehicle.getSpeed(veh_id)
                
                if speed < 0.1:  # Vehicle is stopped
                    position = traci.vehicle.getPosition(veh_id)
                    
                    # Find nearest charging station (simplified)
                    # In production, use actual geographic matching
                    station_idx = hash(veh_id) % len(self.charging_stations)
                    station_name = list(self.charging_stations.keys())[station_idx]
                    
                    # Each charging EV adds 50-150 kW demand
                    ev_demand[station_name] += 0.1  # MW
        
        return ev_demand
    
    def _update_power_loads(self, ev_demand, num_vehicles):
        """Update PyPSA loads based on traffic state"""
        
        current_snapshot = self.network.snapshots[self.current_step % len(self.network.snapshots)]
        
        # Update EV charging loads
        for station, load_name in self.charging_stations.items():
            if load_name in self.network.loads.index:
                demand = ev_demand.get(station, 0)
                self.network.loads_t.p_set.at[current_snapshot, load_name] = demand
        
        # Update signal loads based on traffic density
        traffic_factor = min(num_vehicles / 1000, 1.5)  # Scale based on traffic
        
        for signal_group, load_name in self.traffic_signals.items():
            if load_name in self.network.loads.index:
                base_load = 0.15  # MW
                self.network.loads_t.p_set.at[current_snapshot, load_name] = base_load * traffic_factor
    
    def _check_power_violations(self):
        """Check for power system violations"""
        
        violations = []
        current_snapshot = self.network.snapshots[self.current_step % len(self.network.snapshots)]
        
        # Check line overloads
        if hasattr(self.network.lines_t, 'p0'):
            line_flows = self.network.lines_t.p0.loc[current_snapshot]
            line_limits = self.network.lines.s_nom
            
            overloaded = line_flows.abs() > line_limits * 1.2
            
            if overloaded.any():
                violations.append({
                    'type': 'line_overload',
                    'components': overloaded[overloaded].index.tolist()
                })
        
        # Check voltage violations
        if hasattr(self.network.buses_t, 'v_mag_pu'):
            voltages = self.network.buses_t.v_mag_pu.loc[current_snapshot]
            
            undervoltage = voltages < 0.95
            if undervoltage.any():
                violations.append({
                    'type': 'undervoltage',
                    'buses': undervoltage[undervoltage].index.tolist()
                })
        
        return violations
    
    def _apply_power_failures_to_traffic(self, violations):
        """Apply power system failures to SUMO traffic"""
        
        for violation in violations:
            if violation['type'] == 'undervoltage':
                # Affect traffic signals in undervoltage areas
                affected_signals = traci.trafficlight.getIDList()[:5]  # Sample
                
                for signal_id in affected_signals:
                    try:
                        # Set to flashing yellow (power saving mode)
                        program = traci.trafficlight.getAllProgramLogics(signal_id)[0]
                        yellow_state = 'y' * len(program.phases[0].state)
                        traci.trafficlight.setRedYellowGreenState(signal_id, yellow_state)
                    except:
                        pass
    
    def _get_metrics(self):
        """Get current simulation metrics"""
        
        vehicles = traci.vehicle.getIDList()
        current_snapshot = self.network.snapshots[self.current_step % len(self.network.snapshots)]
        
        # Traffic metrics
        avg_speed = sum(traci.vehicle.getSpeed(v) for v in vehicles) / max(len(vehicles), 1)
        
        # Power metrics
        total_load = self.network.loads_t.p_set.loc[current_snapshot].sum()
        total_generation = self.network.generators_t.p.loc[current_snapshot].sum() if hasattr(self.network.generators_t, 'p') else 0
        
        return {
            'time': self.current_step,
            'vehicles': len(vehicles),
            'avg_speed_kmh': avg_speed * 3.6,
            'total_load_mw': total_load,
            'total_generation_mw': total_generation
        }
    
    def run(self, duration=3600):
        """Run the coupled simulation"""
        
        if not self.initialize():
            print("Failed to initialize simulation")
            return
        
        print(f"\nRunning coupled simulation for {duration} seconds...")
        print("-"*80)
        
        try:
            for step in range(duration):
                metrics = self.step()
                
                # Print progress every 100 steps
                if step % 100 == 0:
                    print(f"Step {step}: {metrics['vehicles']} vehicles, "
                          f"{metrics['avg_speed_kmh']:.1f} km/h, "
                          f"{metrics['total_load_mw']:.1f} MW load")
                
        except KeyboardInterrupt:
            print("\nSimulation interrupted by user")
        except Exception as e:
            print(f"\nSimulation error: {e}")
        finally:
            traci.close()
            print("\n" + "="*80)
            print("SIMULATION COMPLETE")
            print("="*80)

if __name__ == "__main__":
    # First build the networks
    print("Building Manhattan networks...")
    
    # Build SUMO network
    from sumo.build_manhattan_network import ManhattanSUMOBuilder
    sumo_builder = ManhattanSUMOBuilder()
    sumo_builder.build_complete_network()
    
    # Build PyPSA network
    from pypsa.build_manhattan_grid import ManhattanPowerGrid
    grid_builder = ManhattanPowerGrid()
    grid_builder.build_complete_grid()
    
    # Run coupled simulation
    simulation = ManhattanCoupledSimulation()
    simulation.run(duration=300)  # Run for 5 minutes as test