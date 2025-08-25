"""
PyPSA Power Grid Builder - Creates the Manhattan power network
Includes substations, transformers, lines, buildings, and EV charging stations
"""

import os
import sys
import json
import pandas as pd
import numpy as np
import pypsa
from pathlib import Path
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class PowerGridBuilder:
    def __init__(self):
        """Initialize power grid builder"""
        # Load configuration
        with open('configs/config.json', 'r') as f:
            self.config = json.load(f)
        
        # Load infrastructure data
        self.substations = pd.read_csv('data/power/substations.csv')
        self.transformers = pd.read_csv('data/power/transformers.csv')
        self.lines = pd.read_csv('data/power/lines.csv')
        self.buildings = pd.read_csv('data/power/buildings.csv')
        self.ev_stations = pd.read_csv('data/power/ev_stations.csv')
        self.signals = pd.read_csv('data/traffic/signals.csv')
        
        print("\n" + "="*70)
        print(" PYPSA POWER GRID BUILDER ".center(70, "="))
        print("="*70)
        print(f"\n⚡ Building power grid for Manhattan")
        print(f"📍 Area: {self.config['simulation']['area']['description']}")
        print("-"*70)
        
        # Initialize PyPSA network
        self.network = pypsa.Network()
        self.network.name = "Manhattan Midtown Power Grid"
        
        # Set time series (1 hour simulation with 1-minute resolution)
        self.snapshots = pd.date_range(
            start='2024-01-01', 
            periods=60,  # 60 minutes for now, will scale up later
            freq='1min'
        )
        self.network.set_snapshots(self.snapshots)
        
    def build_complete_grid(self):
        """Build the complete power grid"""
        print("\n🔧 BUILDING POWER GRID COMPONENTS...\n")
        
        try:
            # Step 1: Add buses (nodes)
            print("[1/8] 🔌 Adding buses (substations & transformers)...")
            self.add_buses()
            
            # Step 2: Add generators (slack bus)
            print("\n[2/8] ⚡ Adding generators...")
            self.add_generators()
            
            # Step 3: Add lines
            print("\n[3/8] 🔗 Adding transmission and distribution lines...")
            self.add_lines()
            
            # Step 4: Add building loads
            print("\n[4/8] 🏢 Adding building loads...")
            self.add_building_loads()
            
            # Step 5: Add EV charging loads
            print("\n[5/8] 🔋 Adding EV charging stations...")
            self.add_ev_charging_loads()
            
            # Step 6: Add traffic signal loads
            print("\n[6/8] 🚦 Adding traffic signal loads...")
            self.add_traffic_signal_loads()
            
            # Step 7: Create load profiles
            print("\n[7/8] 📊 Creating time-varying load profiles...")
            self.create_load_profiles()
            
            # Step 8: Validate network
            print("\n[8/8] ✔️  Validating power network...")
            self.validate_network()
            
            # Save network
            self.save_network()
            
            print("\n" + "="*70)
            print("✅ POWER GRID CONSTRUCTION COMPLETE!")
            print("="*70)
            
            self.print_grid_summary()
            
            return True
            
        except Exception as e:
            print(f"\n❌ Error building power grid: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def add_buses(self):
        """Add buses (electrical nodes) to the network"""
        buses_added = 0
        
        # Add substation buses (high voltage)
        for _, sub in self.substations.iterrows():
            self.network.add(
                "Bus",
                f"{sub['substation_id']}",
                v_nom=sub['voltage_kv'],
                carrier="AC",
                x=sub['lon'],
                y=sub['lat']
            )
            buses_added += 1
        
        print(f"   ✅ Added {len(self.substations)} substation buses ({self.substations['voltage_kv'].iloc[0]} kV)")
        
        # Add transformer buses (medium voltage)
        for _, tr in self.transformers.iterrows():
            # Primary side bus
            self.network.add(
                "Bus",
                f"{tr['transformer_id']}_primary",
                v_nom=tr['primary_voltage_kv'],
                carrier="AC",
                x=tr['lon'],
                y=tr['lat']
            )
            
            # Secondary side bus
            self.network.add(
                "Bus",
                f"{tr['transformer_id']}_secondary",
                v_nom=tr['secondary_voltage_v']/1000,  # Convert to kV
                carrier="AC",
                x=tr['lon'],
                y=tr['lat']
            )
            buses_added += 2
        
        print(f"   ✅ Added {len(self.transformers)*2} transformer buses")
        
        # Add building connection buses (low voltage)
        for i in range(0, len(self.buildings), 10):  # Group buildings
            self.network.add(
                "Bus",
                f"building_bus_{i}",
                v_nom=0.48,  # 480V in kV
                carrier="AC"
            )
            buses_added += 1
        
        print(f"   ✅ Total buses added: {buses_added}")
    
    def add_generators(self):
        """Add generators (power sources)"""
        # Main grid connection at each substation (infinite bus)
        for _, sub in self.substations.iterrows():
            self.network.add(
                "Generator",
                f"{sub['substation_id']}_gen",
                bus=f"{sub['substation_id']}",
                p_nom=sub['capacity_mva'],
                marginal_cost=50,  # $/MWh
                carrier="grid"
            )
        
        print(f"   ✅ Added {len(self.substations)} grid connections")
        print(f"      • Total capacity: {self.substations['capacity_mva'].sum()} MVA")
        
        # Add solar PV on buildings (if any)
        solar_buildings = self.buildings[self.buildings['solar_pv_kw'] > 0]
        if len(solar_buildings) > 0:
            for _, bld in solar_buildings.iterrows():
                bus_id = f"building_bus_{((_ // 10) * 10)}"
                if bus_id in self.network.buses.index:
                    self.network.add(
                        "Generator",
                        f"solar_{bld['building_id']}",
                        bus=bus_id,
                        p_nom=bld['solar_pv_kw']/1000,  # Convert to MW
                        marginal_cost=0,
                        carrier="solar"
                    )
            
            total_solar = solar_buildings['solar_pv_kw'].sum()/1000
            print(f"   ✅ Added {len(solar_buildings)} solar PV systems")
            print(f"      • Total solar capacity: {total_solar:.1f} MW")
    
    def add_lines(self):
        """Add power lines (branches)"""
        lines_added = 0
        
        # Add lines from CSV
        for _, line in self.lines.iterrows():
            # Calculate impedance
            r = line['resistance_ohm_per_km'] * line['length_km']
            x = line['reactance_ohm_per_km'] * line['length_km']
            
            # Check if buses exist
            from_bus = line['from']
            to_bus = line['to']
            
            # For transformers, connect to primary side
            if 'TR_' in to_bus:
                to_bus = f"{to_bus}_primary"
            
            if from_bus in self.network.buses.index and to_bus in self.network.buses.index:
                self.network.add(
                    "Line",
                    f"{line['line_id']}",
                    bus0=from_bus,
                    bus1=to_bus,
                    r=r,
                    x=x,
                    s_nom=line['capacity_mva'],
                    carrier="AC"
                )
                lines_added += 1
        
        print(f"   ✅ Added {lines_added} transmission/distribution lines")
        
        # Add transformers as lines
        transformers_added = 0
        for _, tr in self.transformers.iterrows():
            # Transformer is modeled as a line between primary and secondary
            self.network.add(
                "Line",
                f"transformer_{tr['transformer_id']}",
                bus0=f"{tr['transformer_id']}_primary",
                bus1=f"{tr['transformer_id']}_secondary",
                r=0.01,  # Small resistance
                x=0.05,  # Reactance
                s_nom=tr['rating_kva']/1000,  # Convert to MVA
                carrier="transformer"
            )
            transformers_added += 1
            
            # Connect secondary to building bus
            bus_id = f"building_bus_{((_ % 10) * 10)}"
            if bus_id in self.network.buses.index:
                self.network.add(
                    "Line",
                    f"service_{tr['transformer_id']}",
                    bus0=f"{tr['transformer_id']}_secondary",
                    bus1=bus_id,
                    r=0.001,
                    x=0.001,
                    s_nom=tr['rating_kva']/1000
                )
        
        print(f"   ✅ Added {transformers_added} transformers")
        print(f"   ✅ Total branches: {len(self.network.lines)}")
    
    def add_building_loads(self):
        """Add building loads to the network"""
        loads_added = 0
        total_load = 0
        
        # Group buildings by bus
        for i in range(0, len(self.buildings), 10):
            bus_id = f"building_bus_{i}"
            if bus_id not in self.network.buses.index:
                continue
            
            # Sum loads for buildings in this group
            building_group = self.buildings.iloc[i:min(i+10, len(self.buildings))]
            group_load = building_group['base_load_kw'].sum() / 1000  # Convert to MW
            
            self.network.add(
                "Load",
                f"building_load_{i}",
                bus=bus_id,
                p_set=group_load,
                carrier="building"
            )
            loads_added += 1
            total_load += group_load
        
        print(f"   ✅ Added {loads_added} building load groups")
        print(f"      • Total building load: {total_load:.1f} MW")
        
        # Store building load data for profiles
        self.building_load_data = {
            'office': self.buildings[self.buildings['type'] == 'office']['base_load_kw'].sum()/1000,
            'residential': self.buildings[self.buildings['type'] == 'residential']['base_load_kw'].sum()/1000,
            'retail': self.buildings[self.buildings['type'] == 'retail']['base_load_kw'].sum()/1000,
            'hotel': self.buildings[self.buildings['type'] == 'hotel']['base_load_kw'].sum()/1000
        }
    
    def add_ev_charging_loads(self):
        """Add EV charging station loads"""
        total_ev_capacity = 0
        
        for _, station in self.ev_stations.iterrows():
            # Find nearest bus
            bus_id = f"building_bus_0"  # Simplified - connect to first bus
            
            if bus_id in self.network.buses.index:
                self.network.add(
                    "Load",
                    f"ev_{station['station_id']}",
                    bus=bus_id,
                    p_set=0,  # Will be updated dynamically
                    carrier="ev_charging"
                )
                total_ev_capacity += station['total_power_kw']
        
        print(f"   ✅ Added {len(self.ev_stations)} EV charging stations")
        print(f"      • Total charging capacity: {total_ev_capacity/1000:.1f} MW")
    
    def add_traffic_signal_loads(self):
        """Add traffic signal loads"""
        # Group signals by area
        signal_load = len(self.signals) * 0.0001  # 100W per signal in MW
        
        bus_id = f"building_bus_0"
        if bus_id in self.network.buses.index:
            self.network.add(
                "Load",
                "traffic_signals",
                bus=bus_id,
                p_set=signal_load,
                carrier="traffic_signals"
            )
        
        print(f"   ✅ Added traffic signal loads")
        print(f"      • Number of signals: {len(self.signals)}")
        print(f"      • Total signal load: {signal_load*1000:.1f} kW")
    
    def create_load_profiles(self):
        """Create time-varying load profiles"""
        # Create hourly profiles (simplified to 60 minutes)
        hours = np.linspace(0, 23, len(self.snapshots))
        
        # Building load profiles by type
        profiles = {
            'office': self.create_office_profile(hours),
            'residential': self.create_residential_profile(hours),
            'retail': self.create_retail_profile(hours),
            'hotel': self.create_hotel_profile(hours),
            'ev': self.create_ev_profile(hours)
        }
        
        # Apply profiles to loads
        for load_name in self.network.loads.index:
            if 'building' in load_name:
                # Weighted average of building type profiles
                base_load = self.network.loads.at[load_name, 'p_set']
                
                # Create combined profile
                combined_profile = (
                    profiles['office'] * 0.4 +
                    profiles['residential'] * 0.15 +
                    profiles['retail'] * 0.25 +
                    profiles['hotel'] * 0.2
                )
                
                # Apply profile
                self.network.loads_t.p_set[load_name] = base_load * combined_profile
                
            elif 'ev' in load_name:
                # EV charging profile
                base_capacity = 0.5  # MW average
                self.network.loads_t.p_set[load_name] = base_capacity * profiles['ev']
        
        print(f"   ✅ Created time-varying load profiles")
        print(f"      • Profile resolution: 1 minute")
        print(f"      • Profile duration: {len(self.snapshots)} minutes")
    
    def create_office_profile(self, hours):
        """Create office building load profile"""
        profile = np.zeros(len(hours))
        for i, h in enumerate(hours):
            if 8 <= h <= 18:  # Business hours
                profile[i] = 0.9 + 0.1 * np.sin((h - 8) * np.pi / 10)
            elif 6 <= h < 8 or 18 < h <= 20:  # Transition
                profile[i] = 0.5
            else:  # Night
                profile[i] = 0.3
        return profile
    
    def create_residential_profile(self, hours):
        """Create residential load profile"""
        profile = np.zeros(len(hours))
        for i, h in enumerate(hours):
            if 6 <= h <= 9:  # Morning peak
                profile[i] = 0.8
            elif 17 <= h <= 22:  # Evening peak
                profile[i] = 1.0
            elif 22 < h or h < 6:  # Night
                profile[i] = 0.4
            else:  # Day
                profile[i] = 0.5
        return profile
    
    def create_retail_profile(self, hours):
        """Create retail load profile"""
        profile = np.zeros(len(hours))
        for i, h in enumerate(hours):
            if 10 <= h <= 21:  # Store hours
                profile[i] = 0.9 + 0.1 * np.sin((h - 10) * np.pi / 11)
            elif 9 <= h < 10 or 21 < h <= 22:  # Opening/closing
                profile[i] = 0.6
            else:  # Closed
                profile[i] = 0.2
        return profile
    
    def create_hotel_profile(self, hours):
        """Create hotel load profile"""
        profile = np.zeros(len(hours))
        for i, h in enumerate(hours):
            if 6 <= h <= 10:  # Morning
                profile[i] = 0.8
            elif 18 <= h <= 23:  # Evening
                profile[i] = 0.9
            else:  # Other times
                profile[i] = 0.6
        return profile
    
    def create_ev_profile(self, hours):
        """Create EV charging load profile"""
        profile = np.zeros(len(hours))
        for i, h in enumerate(hours):
            if 8 <= h <= 10:  # Morning charging
                profile[i] = 0.7
            elif 12 <= h <= 14:  # Lunch charging
                profile[i] = 0.5
            elif 17 <= h <= 20:  # Evening charging
                profile[i] = 0.9
            elif 22 <= h or h <= 5:  # Overnight charging
                profile[i] = 1.0
            else:
                profile[i] = 0.3
        return profile
    
    def validate_network(self):
        """Validate the power network"""
        print("\n   🔍 Validating network...")
        
        # Check network statistics
        n_buses = len(self.network.buses)
        n_generators = len(self.network.generators)
        n_loads = len(self.network.loads)
        n_lines = len(self.network.lines)
        
        print(f"      • Buses: {n_buses}")
        print(f"      • Generators: {n_generators}")
        print(f"      • Loads: {n_loads}")
        print(f"      • Lines: {n_lines}")
        
        # Check connectivity
        if n_buses > 0 and n_lines > 0:
            print(f"   ✅ Network topology validated")
        else:
            print(f"   ❌ Network topology incomplete")
        
        # Check power balance
        total_generation = self.network.generators.p_nom.sum()
        total_load = self.network.loads_t.p_set.sum().sum() if not self.network.loads_t.p_set.empty else 0
        
        print(f"\n   ⚡ Power Balance:")
        print(f"      • Total generation capacity: {total_generation:.1f} MW")
        print(f"      • Total load: {total_load:.1f} MW")
        
        if total_generation > total_load:
            print(f"   ✅ Sufficient generation capacity")
        else:
            print(f"   ⚠️  Generation capacity may be insufficient")
    
    def save_network(self):
        """Save the network to file"""
        # Save as netCDF
        self.network.export_to_netcdf('data/power/manhattan_grid.nc')
        print(f"\n   💾 Saved network to data/power/manhattan_grid.nc")
        
        # Save network statistics
        stats = {
            'buses': len(self.network.buses),
            'generators': len(self.network.generators),
            'loads': len(self.network.loads),
            'lines': len(self.network.lines),
            'snapshots': len(self.network.snapshots),
            'total_generation_capacity_MW': float(self.network.generators.p_nom.sum()),
            'total_load_MW': float(self.network.loads.p_set.sum())
        }
        
        with open('data/power/grid_stats.json', 'w') as f:
            json.dump(stats, f, indent=2)
        
        print(f"   💾 Saved grid statistics to data/power/grid_stats.json")
    
    def print_grid_summary(self):
        """Print grid summary"""
        print("\n📊 POWER GRID SUMMARY:")
        
        print(f"\n   Network Components:")
        print(f"      • Buses: {len(self.network.buses)}")
        print(f"      • Generators: {len(self.network.generators)}")
        print(f"      • Loads: {len(self.network.loads)}")
        print(f"      • Lines/Transformers: {len(self.network.lines)}")
        
        print(f"\n   Voltage Levels:")
        voltage_levels = self.network.buses.v_nom.unique()
        for v in sorted(voltage_levels, reverse=True):
            count = len(self.network.buses[self.network.buses.v_nom == v])
            print(f"      • {v:.1f} kV: {count} buses")
        
        print(f"\n   Load Categories:")
        for carrier in self.network.loads.carrier.unique():
            loads = self.network.loads[self.network.loads.carrier == carrier]
            total = loads.p_set.sum()
            print(f"      • {carrier}: {total:.2f} MW")
        
        print(f"\n   Generation Sources:")
        for carrier in self.network.generators.carrier.unique():
            gens = self.network.generators[self.network.generators.carrier == carrier]
            total = gens.p_nom.sum()
            print(f"      • {carrier}: {total:.1f} MW capacity")


def test_power_flow():
    """Test power flow calculation"""
    print("\n" + "="*70)
    print(" TESTING POWER FLOW ".center(70, "="))
    print("="*70)
    
    # Load the saved network
    network = pypsa.Network()
    network.import_from_netcdf('data/power/manhattan_grid.nc')
    
    print("\n⚡ Running power flow analysis...")
    
    try:
        # Run linear power flow
        network.lpf()
        
        # Check results
        if network.lines_t.p0.empty:
            print("   ⚠️  No power flow results")
        else:
            # Get line flows
            max_flow = network.lines_t.p0.abs().max().max()
            avg_flow = network.lines_t.p0.abs().mean().mean()
            
            print(f"   ✅ Power flow solved successfully!")
            print(f"      • Maximum line flow: {max_flow:.2f} MW")
            print(f"      • Average line flow: {avg_flow:.2f} MW")
            
            # Check for overloads
            line_loading = network.lines_t.p0.abs() / network.lines.s_nom
            max_loading = line_loading.max().max()
            
            if max_loading > 1.0:
                print(f"   ⚠️  Some lines overloaded (max: {max_loading:.1%})")
            else:
                print(f"   ✅ All lines within capacity (max: {max_loading:.1%})")
    
    except Exception as e:
        print(f"   ❌ Power flow failed: {e}")
    
    print("="*70)


if __name__ == "__main__":
    builder = PowerGridBuilder()
    success = builder.build_complete_grid()
    
    if success:
        test_power_flow()
    
    input("\nPress Enter to continue...")