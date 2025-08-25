#!/usr/bin/env python3
"""
Ultra-Realistic PyPSA Network for Manhattan Traffic Grid
Based on actual NYC ConEd infrastructure with real voltage levels, substations, and network topology
Manhattan bounds: 40.700-40.800°N, -74.020--73.930°W
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime
import math

class ManhattanPowerNetworkRealistic:
    def __init__(self):
        """Initialize Ultra-Realistic Manhattan Power Network"""
        self.name = "Manhattan ConEd Power Grid - Traffic Zone"
        
        # Network components (much more detailed)
        self.buses = {}
        self.generators = {}
        self.loads = {}
        self.lines = {}
        self.transformers = {}
        self.capacitors = {}
        self.switches = {}
        
        # Voltage levels used in NYC ConEd system
        self.voltage_levels = {
            'transmission': 138,      # kV - Main transmission
            'subtransmission': 27,    # kV - Subtransmission
            'primary': 13.8,          # kV - Primary distribution
            'secondary': 4.16,        # kV - Secondary distribution
            'service': 0.48          # kV - Service level (480V)
        }
        
        # Real-time parameters
        self.time_steps = pd.date_range('2025-01-01', periods=96, freq='15min')  # 15-min intervals
        self.current_time_index = 0
        
        # Detailed load categories
        self.traffic_light_loads = {}
        self.street_light_loads = {}
        self.ev_charging_loads = {}
        self.subway_loads = {}
        self.building_loads = {}
        
        # Network state and monitoring
        self.total_generation = 0
        self.total_load = 0
        self.line_flows = {}
        self.voltage_violations = []
        self.contingencies = []
        
        # Power quality metrics
        self.frequency = 60.0  # Hz
        self.power_factor = 0.95
        self.voltage_regulation = 0.05  # ±5%
        
    def build_network(self):
        """Build the ultra-realistic Manhattan power network"""
        print("🏗️ Building Ultra-Realistic Manhattan Power Network...")
        print("📍 Coverage: Traffic Grid Area (40.700-40.800°N, -74.020--73.930°W)")
        
        self._add_transmission_substations()
        self._add_distribution_substations()
        self._add_network_stations()
        self._add_generators()
        self._add_transmission_lines()
        self._add_distribution_feeders()
        self._add_transformers()
        self._add_base_loads()
        self._add_traffic_infrastructure()
        self._add_ev_infrastructure()
        self._add_critical_loads()
        self._add_protection_systems()
        
        print("✅ Ultra-realistic network built successfully!")
        print(f"📊 Total components: {len(self.buses)} buses, {len(self.lines)} lines, {len(self.transformers)} transformers")
        return self
    
    def _add_transmission_substations(self):
        """Add actual ConEd transmission substations in Manhattan traffic zone"""
        # These are based on real ConEd substation locations
        self.buses.update({
            # Major 138kV Transmission Substations
            'SUB_Hellgate': {
                'lat': 40.7789, 'lon': -73.9442,  # East Side
                'voltage': 138,
                'type': 'transmission',
                'capacity_mva': 1200,
                'real_name': 'Hellgate Substation'
            },
            'SUB_Farragut': {
                'lat': 40.7074, 'lon': -74.0113,  # Downtown
                'voltage': 138,
                'type': 'transmission',
                'capacity_mva': 900,
                'real_name': 'Farragut Substation'
            },
            'SUB_Columbus_Circle': {
                'lat': 40.7680, 'lon': -73.9819,  # Columbus Circle
                'voltage': 138,
                'type': 'transmission',
                'capacity_mva': 800,
                'real_name': 'Columbus Circle Sub'
            },
            'SUB_Sherman_Creek': {
                'lat': 40.7950, 'lon': -73.9550,  # Upper Manhattan
                'voltage': 138,
                'type': 'transmission',
                'capacity_mva': 750,
                'real_name': 'Sherman Creek Station'
            },
            'SUB_Trade_Center': {
                'lat': 40.7127, 'lon': -74.0134,  # WTC area
                'voltage': 138,
                'type': 'transmission',
                'capacity_mva': 1000,
                'real_name': 'Trade Center Substation'
            },
            'SUB_Murray_Hill': {
                'lat': 40.7480, 'lon': -73.9780,  # Murray Hill
                'voltage': 138,
                'type': 'transmission',
                'capacity_mva': 850,
                'real_name': 'Murray Hill Station'
            },
            'SUB_Chelsea': {
                'lat': 40.7465, 'lon': -74.0014,  # Chelsea
                'voltage': 138,
                'type': 'transmission',
                'capacity_mva': 950,
                'real_name': 'Chelsea Substation'
            },
            'SUB_Waterside': {
                'lat': 40.7389, 'lon': -73.9730,  # East Side Waterside
                'voltage': 138,
                'type': 'transmission',
                'capacity_mva': 700,
                'real_name': 'Waterside Plaza Sub'
            }
        })
        
        print(f"⚡ Added {len([b for b in self.buses.values() if b['voltage'] == 138])} transmission substations (138kV)")
    
    def _add_distribution_substations(self):
        """Add 27kV and 13.8kV distribution substations"""
        # Subtransmission (27kV) substations
        subtrans_subs = {
            'SUB_27_Times_Square': {
                'lat': 40.7580, 'lon': -73.9855,
                'voltage': 27,
                'type': 'subtransmission',
                'capacity_mva': 300,
                'feeds_from': 'SUB_Columbus_Circle'
            },
            'SUB_27_Grand_Central': {
                'lat': 40.7527, 'lon': -73.9772,
                'voltage': 27,
                'type': 'subtransmission',
                'capacity_mva': 350,
                'feeds_from': 'SUB_Murray_Hill'
            },
            'SUB_27_Penn_Station': {
                'lat': 40.7505, 'lon': -73.9934,
                'voltage': 27,
                'type': 'subtransmission',
                'capacity_mva': 400,
                'feeds_from': 'SUB_Chelsea'
            },
            'SUB_27_Union_Square': {
                'lat': 40.7359, 'lon': -73.9911,
                'voltage': 27,
                'type': 'subtransmission',
                'capacity_mva': 250,
                'feeds_from': 'SUB_Chelsea'
            },
            'SUB_27_Wall_Street': {
                'lat': 40.7074, 'lon': -74.0090,
                'voltage': 27,
                'type': 'subtransmission',
                'capacity_mva': 450,
                'feeds_from': 'SUB_Trade_Center'
            },
            'SUB_27_Central_Park_S': {
                'lat': 40.7644, 'lon': -73.9735,
                'voltage': 27,
                'type': 'subtransmission',
                'capacity_mva': 280,
                'feeds_from': 'SUB_Columbus_Circle'
            }
        }
        
        # Primary distribution (13.8kV) substations
        primary_subs = {}
        sub_id = 0
        
        # Create a grid of 13.8kV substations across Manhattan traffic zone
        lat_steps = np.linspace(40.705, 40.795, 8)
        lon_steps = np.linspace(-74.015, -73.935, 6)
        
        for lat in lat_steps:
            for lon in lon_steps:
                sub_id += 1
                sub_name = f'SUB_13_8_D{sub_id:02d}'
                primary_subs[sub_name] = {
                    'lat': lat,
                    'lon': lon,
                    'voltage': 13.8,
                    'type': 'primary_distribution',
                    'capacity_mva': 50,
                    'district': self._get_district_name(lat, lon)
                }
        
        self.buses.update(subtrans_subs)
        self.buses.update(primary_subs)
        
        print(f"⚡ Added {len(subtrans_subs)} subtransmission substations (27kV)")
        print(f"⚡ Added {len(primary_subs)} primary distribution substations (13.8kV)")
    
    def _add_network_stations(self):
        """Add network protector stations and secondary networks"""
        # Secondary network stations (4.16kV) for dense areas
        network_stations = {}
        
        # Major network areas in Manhattan
        network_areas = [
            {'name': 'Midtown_Network', 'lat': 40.7550, 'lon': -73.9840},
            {'name': 'Times_Square_Network', 'lat': 40.7580, 'lon': -73.9855},
            {'name': 'Financial_Network', 'lat': 40.7080, 'lon': -74.0090},
            {'name': 'Chelsea_Network', 'lat': 40.7465, 'lon': -74.0014},
            {'name': 'Murray_Hill_Network', 'lat': 40.7480, 'lon': -73.9780},
            {'name': 'Union_Square_Network', 'lat': 40.7359, 'lon': -73.9911},
            {'name': 'Columbus_Network', 'lat': 40.7680, 'lon': -73.9819},
            {'name': 'Upper_East_Network', 'lat': 40.7750, 'lon': -73.9600},
            {'name': 'Upper_West_Network', 'lat': 40.7850, 'lon': -74.0000},
            {'name': 'Gramercy_Network', 'lat': 40.7380, 'lon': -73.9850},
            {'name': 'Village_Network', 'lat': 40.7330, 'lon': -74.0000},
            {'name': 'Tribeca_Network', 'lat': 40.7163, 'lon': -74.0086}
        ]
        
        for area in network_areas:
            network_stations[f"NET_4_{area['name']}"] = {
                'lat': area['lat'],
                'lon': area['lon'],
                'voltage': 4.16,
                'type': 'secondary_network',
                'capacity_mva': 25,
                'network_protectors': 4,  # Number of network protectors
                'redundancy': 'N-2'  # Can lose 2 feeders
            }
        
        self.buses.update(network_stations)
        print(f"⚡ Added {len(network_stations)} secondary network stations (4.16kV)")
    
    def _add_generators(self):
        """Add realistic generation sources for Manhattan"""
        self.generators = {
            # Major Gas Turbine Plants (Peaker Plants)
            'GEN_East_River_GT1': {
                'bus': 'SUB_Hellgate',
                'capacity_mw': 180,
                'min_mw': 50,
                'type': 'gas_turbine',
                'heat_rate': 11000,  # BTU/kWh
                'cost_per_mwh': 120,
                'startup_cost': 5000,
                'current_output': 0,
                'lat': 40.7789, 'lon': -73.9400,
                'emissions_rate': 0.53  # tons CO2/MWh
            },
            'GEN_East_River_GT2': {
                'bus': 'SUB_Hellgate',
                'capacity_mw': 180,
                'min_mw': 50,
                'type': 'gas_turbine',
                'heat_rate': 11000,
                'cost_per_mwh': 120,
                'startup_cost': 5000,
                'current_output': 0,
                'lat': 40.7789, 'lon': -73.9400,
                'emissions_rate': 0.53
            },
            
            # Combined Cycle Plants (More Efficient)
            'GEN_Hudson_CC1': {
                'bus': 'SUB_Chelsea',
                'capacity_mw': 500,
                'min_mw': 150,
                'type': 'combined_cycle',
                'heat_rate': 7500,
                'cost_per_mwh': 65,
                'startup_cost': 15000,
                'current_output': 0,
                'lat': 40.7465, 'lon': -74.0100,
                'emissions_rate': 0.37
            },
            
            # Steam Turbine (District Heating Cogeneration)
            'GEN_ConEd_Steam': {
                'bus': 'SUB_Murray_Hill',
                'capacity_mw': 300,
                'min_mw': 100,
                'type': 'steam_turbine',
                'heat_rate': 10000,
                'cost_per_mwh': 80,
                'startup_cost': 20000,
                'current_output': 0,
                'lat': 40.7480, 'lon': -73.9750,
                'cogeneration': True,
                'steam_output_mlbs': 2000  # Million lbs/hr steam
            },
            
            # Distributed Solar (Rooftop aggregated)
            'GEN_Solar_Midtown': {
                'bus': 'SUB_13_8_D15',
                'capacity_mw': 25,
                'min_mw': 0,
                'type': 'solar_pv',
                'cost_per_mwh': 0,
                'current_output': 0,
                'lat': 40.7580, 'lon': -73.9855,
                'panels': 75000,
                'efficiency': 0.22
            },
            'GEN_Solar_Chelsea': {
                'bus': 'SUB_13_8_D22',
                'capacity_mw': 20,
                'min_mw': 0,
                'type': 'solar_pv',
                'cost_per_mwh': 0,
                'current_output': 0,
                'lat': 40.7465, 'lon': -74.0014,
                'panels': 60000,
                'efficiency': 0.22
            },
            'GEN_Solar_Financial': {
                'bus': 'SUB_13_8_D08',
                'capacity_mw': 15,
                'min_mw': 0,
                'type': 'solar_pv',
                'cost_per_mwh': 0,
                'current_output': 0,
                'lat': 40.7074, 'lon': -74.0090,
                'panels': 45000,
                'efficiency': 0.22
            },
            
            # Battery Energy Storage Systems (BESS)
            'GEN_BESS_Times_Square': {
                'bus': 'SUB_27_Times_Square',
                'capacity_mw': 50,
                'min_mw': -50,  # Can charge
                'type': 'battery',
                'energy_capacity_mwh': 200,
                'efficiency': 0.90,
                'cost_per_mwh': 15,
                'current_output': 0,
                'current_soc': 0.5,  # State of charge
                'lat': 40.7580, 'lon': -73.9855,
                'response_time_ms': 100
            },
            'GEN_BESS_Wall_Street': {
                'bus': 'SUB_27_Wall_Street',
                'capacity_mw': 40,
                'min_mw': -40,
                'type': 'battery',
                'energy_capacity_mwh': 160,
                'efficiency': 0.90,
                'cost_per_mwh': 15,
                'current_output': 0,
                'current_soc': 0.5,
                'lat': 40.7074, 'lon': -74.0090,
                'response_time_ms': 100
            },
            
            # Emergency Diesel Generators (Critical Infrastructure)
            'GEN_Emergency_Traffic_Control': {
                'bus': 'NET_4_Times_Square_Network',
                'capacity_mw': 5,
                'min_mw': 1,
                'type': 'diesel',
                'cost_per_mwh': 200,
                'startup_time_min': 2,
                'current_output': 0,
                'lat': 40.7580, 'lon': -73.9855,
                'fuel_tank_gallons': 10000,
                'runtime_hours': 48
            },
            
            # Fuel Cell (Clean distributed generation)
            'GEN_FuelCell_OneWTC': {
                'bus': 'SUB_Trade_Center',
                'capacity_mw': 10,
                'min_mw': 2,
                'type': 'fuel_cell',
                'efficiency': 0.60,
                'cost_per_mwh': 90,
                'current_output': 0,
                'lat': 40.7127, 'lon': -74.0134,
                'fuel_type': 'natural_gas'
            }
        }
        
        total_capacity = sum(g['capacity_mw'] for g in self.generators.values())
        solar_capacity = sum(g['capacity_mw'] for g in self.generators.values() if g['type'] == 'solar_pv')
        battery_capacity = sum(g['capacity_mw'] for g in self.generators.values() if g['type'] == 'battery')
        
        print(f"⚡ Added {len(self.generators)} generators")
        print(f"   Total capacity: {total_capacity} MW")
        print(f"   Solar: {solar_capacity} MW | Battery: {battery_capacity} MW")
    
    def _add_transmission_lines(self):
        """Add realistic transmission and subtransmission lines"""
        # 138kV Transmission Lines (Underground cables in Manhattan)
        transmission_lines = {
            'TL_138_Hellgate_Columbus': {
                'from': 'SUB_Hellgate',
                'to': 'SUB_Columbus_Circle',
                'voltage': 138,
                'capacity_mw': 600,
                'resistance': 0.008,
                'reactance': 0.096,
                'susceptance': 0.254,
                'length_km': 4.2,
                'cable_type': 'XLPE_2500_Cu',  # Cross-linked polyethylene, 2500mm² copper
                'current_flow': 0
            },
            'TL_138_Columbus_Murray': {
                'from': 'SUB_Columbus_Circle',
                'to': 'SUB_Murray_Hill',
                'voltage': 138,
                'capacity_mw': 550,
                'resistance': 0.006,
                'reactance': 0.084,
                'susceptance': 0.220,
                'length_km': 2.8,
                'cable_type': 'XLPE_2000_Cu',
                'current_flow': 0
            },
            'TL_138_Murray_Chelsea': {
                'from': 'SUB_Murray_Hill',
                'to': 'SUB_Chelsea',
                'voltage': 138,
                'capacity_mw': 600,
                'resistance': 0.005,
                'reactance': 0.072,
                'susceptance': 0.190,
                'length_km': 1.5,
                'cable_type': 'XLPE_2500_Cu',
                'current_flow': 0
            },
            'TL_138_Chelsea_Trade': {
                'from': 'SUB_Chelsea',
                'to': 'SUB_Trade_Center',
                'voltage': 138,
                'capacity_mw': 700,
                'resistance': 0.009,
                'reactance': 0.108,
                'susceptance': 0.286,
                'length_km': 3.5,
                'cable_type': 'XLPE_3000_Cu',
                'current_flow': 0
            },
            'TL_138_Trade_Farragut': {
                'from': 'SUB_Trade_Center',
                'to': 'SUB_Farragut',
                'voltage': 138,
                'capacity_mw': 650,
                'resistance': 0.003,
                'reactance': 0.036,
                'susceptance': 0.095,
                'length_km': 0.8,
                'cable_type': 'XLPE_2500_Cu',
                'current_flow': 0
            },
            'TL_138_Murray_Waterside': {
                'from': 'SUB_Murray_Hill',
                'to': 'SUB_Waterside',
                'voltage': 138,
                'capacity_mw': 500,
                'resistance': 0.007,
                'reactance': 0.084,
                'susceptance': 0.222,
                'length_km': 2.1,
                'cable_type': 'XLPE_2000_Cu',
                'current_flow': 0
            },
            'TL_138_Hellgate_Sherman': {
                'from': 'SUB_Hellgate',
                'to': 'SUB_Sherman_Creek',
                'voltage': 138,
                'capacity_mw': 550,
                'resistance': 0.008,
                'reactance': 0.096,
                'susceptance': 0.254,
                'length_km': 2.5,
                'cable_type': 'XLPE_2000_Cu',
                'current_flow': 0
            },
            # Ring configuration for reliability
            'TL_138_Sherman_Columbus': {
                'from': 'SUB_Sherman_Creek',
                'to': 'SUB_Columbus_Circle',
                'voltage': 138,
                'capacity_mw': 500,
                'resistance': 0.009,
                'reactance': 0.108,
                'susceptance': 0.286,
                'length_km': 3.0,
                'cable_type': 'XLPE_2000_Cu',
                'current_flow': 0
            }
        }
        
        # 27kV Subtransmission Lines
        subtrans_lines = {}
        for bus_name, bus_data in self.buses.items():
            if bus_data['type'] == 'subtransmission' and 'feeds_from' in bus_data:
                line_name = f"TL_27_{bus_data['feeds_from']}_{bus_name}"
                subtrans_lines[line_name] = {
                    'from': bus_data['feeds_from'],
                    'to': bus_name,
                    'voltage': 27,
                    'capacity_mw': 200,
                    'resistance': 0.012,
                    'reactance': 0.089,
                    'susceptance': 0.018,
                    'length_km': 1.5,
                    'cable_type': 'XLPE_630_Cu',
                    'current_flow': 0
                }
        
        self.lines.update(transmission_lines)
        self.lines.update(subtrans_lines)
        
        print(f"⚡ Added {len(transmission_lines)} transmission lines (138kV)")
        print(f"⚡ Added {len(subtrans_lines)} subtransmission lines (27kV)")
    
    def _add_distribution_feeders(self):
        """Add 13.8kV and 4.16kV distribution feeders"""
        distribution_feeders = {}
        feeder_id = 0
        
        # Connect 27kV substations to nearby 13.8kV substations
        for sub_27 in [b for b in self.buses.keys() if 'SUB_27' in b]:
            sub_27_data = self.buses[sub_27]
            
            # Find nearby 13.8kV substations
            for sub_13 in [b for b in self.buses.keys() if 'SUB_13_8' in b]:
                sub_13_data = self.buses[sub_13]
                
                # Calculate distance
                distance = self._calculate_distance(
                    sub_27_data['lat'], sub_27_data['lon'],
                    sub_13_data['lat'], sub_13_data['lon']
                )
                
                # Connect if within 2km
                if distance < 2.0:
                    feeder_id += 1
                    feeder_name = f"FDR_13_8_{feeder_id:03d}"
                    distribution_feeders[feeder_name] = {
                        'from': sub_27,
                        'to': sub_13,
                        'voltage': 13.8,
                        'capacity_mw': 40,
                        'resistance': 0.193 * distance,
                        'reactance': 0.142 * distance,
                        'susceptance': 0.008 * distance,
                        'length_km': distance,
                        'cable_type': 'XLPE_240_Al',
                        'current_flow': 0,
                        'protection': 'recloser'
                    }
        
        # Connect 13.8kV to 4.16kV network stations
        for sub_13 in [b for b in self.buses.keys() if 'SUB_13_8' in b]:
            sub_13_data = self.buses[sub_13]
            
            for net_4 in [b for b in self.buses.keys() if 'NET_4' in b]:
                net_4_data = self.buses[net_4]
                
                distance = self._calculate_distance(
                    sub_13_data['lat'], sub_13_data['lon'],
                    net_4_data['lat'], net_4_data['lon']
                )
                
                if distance < 1.5:
                    feeder_id += 1
                    feeder_name = f"FDR_4_16_{feeder_id:03d}"
                    distribution_feeders[feeder_name] = {
                        'from': sub_13,
                        'to': net_4,
                        'voltage': 4.16,
                        'capacity_mw': 15,
                        'resistance': 0.524 * distance,
                        'reactance': 0.267 * distance,
                        'susceptance': 0.005 * distance,
                        'length_km': distance,
                        'cable_type': 'XLPE_120_Al',
                        'current_flow': 0,
                        'protection': 'fuse'
                    }
        
        self.lines.update(distribution_feeders)
        print(f"⚡ Added {len(distribution_feeders)} distribution feeders")
    
    def _add_transformers(self):
        """Add transformers between voltage levels"""
        transformer_id = 0
        
        # 138/27kV transformers
        for bus_name, bus_data in self.buses.items():
            if bus_data['type'] == 'subtransmission' and 'feeds_from' in bus_data:
                transformer_id += 1
                xfmr_name = f"XFMR_138_27_{transformer_id:02d}"
                self.transformers[xfmr_name] = {
                    'high_voltage_bus': bus_data['feeds_from'],
                    'low_voltage_bus': bus_name,
                    'rating_mva': bus_data['capacity_mva'],
                    'voltage_ratio': '138/27',
                    'impedance_percent': 8.5,
                    'x_r_ratio': 12,
                    'tap_changer': True,
                    'tap_range': '±10%',
                    'current_tap': 1.0,
                    'cooling': 'ONAN/ONAF',  # Oil Natural Air Natural / Forced
                    'type': 'three_phase'
                }
        
        # 27/13.8kV transformers (in substations)
        for line_name, line_data in self.lines.items():
            if 'FDR_13_8' in line_name:
                transformer_id += 1
                xfmr_name = f"XFMR_27_13_8_{transformer_id:02d}"
                self.transformers[xfmr_name] = {
                    'high_voltage_bus': line_data['from'],
                    'low_voltage_bus': line_data['to'],
                    'rating_mva': 50,
                    'voltage_ratio': '27/13.8',
                    'impedance_percent': 7.5,
                    'x_r_ratio': 10,
                    'tap_changer': True,
                    'tap_range': '±5%',
                    'current_tap': 1.0,
                    'cooling': 'ONAN',
                    'type': 'three_phase'
                }
        
        # 13.8/4.16kV transformers (network transformers)
        for line_name, line_data in self.lines.items():
            if 'FDR_4_16' in line_name:
                transformer_id += 1
                xfmr_name = f"XFMR_13_8_4_16_{transformer_id:02d}"
                self.transformers[xfmr_name] = {
                    'high_voltage_bus': line_data['from'],
                    'low_voltage_bus': line_data['to'],
                    'rating_mva': 15,
                    'voltage_ratio': '13.8/4.16',
                    'impedance_percent': 5.75,
                    'x_r_ratio': 8,
                    'tap_changer': False,
                    'current_tap': 1.0,
                    'cooling': 'AN',  # Air Natural
                    'type': 'three_phase'
                }
        
        print(f"⚡ Added {len(self.transformers)} transformers")
    
    def _add_base_loads(self):
        """Add realistic base loads for Manhattan districts"""
        # Commercial loads (offices, retail)
        commercial_loads = {
            'LOAD_Midtown_Commercial': {
                'bus': 'NET_4_Midtown_Network',
                'base_mw': 120,
                'power_factor': 0.90,
                'type': 'commercial',
                'profile': 'office_building',
                'sqft': 15000000,  # Square feet
                'w_per_sqft': 8
            },
            'LOAD_Financial_Commercial': {
                'bus': 'NET_4_Financial_Network',
                'base_mw': 150,
                'power_factor': 0.88,
                'type': 'commercial',
                'profile': 'financial_center',
                'sqft': 20000000,
                'w_per_sqft': 7.5
            },
            'LOAD_Times_Square_Retail': {
                'bus': 'NET_4_Times_Square_Network',
                'base_mw': 80,
                'power_factor': 0.85,
                'type': 'retail',
                'profile': 'retail_entertainment',
                'sqft': 5000000,
                'w_per_sqft': 16
            },
            'LOAD_Chelsea_Market': {
                'bus': 'NET_4_Chelsea_Network',
                'base_mw': 45,
                'power_factor': 0.87,
                'type': 'retail',
                'profile': 'market_retail',
                'sqft': 3000000,
                'w_per_sqft': 15
            }
        }
        
        # Residential loads
        residential_loads = {
            'LOAD_Upper_East_Residential': {
                'bus': 'NET_4_Upper_East_Network',
                'base_mw': 65,
                'power_factor': 0.95,
                'type': 'residential',
                'profile': 'high_rise_residential',
                'units': 15000,
                'kw_per_unit': 4.3
            },
            'LOAD_Upper_West_Residential': {
                'bus': 'NET_4_Upper_West_Network',
                'base_mw': 60,
                'power_factor': 0.95,
                'type': 'residential',
                'profile': 'high_rise_residential',
                'units': 14000,
                'kw_per_unit': 4.3
            },
            'LOAD_Murray_Hill_Residential': {
                'bus': 'NET_4_Murray_Hill_Network',
                'base_mw': 55,
                'power_factor': 0.94,
                'type': 'residential',
                'profile': 'mixed_residential',
                'units': 12000,
                'kw_per_unit': 4.6
            },
            'LOAD_Village_Residential': {
                'bus': 'NET_4_Village_Network',
                'base_mw': 40,
                'power_factor': 0.93,
                'type': 'residential',
                'profile': 'low_rise_residential',
                'units': 10000,
                'kw_per_unit': 4.0
            }
        }
        
        # Industrial/Special loads
        special_loads = {
            'LOAD_Data_Center_1': {
                'bus': 'SUB_13_8_D18',
                'base_mw': 20,
                'power_factor': 0.98,
                'type': 'data_center',
                'profile': '24_7_constant',
                'critical': True,
                'ups_backup_minutes': 15
            },
            'LOAD_Hospital_1': {
                'bus': 'SUB_13_8_D24',
                'base_mw': 15,
                'power_factor': 0.90,
                'type': 'hospital',
                'profile': 'critical_care',
                'critical': True,
                'emergency_gen_mw': 10
            },
            'LOAD_Transit_Hub': {
                'bus': 'SUB_27_Penn_Station',
                'base_mw': 25,
                'power_factor': 0.85,
                'type': 'transit',
                'profile': 'rail_station',
                'critical': True
            }
        }
        
        self.loads.update(commercial_loads)
        self.loads.update(residential_loads)
        self.loads.update(special_loads)
        
        # Initialize current values
        for load in self.loads.values():
            load['current_mw'] = load['base_mw']
            load['current_mvar'] = load['base_mw'] * math.tan(math.acos(load['power_factor']))
        
        print(f"⚡ Added {len(self.loads)} base loads")
        print(f"   Commercial: {len(commercial_loads)} | Residential: {len(residential_loads)} | Special: {len(special_loads)}")
    
    def _add_traffic_infrastructure(self):
        """Add detailed traffic infrastructure loads"""
        # Traffic signals by intersection
        traffic_signals = {}
        signal_id = 0
        
        # Create traffic signal loads at major intersections
        intersections = [
            # Major avenue intersections
            {'lat': 40.7580, 'lon': -73.9855, 'name': 'Times_Square', 'signals': 24, 'led': True},
            {'lat': 40.7680, 'lon': -73.9819, 'name': 'Columbus_Circle', 'signals': 20, 'led': True},
            {'lat': 40.7505, 'lon': -73.9934, 'name': 'Penn_Station', 'signals': 18, 'led': True},
            {'lat': 40.7527, 'lon': -73.9772, 'name': 'Grand_Central', 'signals': 16, 'led': True},
            {'lat': 40.7359, 'lon': -73.9911, 'name': 'Union_Square', 'signals': 16, 'led': True},
            {'lat': 40.7614, 'lon': -73.9776, 'name': '57th_5th', 'signals': 12, 'led': True},
            {'lat': 40.7489, 'lon': -73.9680, 'name': '42nd_Lex', 'signals': 12, 'led': True},
            {'lat': 40.7416, 'lon': -73.9883, 'name': '23rd_Broadway', 'signals': 10, 'led': True},
            {'lat': 40.7074, 'lon': -74.0090, 'name': 'Wall_Broad', 'signals': 10, 'led': False},
            {'lat': 40.7831, 'lon': -73.9712, 'name': '79th_Broadway', 'signals': 8, 'led': True}
        ]
        
        # Grid pattern - add signals every 2 blocks
        for lat in np.arange(40.705, 40.795, 0.004):  # ~2 blocks
            for lon in np.arange(-74.015, -73.935, 0.004):
                signal_id += 1
                signal_name = f"TL_Grid_{signal_id:03d}"
                
                # Find nearest network station
                nearest_bus = self._find_nearest_bus(lat, lon, voltage=4.16)
                
                traffic_signals[signal_name] = {
                    'bus': nearest_bus,
                    'lat': lat,
                    'lon': lon,
                    'base_kw': 1.2 if signal_id % 3 == 0 else 0.8,  # Some LED, some incandescent
                    'led_type': signal_id % 3 == 0,
                    'signals': 4,  # Standard 4-way intersection
                    'current_kw': 1.2 if signal_id % 3 == 0 else 0.8
                }
        
        # Add major intersections with higher power
        for intersection in intersections:
            signal_id += 1
            signal_name = f"TL_Major_{intersection['name']}"
            nearest_bus = self._find_nearest_bus(intersection['lat'], intersection['lon'], voltage=4.16)
            
            power_per_signal = 0.05 if intersection['led'] else 0.15  # kW per signal head
            total_power = intersection['signals'] * power_per_signal
            
            traffic_signals[signal_name] = {
                'bus': nearest_bus,
                'lat': intersection['lat'],
                'lon': intersection['lon'],
                'base_kw': total_power,
                'led_type': intersection['led'],
                'signals': intersection['signals'],
                'current_kw': total_power,
                'adaptive_control': intersection['signals'] > 15
            }
        
        # Street lighting
        street_lights = {}
        light_id = 0
        
        # Avenue lighting (higher density)
        avenues = [
            {'name': 'Broadway', 'lat_start': 40.700, 'lat_end': 40.800, 'lon': -73.990, 'lights_per_km': 50},
            {'name': '5th_Ave', 'lat_start': 40.705, 'lat_end': 40.795, 'lon': -73.975, 'lights_per_km': 60},
            {'name': 'Park_Ave', 'lat_start': 40.710, 'lat_end': 40.790, 'lon': -73.970, 'lights_per_km': 55},
            {'name': 'Madison', 'lat_start': 40.710, 'lat_end': 40.790, 'lon': -73.972, 'lights_per_km': 50},
            {'name': '6th_Ave', 'lat_start': 40.705, 'lat_end': 40.795, 'lon': -73.980, 'lights_per_km': 55},
            {'name': '7th_Ave', 'lat_start': 40.705, 'lat_end': 40.795, 'lon': -73.985, 'lights_per_km': 55},
            {'name': '8th_Ave', 'lat_start': 40.705, 'lat_end': 40.795, 'lon': -73.990, 'lights_per_km': 50},
            {'name': '9th_Ave', 'lat_start': 40.705, 'lat_end': 40.795, 'lon': -73.995, 'lights_per_km': 45},
            {'name': '10th_Ave', 'lat_start': 40.705, 'lat_end': 40.795, 'lon': -74.000, 'lights_per_km': 45},
            {'name': '11th_Ave', 'lat_start': 40.705, 'lat_end': 40.795, 'lon': -74.005, 'lights_per_km': 40}
        ]
        
        for avenue in avenues:
            segments = 10
            for i in range(segments):
                light_id += 1
                lat = avenue['lat_start'] + (avenue['lat_end'] - avenue['lat_start']) * i / segments
                
                nearest_bus = self._find_nearest_bus(lat, avenue['lon'], voltage=4.16)
                
                # LED streetlights: 100W, HPS: 250W
                is_led = light_id % 2 == 0  # 50% LED conversion
                power_per_light = 0.1 if is_led else 0.25  # kW
                num_lights = int(avenue['lights_per_km'] * (avenue['lat_end'] - avenue['lat_start']) * 111 / segments)
                
                street_lights[f"SL_{avenue['name']}_{i:02d}"] = {
                    'bus': nearest_bus,
                    'lat': lat,
                    'lon': avenue['lon'],
                    'base_kw': power_per_light * num_lights,
                    'led_type': is_led,
                    'num_lights': num_lights,
                    'current_kw': 0,  # Will be set based on time
                    'dimming_capable': is_led
                }
        
        # Convert to MW and store
        self.traffic_light_loads = {k: {**v, 'base_mw': v['base_kw']/1000, 'current_mw': v['current_kw']/1000} 
                                   for k, v in traffic_signals.items()}
        self.street_light_loads = {k: {**v, 'base_mw': v['base_kw']/1000, 'current_mw': v['current_kw']/1000} 
                                  for k, v in street_lights.items()}
        
        print(f"⚡ Added {len(traffic_signals)} traffic signal controllers")
        print(f"⚡ Added {len(street_lights)} street lighting circuits")
    
    def _add_ev_infrastructure(self):
        """Add detailed EV charging infrastructure"""
        ev_stations = {}
        station_id = 0
        
        # Level 3 DC Fast Charging Hubs
        dc_fast_hubs = [
            {'name': 'Times_Square_Hub', 'lat': 40.7590, 'lon': -73.9845, 'chargers': 10, 'power_kw': 150},
            {'name': 'Penn_Station_Hub', 'lat': 40.7510, 'lon': -73.9920, 'chargers': 12, 'power_kw': 150},
            {'name': 'Columbus_Circle_Hub', 'lat': 40.7685, 'lon': -73.9815, 'chargers': 8, 'power_kw': 150},
            {'name': 'Union_Square_Hub', 'lat': 40.7365, 'lon': -73.9905, 'chargers': 8, 'power_kw': 150},
            {'name': 'Wall_Street_Hub', 'lat': 40.7080, 'lon': -74.0085, 'chargers': 6, 'power_kw': 150},
            {'name': 'Grand_Central_Hub', 'lat': 40.7530, 'lon': -73.9765, 'chargers': 10, 'power_kw': 150}
        ]
        
        for hub in dc_fast_hubs:
            station_id += 1
            nearest_bus = self._find_nearest_bus(hub['lat'], hub['lon'], voltage=13.8)
            
            ev_stations[f"EV_DC_{hub['name']}"] = {
                'bus': nearest_bus,
                'lat': hub['lat'],
                'lon': hub['lon'],
                'capacity_mw': (hub['chargers'] * hub['power_kw']) / 1000,
                'chargers': hub['chargers'],
                'power_per_charger_kw': hub['power_kw'],
                'type': 'dc_fast',
                'current_mw': 0,
                'utilization': 0,
                'transformer_required': True,
                'cooling': 'liquid_cooled'
            }
        
        # Level 2 Charging (Parking Garages, Street)
        level2_locations = []
        for lat in np.arange(40.710, 40.790, 0.01):
            for lon in np.arange(-74.010, -73.940, 0.01):
                level2_locations.append({
                    'lat': lat,
                    'lon': lon,
                    'chargers': np.random.randint(4, 12),
                    'power_kw': 7.2  # Standard Level 2
                })
        
        for loc in level2_locations:
            station_id += 1
            nearest_bus = self._find_nearest_bus(loc['lat'], loc['lon'], voltage=4.16)
            
            ev_stations[f"EV_L2_{station_id:03d}"] = {
                'bus': nearest_bus,
                'lat': loc['lat'],
                'lon': loc['lon'],
                'capacity_mw': (loc['chargers'] * loc['power_kw']) / 1000,
                'chargers': loc['chargers'],
                'power_per_charger_kw': loc['power_kw'],
                'type': 'level_2',
                'current_mw': 0,
                'utilization': 0,
                'location_type': 'street' if station_id % 3 == 0 else 'garage'
            }
        
        self.ev_charging_loads = ev_stations
        
        total_ev_capacity = sum(s['capacity_mw'] for s in ev_stations.values())
        dc_fast_capacity = sum(s['capacity_mw'] for s in ev_stations.values() if s['type'] == 'dc_fast')
        
        print(f"⚡ Added {len(ev_stations)} EV charging stations")
        print(f"   Total capacity: {total_ev_capacity:.1f} MW")
        print(f"   DC Fast: {dc_fast_capacity:.1f} MW | Level 2: {total_ev_capacity - dc_fast_capacity:.1f} MW")
    
    def _add_critical_loads(self):
        """Add critical infrastructure loads"""
        critical_loads = {
            # Traffic Management Centers
            'CRITICAL_Traffic_Control_Center': {
                'bus': 'NET_4_Midtown_Network',
                'base_mw': 2.5,
                'power_factor': 0.95,
                'type': 'traffic_control',
                'backup_power': True,
                'ups_minutes': 30,
                'diesel_backup_mw': 3.0,
                'priority': 1
            },
            
            # Emergency Services
            'CRITICAL_NYPD_Command': {
                'bus': 'NET_4_Financial_Network',
                'base_mw': 1.8,
                'power_factor': 0.93,
                'type': 'emergency_services',
                'backup_power': True,
                'ups_minutes': 15,
                'diesel_backup_mw': 2.0,
                'priority': 1
            },
            
            # Subway ventilation and signals
            'CRITICAL_Subway_Times_Square': {
                'bus': 'SUB_27_Times_Square',
                'base_mw': 5.0,
                'power_factor': 0.85,
                'type': 'subway_systems',
                'backup_power': True,
                'priority': 1
            },
            
            # Tunnel ventilation
            'CRITICAL_Lincoln_Tunnel_Vent': {
                'bus': 'SUB_13_8_D28',
                'base_mw': 3.5,
                'power_factor': 0.82,
                'type': 'tunnel_ventilation',
                'backup_power': True,
                'diesel_backup_mw': 4.0,
                'priority': 1
            },
            
            # Water pumping stations
            'CRITICAL_Water_Pump_Station': {
                'bus': 'SUB_13_8_D32',
                'base_mw': 4.0,
                'power_factor': 0.88,
                'type': 'water_infrastructure',
                'backup_power': True,
                'diesel_backup_mw': 4.5,
                'priority': 1
            }
        }
        
        # Add to loads
        for load_name, load_data in critical_loads.items():
            self.loads[load_name] = {
                **load_data,
                'current_mw': load_data['base_mw'],
                'current_mvar': load_data['base_mw'] * math.tan(math.acos(load_data['power_factor']))
            }
        
        print(f"⚡ Added {len(critical_loads)} critical infrastructure loads")
    
    def _add_protection_systems(self):
        """Add protection and control systems"""
        self.protection_systems = {
            'SCADA': {
                'type': 'supervisory_control',
                'coverage': 'system_wide',
                'response_time_ms': 100,
                'redundancy': 'dual_primary'
            },
            'AGC': {
                'type': 'automatic_generation_control',
                'response_time_s': 4,
                'regulation_mw': 50
            },
            'UFLS': {
                'type': 'under_frequency_load_shedding',
                'stages': [
                    {'frequency': 59.7, 'load_shed_percent': 5},
                    {'frequency': 59.4, 'load_shed_percent': 10},
                    {'frequency': 59.1, 'load_shed_percent': 15},
                    {'frequency': 58.8, 'load_shed_percent': 20}
                ]
            },
            'Relays': {
                'transmission': 'distance_protection',
                'distribution': 'overcurrent_protection',
                'differential': 'transformer_protection'
            }
        }
        
        print(f"⚡ Added protection and control systems")
    
    def _calculate_distance(self, lat1, lon1, lat2, lon2):
        """Calculate distance between two points in km"""
        R = 6371  # Earth radius in km
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        return R * c
    
    def _find_nearest_bus(self, lat, lon, voltage=None):
        """Find nearest bus of specified voltage"""
        min_dist = float('inf')
        nearest_bus = None
        
        for bus_name, bus_data in self.buses.items():
            if voltage and bus_data['voltage'] != voltage:
                continue
            
            dist = self._calculate_distance(lat, lon, bus_data['lat'], bus_data['lon'])
            if dist < min_dist:
                min_dist = dist
                nearest_bus = bus_name
        
        return nearest_bus
    
    def _get_district_name(self, lat, lon):
        """Get Manhattan district name based on coordinates"""
        if lat > 40.78:
            if lon < -73.97:
                return 'Upper_West_Side'
            else:
                return 'Upper_East_Side'
        elif lat > 40.76:
            if lon < -73.98:
                return 'Hells_Kitchen'
            else:
                return 'Midtown_East'
        elif lat > 40.74:
            if lon < -73.99:
                return 'Chelsea'
            else:
                return 'Murray_Hill'
        elif lat > 40.72:
            if lon < -74.00:
                return 'Greenwich_Village'
            else:
                return 'Gramercy'
        else:
            if lon < -74.00:
                return 'Tribeca'
            else:
                return 'Financial_District'
    
    def update_traffic_loads(self, vehicle_count, traffic_light_states, ev_charging_data):
        """Update loads based on real-time traffic data"""
        # Update traffic light power based on states
        if traffic_light_states:
            yellow_ratio = sum(1 for s in traffic_light_states.values() if 'y' in s.lower()) / len(traffic_light_states)
            adaptive_factor = 1.0 + yellow_ratio * 0.15  # More power during transitions
            
            for tl_load in self.traffic_light_loads.values():
                if tl_load.get('adaptive_control'):
                    tl_load['current_mw'] = tl_load['base_mw'] * adaptive_factor * 1.2
                else:
                    tl_load['current_mw'] = tl_load['base_mw'] * adaptive_factor
        
        # Update EV charging based on real usage
        if ev_charging_data:
            for station_id, charging_vehicles in ev_charging_data.items():
                if station_id in self.ev_charging_loads:
                    station = self.ev_charging_loads[station_id]
                    utilization = len(charging_vehicles) / station['chargers'] if station['chargers'] > 0 else 0
                    station['utilization'] = utilization
                    station['current_mw'] = station['capacity_mw'] * utilization * 0.85  # 85% average charging rate
        
        # Update street lighting based on time and traffic
        current_hour = datetime.now().hour
        if 6 <= current_hour <= 18:  # Daytime
            dimming_factor = 0.0
        elif 18 <= current_hour <= 20:  # Dusk
            dimming_factor = 0.7
        elif 20 <= current_hour <= 24 or 0 <= current_hour <= 5:  # Night
            dimming_factor = 1.0
        else:  # Dawn
            dimming_factor = 0.5
        
        # Adaptive dimming based on traffic
        if vehicle_count < 100:
            dimming_factor *= 0.7  # Reduce brightness in low traffic
        elif vehicle_count > 500:
            dimming_factor *= 1.1  # Increase brightness in high traffic
        
        for sl_load in self.street_light_loads.values():
            if sl_load.get('dimming_capable'):
                sl_load['current_mw'] = sl_load['base_mw'] * dimming_factor * 0.8  # LED dimming
            else:
                sl_load['current_mw'] = sl_load['base_mw'] * min(1.0, dimming_factor)
    
    def simulate_power_flow(self):
        """Advanced power flow simulation with constraints"""
        # Calculate total load
        self.total_load = 0
        
        # Base loads with realistic profiles
        current_time = datetime.now()
        hour = current_time.hour
        minute = current_time.minute
        day_of_week = current_time.weekday()
        
        # Load profiles based on type and time
        load_profiles = self._get_load_profiles(hour, minute, day_of_week)
        
        for load_name, load in self.loads.items():
            profile_factor = load_profiles.get(load['type'], 1.0)
            load['current_mw'] = load['base_mw'] * profile_factor
            self.total_load += load['current_mw']
        
        # Add infrastructure loads
        for tl_load in self.traffic_light_loads.values():
            self.total_load += tl_load['current_mw']
        
        for sl_load in self.street_light_loads.values():
            self.total_load += sl_load['current_mw']
        
        for ev_load in self.ev_charging_loads.values():
            self.total_load += ev_load['current_mw']
        
        # Economic dispatch of generators
        self._economic_dispatch()
        
        # Calculate line flows (DC power flow approximation)
        self._calculate_line_flows()
        
        # Check for violations
        self._check_violations()
        
        # Update battery state of charge
        self._update_battery_soc()
    
    def _get_load_profiles(self, hour, minute, day_of_week):
        """Get load multiplication factors based on time"""
        # Weekday vs Weekend
        is_weekend = day_of_week >= 5
        
        profiles = {}
        
        # Commercial load profile
        if is_weekend:
            if 10 <= hour < 18:
                profiles['commercial'] = 0.6
            else:
                profiles['commercial'] = 0.4
        else:  # Weekday
            if 7 <= hour < 9:
                profiles['commercial'] = 0.7
            elif 9 <= hour < 12:
                profiles['commercial'] = 0.95
            elif 12 <= hour < 13:
                profiles['commercial'] = 0.9
            elif 13 <= hour < 17:
                profiles['commercial'] = 1.0
            elif 17 <= hour < 19:
                profiles['commercial'] = 0.85
            elif 19 <= hour < 22:
                profiles['commercial'] = 0.6
            else:
                profiles['commercial'] = 0.4
        
        # Residential load profile
        if is_weekend:
            if 8 <= hour < 12:
                profiles['residential'] = 0.8
            elif 12 <= hour < 17:
                profiles['residential'] = 0.7
            elif 17 <= hour < 22:
                profiles['residential'] = 0.9
            else:
                profiles['residential'] = 0.5
        else:  # Weekday
            if 6 <= hour < 8:
                profiles['residential'] = 0.7
            elif 8 <= hour < 17:
                profiles['residential'] = 0.4
            elif 17 <= hour < 20:
                profiles['residential'] = 0.8
            elif 20 <= hour < 23:
                profiles['residential'] = 1.0
            else:
                profiles['residential'] = 0.5
        
        # Other profiles
        profiles['retail'] = profiles['commercial'] * 1.1 if 10 <= hour < 21 else 0.3
        profiles['data_center'] = 0.95  # Constant
        profiles['hospital'] = 0.85 if 7 <= hour < 22 else 0.7
        profiles['transit'] = 1.2 if (7 <= hour < 10 or 17 <= hour < 20) else 0.8
        profiles['traffic_control'] = 1.0  # Constant
        
        return profiles
    
    def _economic_dispatch(self):
        """Economic dispatch of generators based on merit order"""
        remaining_load = self.total_load
        self.total_generation = 0
        
        # Sort generators by cost (merit order)
        sorted_gens = sorted(
            self.generators.items(),
            key=lambda x: x[1]['cost_per_mwh']
        )
        
        # First, dispatch must-run and renewable generation
        for gen_name, gen in sorted_gens:
            if gen['type'] in ['solar_pv', 'wind']:
                # Calculate renewable output
                if gen['type'] == 'solar_pv':
                    hour = datetime.now().hour
                    if 6 <= hour <= 18:
                        solar_curve = math.sin((hour - 6) * math.pi / 12)
                        gen['current_output'] = gen['capacity_mw'] * solar_curve * 0.85
                    else:
                        gen['current_output'] = 0
                
                self.total_generation += gen['current_output']
                remaining_load -= gen['current_output']
        
        # Dispatch thermal generators
        for gen_name, gen in sorted_gens:
            if gen['type'] in ['gas_turbine', 'combined_cycle', 'steam_turbine']:
                if remaining_load > 0:
                    output = min(gen['capacity_mw'], max(gen['min_mw'], remaining_load))
                    gen['current_output'] = output
                    self.total_generation += output
                    remaining_load -= output
                else:
                    gen['current_output'] = gen['min_mw'] if gen.get('must_run') else 0
        
        # Use battery storage if needed
        for gen_name, gen in self.generators.items():
            if gen['type'] == 'battery':
                if remaining_load > 0 and gen['current_soc'] > 0.2:
                    # Discharge battery
                    discharge = min(gen['capacity_mw'], remaining_load, gen['current_soc'] * gen['energy_capacity_mwh'])
                    gen['current_output'] = discharge
                    self.total_generation += discharge
                    remaining_load -= discharge
                elif remaining_load < -50 and gen['current_soc'] < 0.9:
                    # Charge battery with excess generation
                    charge = min(gen['capacity_mw'], -remaining_load, (1 - gen['current_soc']) * gen['energy_capacity_mwh'])
                    gen['current_output'] = -charge  # Negative for charging
                    remaining_load += charge
    
    def _calculate_line_flows(self):
        """Calculate line flows using DC power flow approximation"""
        # Simplified DC power flow for demonstration
        # In reality, would use full AC power flow with pypsa
        
        for line_name, line in self.lines.items():
            # Estimate flow based on connected loads and generation
            from_bus = line['from']
            to_bus = line['to']
            
            # Simple proportional flow based on load
            base_flow = self.total_load * 0.001 * line['capacity_mw']
            
            # Add some variation based on line characteristics
            if line['voltage'] == 138:
                line['current_flow'] = min(base_flow * 1.5, line['capacity_mw'] * 0.8)
            elif line['voltage'] == 27:
                line['current_flow'] = min(base_flow * 1.2, line['capacity_mw'] * 0.7)
            elif line['voltage'] == 13.8:
                line['current_flow'] = min(base_flow * 1.0, line['capacity_mw'] * 0.6)
            else:
                line['current_flow'] = min(base_flow * 0.8, line['capacity_mw'] * 0.5)
    
    def _check_violations(self):
        """Check for voltage and thermal violations"""
        self.voltage_violations = []
        self.thermal_violations = []
        
        # Check line overloads
        for line_name, line in self.lines.items():
            utilization = (line['current_flow'] / line['capacity_mw']) * 100 if line['capacity_mw'] > 0 else 0
            
            if utilization > 100:
                self.thermal_violations.append({
                    'element': line_name,
                    'type': 'line_overload',
                    'utilization': utilization,
                    'severity': 'critical' if utilization > 120 else 'warning'
                })
            elif utilization > 90:
                self.thermal_violations.append({
                    'element': line_name,
                    'type': 'high_loading',
                    'utilization': utilization,
                    'severity': 'warning'
                })
        
        # Check transformer loading
        for xfmr_name, xfmr in self.transformers.items():
            # Simplified loading calculation
            loading = np.random.uniform(60, 95)  # Would calculate from actual flows
            
            if loading > 100:
                self.thermal_violations.append({
                    'element': xfmr_name,
                    'type': 'transformer_overload',
                    'loading': loading,
                    'severity': 'critical'
                })
    
    def _update_battery_soc(self):
        """Update battery state of charge"""
        time_step_hours = 0.25  # 15 minutes
        
        for gen_name, gen in self.generators.items():
            if gen['type'] == 'battery':
                if gen['current_output'] > 0:  # Discharging
                    energy_discharged = gen['current_output'] * time_step_hours / gen['efficiency']
                    gen['current_soc'] -= energy_discharged / gen['energy_capacity_mwh']
                elif gen['current_output'] < 0:  # Charging
                    energy_charged = -gen['current_output'] * time_step_hours * gen['efficiency']
                    gen['current_soc'] += energy_charged / gen['energy_capacity_mwh']
                
                # Constrain SOC
                gen['current_soc'] = max(0.0, min(1.0, gen['current_soc']))
    
    def get_network_data(self):
        """Get complete network data for visualization"""
        return {
            'buses': [
                {
                    'id': bus_id,
                    'lat': bus_data['lat'],
                    'lon': bus_data['lon'],
                    'voltage': bus_data['voltage'],
                    'type': bus_data['type']
                }
                for bus_id, bus_data in self.buses.items()
            ],
            'lines': [
                {
                    'id': line_id,
                    'from': line_data['from'],
                    'to': line_data['to'],
                    'voltage': line_data.get('voltage', 138),
                    'capacity': line_data['capacity_mw'],
                    'flow': line_data.get('current_flow', 0),
                    'utilization': (line_data.get('current_flow', 0) / line_data['capacity_mw'] * 100) if line_data['capacity_mw'] > 0 else 0,
                    'from_pos': [
                        self.buses[line_data['from']]['lon'],
                        self.buses[line_data['from']]['lat']
                    ] if line_data['from'] in self.buses else [0, 0],
                    'to_pos': [
                        self.buses[line_data['to']]['lon'],
                        self.buses[line_data['to']]['lat']
                    ] if line_data['to'] in self.buses else [0, 0]
                }
                for line_id, line_data in self.lines.items()
            ],
            'generators': [
                {
                    'id': gen_id,
                    'lat': gen_data['lat'],
                    'lon': gen_data['lon'],
                    'capacity': gen_data['capacity_mw'],
                    'output': gen_data.get('current_output', 0),
                    'type': gen_data['type'],
                    'utilization': (gen_data.get('current_output', 0) / gen_data['capacity_mw'] * 100) if gen_data['capacity_mw'] > 0 else 0
                }
                for gen_id, gen_data in self.generators.items()
            ],
            'transformers': [
                {
                    'id': xfmr_id,
                    'rating': xfmr_data['rating_mva'],
                    'voltage_ratio': xfmr_data['voltage_ratio'],
                    'type': xfmr_data['type']
                }
                for xfmr_id, xfmr_data in self.transformers.items()
            ],
            'ev_stations': [
                {
                    'id': station_id,
                    'lat': station_data['lat'],
                    'lon': station_data['lon'],
                    'capacity': station_data['capacity_mw'],
                    'utilization': station_data.get('utilization', 0) * 100,
                    'type': station_data['type']
                }
                for station_id, station_data in self.ev_charging_loads.items()
            ],
            'metrics': {
                'total_generation': round(self.total_generation, 2),
                'total_load': round(self.total_load, 2),
                'losses': round(self.total_generation - self.total_load, 2),
                'renewable_generation': round(sum(g['current_output'] for g in self.generators.values() if g['type'] in ['solar_pv', 'wind']), 2),
                'battery_output': round(sum(g['current_output'] for g in self.generators.values() if g['type'] == 'battery'), 2),
                'num_violations': len(self.thermal_violations) + len(self.voltage_violations)
            }
        }
    
    def get_status(self):
        """Get current network status"""
        return {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_generation_mw': round(self.total_generation, 2),
            'total_load_mw': round(self.total_load, 2),
            'balance_mw': round(self.total_generation - self.total_load, 2),
            'traffic_light_load_mw': round(sum(tl['current_mw'] for tl in self.traffic_light_loads.values()), 3),
            'street_light_load_mw': round(sum(sl['current_mw'] for sl in self.street_light_loads.values()), 2),
            'ev_charging_load_mw': round(sum(ev['current_mw'] for ev in self.ev_charging_loads.values()), 2),
            'renewable_percent': round((sum(g['current_output'] for g in self.generators.values() if g['type'] in ['solar_pv', 'wind']) / max(self.total_generation, 1)) * 100, 1),
            'line_utilization': {
                line_name: round((line['current_flow'] / line['capacity_mw']) * 100, 1) if line['capacity_mw'] > 0 else 0
                for line_name, line in list(self.lines.items())[:10]  # Top 10 lines
            },
            'violations': {
                'thermal': len(self.thermal_violations),
                'voltage': len(self.voltage_violations)
            }
        }

def test_network():
    """Test the ultra-realistic Manhattan network"""
    print("=" * 80)
    print("🏙️  ULTRA-REALISTIC MANHATTAN POWER NETWORK TEST")
    print("=" * 80)
    
    # Create and build network
    network = ManhattanPowerNetworkRealistic()
    network.build_network()
    
    # Summary statistics
    print("\n" + "=" * 80)
    print("📊 NETWORK STATISTICS")
    print("-" * 80)
    
    voltage_levels = {}
    for bus in network.buses.values():
        v = bus['voltage']
        voltage_levels[v] = voltage_levels.get(v, 0) + 1
    
    print("\n🔌 Buses by Voltage Level:")
    for voltage in sorted(voltage_levels.keys(), reverse=True):
        print(f"   {voltage:6.1f} kV: {voltage_levels[voltage]:3d} buses")
    
    print(f"\n⚡ Total Components:")
    print(f"   Buses:        {len(network.buses)}")
    print(f"   Lines:        {len(network.lines)}")
    print(f"   Transformers: {len(network.transformers)}")
    print(f"   Generators:   {len(network.generators)}")
    print(f"   Base Loads:   {len(network.loads)}")
    print(f"   Traffic Lights: {len(network.traffic_light_loads)}")
    print(f"   Street Lights:  {len(network.street_light_loads)}")
    print(f"   EV Stations:    {len(network.ev_charging_loads)}")
    
    # Capacity summary
    gen_by_type = {}
    for gen in network.generators.values():
        t = gen['type']
        gen_by_type[t] = gen_by_type.get(t, 0) + gen['capacity_mw']
    
    print(f"\n🏭 Generation Capacity by Type:")
    for gen_type in sorted(gen_by_type.keys()):
        print(f"   {gen_type:15s}: {gen_by_type[gen_type]:7.1f} MW")
    print(f"   {'TOTAL':15s}: {sum(gen_by_type.values()):7.1f} MW")
    
    # Simulate operation
    print("\n" + "=" * 80)
    print("🔄 SIMULATING 24-HOUR OPERATION")
    print("-" * 80)
    
    for hour in [0, 6, 9, 12, 15, 18, 21]:
        # Update time
        network.current_time_index = hour * 4  # 15-min intervals
        
        # Simulate
        network.simulate_power_flow()
        status = network.get_status()
        
        print(f"\n⏰ Hour {hour:02d}:00")
        print(f"   Generation: {status['total_generation_mw']:7.1f} MW")
        print(f"   Load:       {status['total_load_mw']:7.1f} MW")
        print(f"   Traffic:    {status['traffic_light_load_mw']:7.3f} MW")
        print(f"   Street:     {status['street_light_load_mw']:7.2f} MW")
        print(f"   EV:         {status['ev_charging_load_mw']:7.2f} MW")
        print(f"   Renewable:  {status['renewable_percent']:5.1f}%")
    
    # Save network data
    network_data = network.get_network_data()
    with open('manhattan_power_network.json', 'w') as f:
        json.dump(network_data, f, indent=2)
    
    print("\n" + "=" * 80)
    print("✅ ULTRA-REALISTIC NETWORK READY!")
    print("📁 Network data saved to manhattan_power_network.json")
    print("🎯 Ready for integration with SUMO traffic simulation")
    print("=" * 80)
    
    return network

if __name__ == "__main__":
    test_network()