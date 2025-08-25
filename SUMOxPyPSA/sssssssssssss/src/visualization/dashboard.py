"""
Real-time Visualization Dashboard for Manhattan Coupled Simulation
Web-based dashboard using Plotly Dash for interactive visualization
"""

import os
import sys
import json
import pandas as pd
import numpy as np
from pathlib import Path
import dash
from dash import dcc, html, Input, Output, State
import plotly.graph_objs as go
import plotly.express as px
from datetime import datetime
import threading
import queue
import time

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from coupling.coupler import SUMOPyPSACoupler

class ManhattanDashboard:
    def __init__(self):
        """Initialize the dashboard"""
        print("\n" + "="*70)
        print(" MANHATTAN SIMULATION DASHBOARD ".center(70, "="))
        print("="*70)
        print("\n🎮 Initializing interactive dashboard...")
        
        # Load configuration
        with open('configs/config.json', 'r') as f:
            self.config = json.load(f)
        
        # Initialize Dash app
        self.app = dash.Dash(__name__, suppress_callback_exceptions=True)
        self.app.title = "Manhattan Coupled Simulation"
        
        # Initialize coupler
        self.coupler = SUMOPyPSACoupler()
        
        # Simulation state
        self.simulation_running = False
        self.simulation_thread = None
        self.current_scenario = 'normal'
        self.simulation_time = 0
        
        # Data storage for visualization
        self.metrics_history = {
            'time': [],
            'vehicles': [],
            'speed': [],
            'load': [],
            'evs_charging': []
        }
        
        # Load infrastructure data
        self.load_infrastructure_data()
        
        # Create layout
        self.create_layout()
        
        # Setup callbacks
        self.setup_callbacks()
        
        print("✅ Dashboard initialized!")
    
    def load_infrastructure_data(self):
        """Load infrastructure data for visualization"""
        self.substations = pd.read_csv('data/power/substations.csv')
        self.ev_stations = pd.read_csv('data/power/ev_stations.csv')
        self.buildings = pd.read_csv('data/power/buildings.csv')
        self.signals = pd.read_csv('data/traffic/signals.csv')
    
    def create_layout(self):
        """Create the dashboard layout"""
        self.app.layout = html.Div([
            # Header
            html.Div([
                html.H1("🏙️ Manhattan Coupled Infrastructure Simulation",
                       style={'textAlign': 'center', 'color': '#00ff88'}),
                html.P("Real-time Traffic-Power Grid Coupling | Midtown Manhattan",
                      style={'textAlign': 'center', 'color': '#888'})
            ], style={'backgroundColor': '#1a1a2e', 'padding': '20px'}),
            
            # Control Panel
            html.Div([
                html.Div([
                    html.H3("⚙️ Simulation Control", style={'color': '#00ccff'}),
                    
                    # Scenario selector
                    html.Label("Scenario:", style={'color': '#fff'}),
                    dcc.Dropdown(
                        id='scenario-selector',
                        options=[
                            {'label': '☀️ Normal Day', 'value': 'normal'},
                            {'label': '🚗 Rush Hour', 'value': 'rush_hour'},
                            {'label': '🌙 Night', 'value': 'night'},
                            {'label': '⚡ Power Outage', 'value': 'blackout'}
                        ],
                        value='normal',
                        style={'backgroundColor': '#333', 'color': '#000'}
                    ),
                    
                    html.Br(),
                    
                    # Control buttons
                    html.Button('▶️ START', id='start-button', 
                               style={'backgroundColor': '#00ff88', 'color': '#000',
                                     'padding': '10px 20px', 'margin': '5px'}),
                    html.Button('⏸️ PAUSE', id='pause-button',
                               style={'backgroundColor': '#ffaa00', 'color': '#000',
                                     'padding': '10px 20px', 'margin': '5px'}),
                    html.Button('⏹️ STOP', id='stop-button',
                               style={'backgroundColor': '#ff4444', 'color': '#fff',
                                     'padding': '10px 20px', 'margin': '5px'}),
                    
                    html.Br(),
                    html.Br(),
                    
                    # Speed control
                    html.Label("Simulation Speed:", style={'color': '#fff'}),
                    dcc.Slider(
                        id='speed-slider',
                        min=1,
                        max=100,
                        value=10,
                        marks={1: '1x', 10: '10x', 50: '50x', 100: '100x'},
                        tooltip={"placement": "bottom", "always_visible": True}
                    ),
                    
                    html.Br(),
                    
                    # Disruption injection
                    html.H4("💥 Inject Disruption", style={'color': '#ffaa00'}),
                    html.Button('🔌 Substation Failure', id='inject-power-failure',
                               style={'backgroundColor': '#ff6666', 'color': '#fff',
                                     'padding': '8px 15px', 'margin': '3px'}),
                    html.Button('🚗 Traffic Incident', id='inject-traffic-incident',
                               style={'backgroundColor': '#ff9944', 'color': '#fff',
                                     'padding': '8px 15px', 'margin': '3px'}),
                    html.Button('🔋 EV Surge', id='inject-ev-surge',
                               style={'backgroundColor': '#44aaff', 'color': '#fff',
                                     'padding': '8px 15px', 'margin': '3px'})
                    
                ], style={'backgroundColor': '#2a2a3e', 'padding': '20px', 'borderRadius': '10px'})
            ], style={'width': '25%', 'float': 'left', 'padding': '10px'}),
            
            # Main Visualization Area
            html.Div([
                # Map View
                html.Div([
                    dcc.Graph(id='main-map', style={'height': '500px'})
                ], style={'backgroundColor': '#1a1a2e', 'padding': '10px', 
                         'borderRadius': '10px', 'marginBottom': '10px'}),
                
                # Metrics Row
                html.Div([
                    # Traffic Metrics
                    html.Div([
                        html.H4("🚗 Traffic", style={'color': '#00ff88', 'textAlign': 'center'}),
                        html.Div(id='vehicle-count', children='0 vehicles',
                                style={'fontSize': '24px', 'textAlign': 'center', 'color': '#fff'}),
                        html.Div(id='avg-speed', children='0 km/h',
                                style={'fontSize': '18px', 'textAlign': 'center', 'color': '#aaa'})
                    ], style={'width': '24%', 'display': 'inline-block',
                             'backgroundColor': '#2a2a3e', 'padding': '15px',
                             'borderRadius': '10px', 'margin': '0.5%'}),
                    
                    # Power Metrics
                    html.Div([
                        html.H4("⚡ Power", style={'color': '#ffaa00', 'textAlign': 'center'}),
                        html.Div(id='power-load', children='0 MW',
                                style={'fontSize': '24px', 'textAlign': 'center', 'color': '#fff'}),
                        html.Div(id='load-factor', children='0%',
                                style={'fontSize': '18px', 'textAlign': 'center', 'color': '#aaa'})
                    ], style={'width': '24%', 'display': 'inline-block',
                             'backgroundColor': '#2a2a3e', 'padding': '15px',
                             'borderRadius': '10px', 'margin': '0.5%'}),
                    
                    # EV Metrics
                    html.Div([
                        html.H4("🔋 EVs", style={'color': '#00ccff', 'textAlign': 'center'}),
                        html.Div(id='evs-charging', children='0 charging',
                                style={'fontSize': '24px', 'textAlign': 'center', 'color': '#fff'}),
                        html.Div(id='ev-demand', children='0 MW',
                                style={'fontSize': '18px', 'textAlign': 'center', 'color': '#aaa'})
                    ], style={'width': '24%', 'display': 'inline-block',
                             'backgroundColor': '#2a2a3e', 'padding': '15px',
                             'borderRadius': '10px', 'margin': '0.5%'}),
                    
                    # System Status
                    html.Div([
                        html.H4("🚦 Status", style={'color': '#ff4444', 'textAlign': 'center'}),
                        html.Div(id='system-status', children='Ready',
                                style={'fontSize': '24px', 'textAlign': 'center', 'color': '#0f0'}),
                        html.Div(id='failures', children='No failures',
                                style={'fontSize': '18px', 'textAlign': 'center', 'color': '#aaa'})
                    ], style={'width': '24%', 'display': 'inline-block',
                             'backgroundColor': '#2a2a3e', 'padding': '15px',
                             'borderRadius': '10px', 'margin': '0.5%'})
                ])
            ], style={'width': '50%', 'float': 'left', 'padding': '10px'}),
            
            # Charts Panel
            html.Div([
                # Time series chart
                html.Div([
                    dcc.Graph(id='time-series-chart', style={'height': '300px'})
                ], style={'backgroundColor': '#2a2a3e', 'padding': '10px',
                         'borderRadius': '10px', 'marginBottom': '10px'}),
                
                # Power flow chart
                html.Div([
                    dcc.Graph(id='power-flow-chart', style={'height': '300px'})
                ], style={'backgroundColor': '#2a2a3e', 'padding': '10px',
                         'borderRadius': '10px'})
            ], style={'width': '25%', 'float': 'right', 'padding': '10px'}),
            
            # Hidden interval component for updates
            dcc.Interval(
                id='interval-component',
                interval=1000,  # Update every second
                n_intervals=0
            ),
            
            # Hidden div to store simulation state
            html.Div(id='simulation-state', style={'display': 'none'})
            
        ], style={'backgroundColor': '#0a0a0a', 'minHeight': '100vh', 'fontFamily': 'Arial'})
    
    def create_map_figure(self, metrics=None):
        """Create the main map visualization"""
        fig = go.Figure()
        
        # Add substations
        fig.add_trace(go.Scattermapbox(
            mode='markers+text',
            lon=self.substations['lon'],
            lat=self.substations['lat'],
            marker={'size': 15, 'color': 'yellow'},
            text=self.substations['name'],
            name='Substations'
        ))
        
        # Add EV stations
        fig.add_trace(go.Scattermapbox(
            mode='markers',
            lon=self.ev_stations['lon'],
            lat=self.ev_stations['lat'],
            marker={'size': 10, 'color': 'green'},
            text=self.ev_stations['name'],
            name='EV Stations'
        ))
        
        # Add traffic signals (sample)
        signal_sample = self.signals.sample(min(20, len(self.signals)))
        fig.add_trace(go.Scattermapbox(
            mode='markers',
            lon=signal_sample['lon'],
            lat=signal_sample['lat'],
            marker={'size': 5, 'color': 'red'},
            name='Traffic Signals'
        ))
        
        # Map layout
        fig.update_layout(
            mapbox=dict(
                accesstoken='pk.eyJ1IjoicGxvdGx5bWFwYm94IiwiYSI6ImNrOWJqb2F4djBnMjEzbG50amg0dnJieG4ifQ.Zme1-Uzoi75IaFbieBDl3A',
                bearing=0,
                center=dict(
                    lat=self.config['simulation']['area']['south'] + 
                        (self.config['simulation']['area']['north'] - 
                         self.config['simulation']['area']['south']) / 2,
                    lon=self.config['simulation']['area']['west'] + 
                        (self.config['simulation']['area']['east'] - 
                         self.config['simulation']['area']['west']) / 2
                ),
                pitch=45,
                zoom=14,
                style='dark'
            ),
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor='rgba(0,0,0,0.5)',
                font=dict(color='white')
            )
        )
        
        return fig
    
    def create_time_series_figure(self):
        """Create time series chart"""
        fig = go.Figure()
        
        # Add traces
        fig.add_trace(go.Scatter(
            x=self.metrics_history['time'],
            y=self.metrics_history['vehicles'],
            name='Vehicles',
            line=dict(color='#00ff88', width=2)
        ))
        
        fig.add_trace(go.Scatter(
            x=self.metrics_history['time'],
            y=self.metrics_history['load'],
            name='Load (MW)',
            line=dict(color='#ffaa00', width=2),
            yaxis='y2'
        ))
        
        fig.update_layout(
            title='System Metrics Over Time',
            xaxis=dict(title='Time (s)', color='white', gridcolor='#333'),
            yaxis=dict(title='Vehicles', color='#00ff88', gridcolor='#333'),
            yaxis2=dict(title='Load (MW)', color='#ffaa00', overlaying='y', side='right'),
            plot_bgcolor='#1a1a2e',
            paper_bgcolor='#2a2a3e',
            font=dict(color='white'),
            showlegend=True,
            legend=dict(x=0, y=1, bgcolor='rgba(0,0,0,0.5)')
        )
        
        return fig
    
    def create_power_flow_figure(self):
        """Create power flow visualization"""
        # Simple bar chart of loads
        loads = ['Buildings', 'EV Charging', 'Traffic Signals', 'Other']
        values = [80, 10, 2, 8]
        
        fig = go.Figure(data=[
            go.Bar(x=loads, y=values, 
                  marker_color=['#4CAF50', '#2196F3', '#FF9800', '#9C27B0'])
        ])
        
        fig.update_layout(
            title='Power Distribution by Category',
            xaxis=dict(color='white', gridcolor='#333'),
            yaxis=dict(title='Load (MW)', color='white', gridcolor='#333'),
            plot_bgcolor='#1a1a2e',
            paper_bgcolor='#2a2a3e',
            font=dict(color='white')
        )
        
        return fig
    
    def setup_callbacks(self):
        """Setup Dash callbacks"""
        
        @self.app.callback(
            [Output('main-map', 'figure'),
             Output('time-series-chart', 'figure'),
             Output('power-flow-chart', 'figure'),
             Output('vehicle-count', 'children'),
             Output('avg-speed', 'children'),
             Output('power-load', 'children'),
             Output('load-factor', 'children'),
             Output('evs-charging', 'children'),
             Output('ev-demand', 'children'),
             Output('system-status', 'children'),
             Output('failures', 'children')],
            [Input('interval-component', 'n_intervals'),
             Input('start-button', 'n_clicks'),
             Input('stop-button', 'n_clicks')],
            [State('scenario-selector', 'value')]
        )
        def update_dashboard(n_intervals, start_clicks, stop_clicks, scenario):
            """Update all dashboard components"""
            
            # Get current metrics
            if hasattr(self.coupler, 'metrics'):
                metrics = self.coupler.metrics
                
                # Update history
                if len(self.metrics_history['time']) > 100:
                    # Keep only last 100 points
                    for key in self.metrics_history:
                        self.metrics_history[key] = self.metrics_history[key][-100:]
                
                self.metrics_history['time'].append(metrics.get('simulation_time', 0))
                self.metrics_history['vehicles'].append(metrics.get('total_vehicles', 0))
                self.metrics_history['speed'].append(metrics.get('avg_speed_kmh', 0))
                self.metrics_history['load'].append(metrics.get('total_load_mw', 0))
                self.metrics_history['evs_charging'].append(metrics.get('evs_charging', 0))
            else:
                metrics = {
                    'total_vehicles': 0,
                    'avg_speed_kmh': 0,
                    'total_load_mw': 0,
                    'load_factor': 0,
                    'evs_charging': 0,
                    'blackout_areas': 0,
                    'signals_affected': 0
                }
            
            # Create figures
            map_fig = self.create_map_figure(metrics)
            ts_fig = self.create_time_series_figure()
            pf_fig = self.create_power_flow_figure()
            
            # Format metrics
            vehicle_text = f"{metrics['total_vehicles']} vehicles"
            speed_text = f"{metrics['avg_speed_kmh']:.1f} km/h"
            load_text = f"{metrics['total_load_mw']:.1f} MW"
            load_factor_text = f"{metrics['load_factor']*100:.1f}%" if metrics['load_factor'] else "0%"
            evs_text = f"{metrics['evs_charging']} charging"
            ev_demand_text = f"{metrics['evs_charging'] * 0.05:.1f} MW"
            
            # System status
            if metrics['blackout_areas'] > 0:
                status_text = "⚠️ BLACKOUT"
                status_style = {'fontSize': '24px', 'textAlign': 'center', 'color': '#f00'}
            elif metrics['signals_affected'] > 0:
                status_text = "⚠️ Degraded"
                status_style = {'fontSize': '24px', 'textAlign': 'center', 'color': '#fa0'}
            else:
                status_text = "✅ Normal"
                status_style = {'fontSize': '24px', 'textAlign': 'center', 'color': '#0f0'}
            
            failures_text = f"{metrics['signals_affected']} signals affected" if metrics['signals_affected'] else "No failures"
            
            return (map_fig, ts_fig, pf_fig, vehicle_text, speed_text, 
                   load_text, load_factor_text, evs_text, ev_demand_text, 
                   status_text, failures_text)
    
    def run(self):
        """Run the dashboard"""
        print("\n" + "="*70)
        print("🚀 Starting Dashboard Server...")
        print("="*70)
        print(f"\n📡 Dashboard URL: http://localhost:{self.config['visualization']['visualization_port']}")
        print("📊 Open this URL in your browser to view the dashboard")
        print("\nPress Ctrl+C to stop the server")
        print("-"*70)
        
        self.app.run_server(
            debug=False,
            host='127.0.0.1',
            port=self.config['visualization']['visualization_port']
        )


def main():
    """Main entry point"""
    dashboard = ManhattanDashboard()
    dashboard.run()


if __name__ == "__main__":
    main()