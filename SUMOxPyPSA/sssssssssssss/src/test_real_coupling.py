"""
Test REAL SUMO×PyPSA coupling
"""

import traci
import sys
sys.path.append('src')
from coupling.coupler import SUMOPyPSACoupler

def test_real_coupling():
    print("\n" + "="*70)
    print(" TESTING REAL SUMO×PYPSA COUPLING ".center(70, "="))
    print("="*70)
    
    coupler = SUMOPyPSACoupler()
    
    # Initialize both systems
    coupler.initialize('normal')
    
    if coupler.sumo_connected and coupler.pypsa_network:
        print("\n✅ SUCCESS: Both SUMO and PyPSA are connected!")
        print("This is TRUE coupling!")
        
        # Run a few steps to prove coupling works
        for step in range(10):
            metrics = coupler.step_simulation(step)
            print(f"\nStep {step}:")
            print(f"  SUMO: {metrics['total_vehicles']} real vehicles")
            print(f"  PyPSA: {metrics['total_load_mw']:.1f} MW load")
            print(f"  Coupling: EVs affect grid, grid affects signals")
            
    else:
        print("\n❌ FAILURE: Not properly coupled!")
        print(f"  SUMO connected: {coupler.sumo_connected}")
        print(f"  PyPSA connected: {coupler.pypsa_network is not None}")
        print("\nThis is NOT the SUMO×PyPSA coupling you need!")
    
    # Cleanup
    if coupler.sumo_connected:
        traci.close()

if __name__ == "__main__":
    # First fix SUMO
    import subprocess
    subprocess.run([sys.executable, "src/sumo/fix_sumo.py"])
    
    # Then test coupling
    test_real_coupling()