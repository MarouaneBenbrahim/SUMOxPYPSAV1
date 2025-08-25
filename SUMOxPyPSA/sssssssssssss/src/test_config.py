import json
import os
from pathlib import Path

def test_configuration():
    """Test if configuration is properly set up"""
    
    print("\n" + "="*60)
    print("TESTING CONFIGURATION SETUP")
    print("="*60)
    
    # Test 1: Config file exists and is valid JSON
    config_path = Path("configs/config.json")
    if config_path.exists():
        print("✅ Config file found")
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            print("✅ Config is valid JSON")
            print(f"   - Simulation name: {config['simulation']['name']}")
            print(f"   - Area: {config['simulation']['area']['description']}")
            print(f"   - Duration: {config['simulation']['duration_seconds']}s")
        except json.JSONDecodeError as e:
            print(f"❌ Config JSON error: {e}")
            return False
    else:
        print("❌ Config file not found")
        return False
    
    # Test 2: Check all required sections exist
    required_sections = ['simulation', 'sumo', 'pypsa', 'vehicles', 
                         'ev_charging', 'scenarios', 'coupling', 'visualization']
    
    missing = []
    for section in required_sections:
        if section in config:
            print(f"✅ Section '{section}' found")
        else:
            missing.append(section)
            print(f"❌ Section '{section}' missing")
    
    if missing:
        return False
    
    # Test 3: Environment file
    if Path(".env").exists():
        print("✅ Environment file found")
    else:
        print("⚠️  Environment file not found (optional)")
    
    # Test 4: Directory structure
    dirs_to_check = ['data', 'configs', 'outputs', 'src', 'static', 'templates']
    all_dirs_exist = True
    for dir_name in dirs_to_check:
        if Path(dir_name).exists():
            print(f"✅ Directory '{dir_name}' exists")
        else:
            print(f"❌ Directory '{dir_name}' missing")
            all_dirs_exist = False
    
    print("\n" + "="*60)
    if not missing and all_dirs_exist:
        print("✅ CONFIGURATION TEST PASSED!")
        print("\nKey Settings:")
        print(f"  📍 Area: {config['simulation']['area']['description']}")
        print(f"  🚗 Total vehicles: {config['vehicles']['total_vehicles']}")
        print(f"  ⚡ EV percentage: {config['vehicles']['ev_percentage']*100}%")
        print(f"  🕐 Simulation duration: {config['simulation']['duration_seconds']/60} minutes")
        print(f"  📊 Scenarios defined: {len(config['scenarios'])}")
    else:
        print("❌ CONFIGURATION TEST FAILED - Fix issues above")
    print("="*60)
    
    return not missing and all_dirs_exist

if __name__ == "__main__":
    test_configuration()