#!/usr/bin/env python3
"""
generate_poc_data.py

Runs a 2-minute simulation to capture metrics for the PoC.
Captures:
1. Normal flow (30s)
2. Outage (30s)
3. Recovery (60s)
Exports to poc_timeline.csv
"""

import subprocess
import time
import os
import json
import requests
import pandas as pd

# Configuration
COLLECTOR_CMD = ["python3", "collector_server.py"]
AGENT_CMD = ["python3", "edge_agent.py"]
HEALTH_URL = "http://localhost:8080/health"

def collect_metrics(step_name, duration_sec):
    data = []
    start_time = time.time()
    print(f"  Collecting for {step_name} ({duration_sec}s)...")
    while (time.time() - start_time) < duration_sec:
        try:
            health = requests.get(HEALTH_URL, timeout=1).json()
            data.append({
                "timestamp": time.time(),
                "step": step_name,
                "buffer_count": health['checks']['disk_buffer_count'],
                "breaker_state": health['checks']['circuit_breaker_state'],
                "msgs_processed": health['metrics']['messages_processed'],
                "bytes_in": health['metrics']['bytes_in'],
                "bytes_out": health['metrics']['bytes_out'],
                "ratio": health['metrics']['compression_ratio']
            })
        except:
            pass
        time.sleep(2)
    return data

def run_simulation():
    print("=== POC Data Generation Simulation ===")
    
    # 1. Reset state
    if os.path.exists("./buffer.db"): os.remove("./buffer.db")
    if os.path.exists("./metrics.csv"): os.remove("./metrics.csv")
    subprocess.run(["python3", "setup_demo_dicts.py"], check=True)
    
    # 2. Start collector and agent
    collector_proc = subprocess.Popen(COLLECTOR_CMD)
    time.sleep(3)
    agent_proc = subprocess.Popen(AGENT_CMD)
    time.sleep(5)
    
    all_data = []
    
    try:
        # Step 1: Normal Flow
        all_data.extend(collect_metrics("Normal", 30))
        
        # Step 2: Outage
        print("\n  !!! SIMULATING OUTAGE !!!")
        collector_proc.terminate()
        collector_proc.wait()
        all_data.extend(collect_metrics("Outage", 30))
        
        # Step 3: Recovery
        print("\n  !!! SIMULATING RECOVERY !!!")
        collector_proc = subprocess.Popen(COLLECTOR_CMD)
        all_data.extend(collect_metrics("Recovery", 60))
        
    finally:
        agent_proc.terminate()
        collector_proc.terminate()
        
    # Export to CSV
    df = pd.DataFrame(all_data)
    df.to_csv("poc_timeline.csv", index=False)
    print("\nSimulation complete. Data saved to poc_timeline.csv")

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    run_simulation()
