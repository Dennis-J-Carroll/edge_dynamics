#!/usr/bin/env python3
"""
demo_resilience.py

Automated demonstration of the Edge Dynamics Agent's ideal use-case:
High-frequency vehicle telemetry with intermittent network connectivity.

This demo shows:
1. High compression via per-topic dictionaries.
2. Fault tolerance via the Circuit Breaker.
3. Persistence via the SQLite Disk Buffer (Zero-Loss).
4. Automatic recovery (Store-and-Forward).
"""

import subprocess
import time
import os
import json
import requests
import signal

# Configuration
COLLECTOR_CMD = ["python3", "collector_server.py"]
AGENT_CMD = ["python3", "edge_agent.py"]
HEALTH_URL = "http://localhost:8080/health"

def run_demo():
    print("=== Edge Dynamics Resilience Demo ===")
    
    # 1. Setup Dictionaries
    print("\n[1/6] Setting up dictionaries...")
    subprocess.run(["python3", "setup_demo_dicts.py"], check=True)
    
    # 2. Start Collector
    print("\n[2/6] Starting Collector Server...")
    collector_proc = subprocess.Popen(COLLECTOR_CMD)
    time.sleep(3) # Give collector time to bind
    
    # 3. Start Agent
    print("\n[3/6] Starting Edge Agent...")
    agent_proc = subprocess.Popen(AGENT_CMD)
    time.sleep(5) # Give agent time to initialize and start threads
    
    try:
        # 4. Verify Normal Operation
        print("\n[4/6] Verifying normal flow (Compression active)...")
        # Retry logic for health check
        health = None
        for _ in range(5):
            try:
                health = requests.get(HEALTH_URL, timeout=2).json()
                break
            except:
                time.sleep(2)
        
        if not health:
            print("  ERROR: Could not connect to Agent Health API")
            return

        print(f"  Current Stats: {health['metrics']}")
        print(f"  Circuit Breaker: {health['checks']['circuit_breaker_state']}")
        
        # 5. Simulate Outage
        print("\n[5/6] SIMULATING NETWORK OUTAGE (Stopping Collector)...")
        collector_proc.terminate()
        collector_proc.wait()
        
        print("  Generating data during outage (Buffering to SQLite)...")
        time.sleep(10) # Let it try to flush and fail
        
        health = requests.get(HEALTH_URL).json()
        print(f"  Disk Buffer Count: {health['checks']['disk_buffer_count']} batches")
        print(f"  Circuit Breaker: {health['checks']['circuit_breaker_state']}")
        
        # 6. Recovery
        print("\n[6/6] SIMULATING NETWORK RECOVERY (Restarting Collector)...")
        collector_proc = subprocess.Popen(COLLECTOR_CMD) # Restart
        print("  Waiting for 'Store-and-Forward' recovery loop (10s interval)...")
        
        # Wait for recovery loop to kick in and drain buffer
        # Giving it more time to ensure breaker resets and loop triggers
        for i in range(6):
            time.sleep(10)
            health = requests.get(HEALTH_URL).json()
            count = health['checks']['disk_buffer_count']
            state = health['checks']['circuit_breaker_state']
            print(f"  Attempt {i+1}: Remaining in Buffer: {count} batches, Breaker: {state}")
            if count == 0:
                print("\nSUCCESS: All buffered data successfully delivered!")
                break
        
    finally:
        print("\n=== Demo Complete. Cleaning up... ===")
        agent_proc.terminate()
        collector_proc.terminate()
        # Clean up temporary DB
        if os.path.exists("./buffer.db"):
            os.remove("./buffer.db")

if __name__ == "__main__":
    # Ensure we are in the right directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    run_demo()
