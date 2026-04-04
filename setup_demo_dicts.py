#!/usr/bin/env python3
import zstandard as zstd
import json
import os

def create_sample_dicts():
    dict_dir = "./dicts"
    os.makedirs(dict_dir, exist_ok=True)
    
    topics = ["vehicle.telemetry", "vehicle.bounding_boxes", "sensor.engine"]
    index = {}
    
    for topic in topics:
        # Generate some sample data for training
        samples = []
        for i in range(1000): # More samples
            sample = {
                "topic": topic,
                "device_id": "vehicle-ox-123",
                "status": "OPERATIONAL",
                "data": {
                    "velocity": 20.0 + (i % 5), # Highly repetitive
                    "location": {"lat": 37.7749, "lng": -122.4194},
                    "engine_temp": 90.0 + (i % 2)
                }
            }
            samples.append(json.dumps(sample).encode())
        
        # Train dictionary
        zdict = zstd.train_dictionary(8192, samples)
        dict_path = os.path.join(dict_dir, f"{topic}.dict")
        with open(dict_path, "wb") as f:
            f.write(zdict.as_bytes())
        
        index[topic] = {
            "dict_id": f"id_{topic.replace('.', '_')}",
            "path": dict_path
        }
    
    with open(os.path.join(dict_dir, "dict_index.json"), "w") as f:
        json.dump(index, f, indent=2)
    
    print(f"Created {len(topics)} sample dictionaries in {dict_dir}")

if __name__ == "__main__":
    create_sample_dicts()
