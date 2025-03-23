import pandas as pd
import numpy as np
import json
import math
from datetime import datetime, timedelta
import boto3

# Initialize Kinesis client
kinesis_client = boto3.client('kinesis', 
                             region_name='us-east-1',  # Replace with your region
                             aws_access_key_id='AWSACCESSKEY',  # Replace with your credentials
                             aws_secret_access_key='SECRETKEY')

# Define your stream name
stream_name = 'streamname'  # Replace with your actual stream name

# Start with a base timestamp (current time)
base_timestamp = datetime.now()
# Convert to epoch seconds
base_epoch = int(base_timestamp.timestamp())

# Generate and stream 100 records, 5 minutes apart
for i in range(10000):
    # Calculate timestamp (5 minutes apart)
    current_timestamp = base_epoch + (i * 300)  # 300 seconds = 5 minutes
    
    # For fire intensity, create a cyclic pattern that increases for 10 minutes (2 intervals) and decreases for 10 minutes
    cycle_position = i % 4  # 4 intervals of 5 minutes = 20 minutes cycle
    
    # Base fire intensity 
    base_intensity = 5.0
    
    # Modify intensity based on position in cycle
    if cycle_position < 2:  # First 10 minutes: increasing
        intensity_modifier = cycle_position * 3.0
    else:  # Last 10 minutes: decreasing
        intensity_modifier = (4 - cycle_position) * 3.0
    
    fire_intensity = base_intensity + intensity_modifier
    
    # Add some randomness to fire intensity
    fire_intensity += np.random.normal(0, 0.5)
    fire_intensity = max(0, fire_intensity)
    
    # Generate correlated features
    # Higher fire intensity -> higher temperature and gas concentration, lower humidity
    
    # Temperature (20-40°C) correlates positively with fire intensity
    temp = 25 + (fire_intensity / 2) + np.random.normal(0, 1.5)
    temp = max(20, min(40, temp))
    
    # Humidity (10-80%) correlates negatively with fire intensity
    humidity = 60 - (fire_intensity * 3) + np.random.normal(0, 5)
    humidity = max(10, min(80, humidity))
    
    # Gas concentration (0-10 ppm) correlates positively with fire intensity
    gas = (fire_intensity / 2) + np.random.normal(0, 0.7)
    gas = max(0, min(10, gas))
    
    # Wind speed (0-15 m/s) with some correlation to fire intensity
    wind = 5 + (fire_intensity / 4) + np.random.normal(0, 2)
    wind = max(0, min(15, wind))
    
    # Distance from sensor (10-100 meters) with loose negative correlation to intensity
    distance = 50 - (fire_intensity / 2) + np.random.normal(0, 10)
    distance = max(10, min(100, distance))

    # Create data record for this iteration
    data = {
        'timestamp': datetime.fromtimestamp(current_timestamp).isoformat(),
        'temperature': temp,
        'humidity': humidity,
        'fire_intensity': fire_intensity,
        'gas_concentration': gas,
        'wind_speed': wind,
        'distance': distance
    }
    
    # Convert data to JSON string
    data_json = json.dumps(data)
    
    # Stream to Kinesis
    response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=data_json,
        PartitionKey=str(current_timestamp)  # Using timestamp as partition key
    )
    
    # Optional: Print response or add a small delay
    print(f"Record sent. Sequence number: {response['SequenceNumber']}")
    
    # To avoid throttling for high-volume streams, you might want to add:
    # import time
    # time.sleep(0.1)  # Small 
