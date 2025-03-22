import pandas as pd
import numpy as np
import csv
import math
from datetime import datetime, timedelta

# Start with a base timestamp (current time)
base_timestamp = datetime.now()
# Convert to epoch seconds
base_epoch = int(base_timestamp.timestamp())

# Create lists to store data
timestamps = []
temperatures = []
humidities = []
fire_intensities = []
gas_concentrations = []
wind_speeds = []
distances = []

# Generate 100 records, 5 minutes apart
for i in range(100):
    # Calculate timestamp (5 minutes apart)
    current_timestamp = base_epoch + (i * 300)  # 300 seconds = 5 minutes
    timestamps.append(current_timestamp)
    
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
    fire_intensities.append(max(0, fire_intensity))
    
    # Generate correlated features
    # Higher fire intensity -> higher temperature and gas concentration, lower humidity
    
    # Temperature (20-40°C) correlates positively with fire intensity
    temp = 25 + (fire_intensity / 2) + np.random.normal(0, 1.5)
    temperatures.append(max(20, min(40, temp)))
    
    # Humidity (10-80%) correlates negatively with fire intensity
    humidity = 60 - (fire_intensity * 3) + np.random.normal(0, 5)
    humidities.append(max(10, min(80, humidity)))
    
    # Gas concentration (0-10 ppm) correlates positively with fire intensity
    gas = (fire_intensity / 2) + np.random.normal(0, 0.7)
    gas_concentrations.append(max(0, min(10, gas)))
    
    # Wind speed (0-15 m/s) with some correlation to fire intensity
    wind = 5 + (fire_intensity / 4) + np.random.normal(0, 2)
    wind_speeds.append(max(0, min(15, wind)))
    
    # Distance from sensor (10-100 meters) with loose negative correlation to intensity
    distance = 50 - (fire_intensity / 2) + np.random.normal(0, 10)
    distances.append(max(10, min(100, distance)))

# Create DataFrame
data = {
    'timestamp': [datetime.fromtimestamp(ts).isoformat() for ts in timestamps],
    'temperature': temperatures,
    'humidity': humidities,
    'fire_intensity': fire_intensities,
    'gas_concentration': gas_concentrations,
    'wind_speed': wind_speeds,
    'distance': distances
}

df = pd.DataFrame(data)

# Save to CSV
df.to_csv('fire_sensor_data.csv', index=False)
print("CSV file 'fire_sensor_data.csv' has been created.")

# Display the first few rows for verification
print("\nFirst 5 rows of data:")
print(df.head())

# Display statistics to verify patterns
print("\nStatistics by 10-minute intervals:")
df['interval'] = [i // 2 for i in range(len(df))]  # Each interval is 10 minutes (2 rows)
print(df.groupby('interval')['fire_intensity'].mean())
