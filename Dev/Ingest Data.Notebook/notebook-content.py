# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "88d86d1c-2274-429c-a9b6-2fa93bb7a621",
# META       "default_lakehouse_name": "LH",
# META       "default_lakehouse_workspace_id": "cfd7654a-1ac0-46ba-83e1-7c5abb6dc4c1"
# META     }
# META   }
# META }

# CELL ********************

from datetime import datetime, date
import pandas as pd
import requests
import json
import os
import time

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Define the start and end date
start_date = '2018-06-01'
end_date = '2024-08-07'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

month_range = pd.date_range(start=start_date, end=end_date, freq='MS')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create Start Date and End Date columns
df = pd.DataFrame({
    'Start Date': month_range,
    'End Date': month_range + pd.offsets.MonthEnd(0)
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = pd.DataFrame({
    'Start Date': month_range,
    'End Date': month_range + pd.DateOffset(days=6)
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert the dates to the desired format
df['Start Date'] = df['Start Date'].dt.strftime('%Y-%m-%d')
df['End Date'] = df['End Date'].dt.strftime('%Y-%m-%d')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# List to hold weekly data
weekly_data = []

# Iterate over each month
for start in month_range:
    # End of the month
    end_of_month = (start + pd.DateOffset(months=1)) - pd.DateOffset(days=1)
    
    # Start and end dates for each week
    current_start = start
    while current_start <= end_of_month:
        current_end = min(current_start + pd.DateOffset(days=6), end_of_month)
        weekly_data.append({
            'Start Date': current_start.strftime('%Y-%m-%d'),
            'End Date': current_end.strftime('%Y-%m-%d')
        })
        # Move to the start of the next week
        current_start = current_end + pd.DateOffset(days=1)

# Create DataFrame
df = pd.DataFrame(weekly_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#for index, row in df.iterrows():
#    start_date = row['Start Date']
#    end_date = row['End Date']

print(f"{start_date}: {end_date}")
url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
params = {
    "format": "geojson",
    "starttime": start_date,
    "endtime": end_date
}

# Fetch the data from the API
response = requests.get(url, params=params)

# Convert the response to JSON
earthquake_data = response.json()

# Save the JSON data to a file
#with open(f"abfss://cfd7654a-1ac0-46ba-83e1-7c5abb6dc4c1@onelake.dfs.fabric.microsoft.com/88d86d1c-2274-429c-a9b6-2fa93bb7a621/Files/Earthquake/Earthquake_data_{end_date}.json", 'w') as json_file:
#    json.dump(earthquake_data, json_file, indent=4)

file_path = f"/lakehouse/default/Files/Earthquake/Earthquake_data_{end_date}.json"

if not os.path.isfile(file_path):
    # File does not exist, create it and write the JSON data
    with open(file_path, 'w') as json_file:
        json.dump(earthquake_data, json_file, indent=4)
    print(f"File created and data saved to {file_path}")
else:
    print(f"File already exists: {file_path}")
print("Data saved to earthquake_data.json")
#time.sleep(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_date = '2010-04-21'
end_date = '2010-04-30'

print(f"{start_date}: {end_date}")
url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
params = {
    "format": "geojson",
    "starttime": start_date,
    "endtime": end_date
}

# Fetch the data from the API
response = requests.get(url, params=params)

# Convert the response to JSON
earthquake_data = response.json()

print(response.status_code)

# Save the JSON data to a file
#with open(f"abfss://cfd7654a-1ac0-46ba-83e1-7c5abb6dc4c1@onelake.dfs.fabric.microsoft.com/88d86d1c-2274-429c-a9b6-2fa93bb7a621/Files/Earthquake/Earthquake_data_{end_date}.json", 'w') as json_file:
#    json.dump(earthquake_data, json_file, indent=4)

file_path = f"/lakehouse/default/Files/Earthquake/Earthquake_data_{end_date}.json"

if not os.path.isfile(file_path):
    # File does not exist, create it and write the JSON data
    with open(file_path, 'w') as json_file:
        json.dump(earthquake_data, json_file, indent=4)
    print(f"File created and data saved to {file_path}")
else:
    print(f"File already exists: {file_path}")
print("Data saved to earthquake_data.json")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
