from pyathena import connect
import pandas as pd
import warnings
import matplotlib.pyplot as plt
import numpy as np

# Define Athena configuration
workgroup = "primary"
output_bucket = "s3://athena-results-iot/"

# Connect to Athena
conn = connect(s3_staging_dir=output_bucket, work_group=workgroup, region_name="us-east-2")

# SQL query
query_sum_per_vehicle = """
SELECT SUM(CAST(vehicle_co2 as DECIMAL(9,2)))/1000 vehicle_co2_sum, 
SUM(CAST(vehicle_fuel as DECIMAL(9,2))) vehicle_fuel_sum, vehicle_id
FROM iotdatabase.lab4data_us_east_2
GROUP BY vehicle_id
"""

query_co2_speed = """
SELECT vspeed_range, AVG(vco2) avg_vco2
FROM(
SELECT 
CASE
WHEN vspeed < 5.0 THEN 'A: 0-5'
WHEN vspeed BETWEEN 5.0 AND 10.0 THEN 'B: 5-10'
WHEN vspeed BETWEEN 10.0 AND 15.0 THEN 'C: 10-15'
WHEN vspeed > 15.0 THEN 'D: 15-20'
END vspeed_range, vco2
FROM
(SELECT CAST(vehicle_co2 as DECIMAL(9,2)) vco2, CAST(vehicle_speed as DECIMAL(9,2)) vspeed
FROM iotdatabase.lab4data_us_east_2)
)
GROUP BY
vspeed_range
ORDER BY
vspeed_range;
"""


with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    df_sum_per_vehicle = pd.read_sql(query_sum_per_vehicle, conn)
    df_co2_speed = pd.read_sql(query_co2_speed, conn)
    
print(df_sum_per_vehicle)
bar_width = 0.4
x_positions = np.arange(len(df_sum_per_vehicle))

fig, ax = plt.subplots()
ax.bar(x_positions - bar_width / 2, df_sum_per_vehicle['vehicle_co2_sum'], bar_width, label='CO2x10^3', color='blue')
ax.bar(x_positions + bar_width / 2, df_sum_per_vehicle['vehicle_fuel_sum'], bar_width, label='Fuel', color='orange')
ax.set_xlabel('Vehicle ID')
ax.set_ylabel('Values')
ax.set_title('Comparison of CO2 and Fuel per Vehicle')
ax.set_xticks(x_positions)
ax.set_xticklabels(df_sum_per_vehicle['vehicle_id'])
ax.legend()

#df_sum_per_vehicle.plot(kind='bar', x='vehicle_id', y='vehicle_co2_sum', title='CO2 Sum per Vehicle')

print(df_co2_speed)
df_co2_speed.plot(x='vspeed_range', y='avg_vco2', kind='line', marker='o', title='CO2 vs Speed')

plt.show()


