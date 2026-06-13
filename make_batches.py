import pandas as pd
import os, time, math

df = pd.read_csv("data/power_sample.csv", sep=";")

# Assigning meter IDs cyclically
meters = [f"M{str(i).zfill(3)}" for i in range(1, 11)]
df["meter_id"] = [meters[i % 10] for i in range(len(df))]

# Combining Date + Time into a single timestamp column
df["timestamp"] = pd.to_datetime(df["Date"] + " " + df["Time"], dayfirst=True)
df = df[["timestamp", "meter_id", "Global_active_power"]].dropna()
df.columns = ["timestamp", "meter_id", "consumption"]

# Splitting into batches of 500 rows
batch_size = 500
n_batches = math.ceil(len(df) / batch_size)
os.makedirs("stream_input", exist_ok=True)

print(f"Creating {n_batches} batch files...")
for i in range(n_batches):
    batch = df.iloc[i*batch_size:(i+1)*batch_size]
    path = f"stream_input/batch_{str(i).zfill(4)}.csv"
    batch.to_csv(path, index=False)

print("Done. All batches ready in stream_input/")
