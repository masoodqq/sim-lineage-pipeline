import json, time, random, os, uuid
from datetime import datetime, timezone

OUTPUT_DIR = "./sim_outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

TOOLS = ["SPICE", "Lumerical", "Cadence"]
CELLS = ["ring_modulator_v3", "mzi_switch_v2", "grating_coupler_v5"]
METRICS = ["insertion_loss_db", "extinction_ratio_db", "bandwidth_ghz"]

def generate_file():
    filename = f"{OUTPUT_DIR}/sim_{uuid.uuid4().hex[:8]}.json"
    with open(filename, "w") as f:
        for _ in range(random.randint(3, 8)):
            record = {
                "cell_name": random.choice(CELLS),
                "tool": random.choice(TOOLS),
                "tool_version": f"{random.randint(17,19)}.{random.randint(0,5)}",
                "metric": random.choice(METRICS),
                "value": round(random.uniform(-5.0, 5.0), 4),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            f.write(json.dumps(record) + "\n")
    print(f"Generated {filename}")

if __name__ == "__main__":
    print("Generating fake simulation files every 10 seconds. Ctrl+C to stop.")
    while True:
        generate_file()
        time.sleep(10)