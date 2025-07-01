import os
import subprocess
import json
import sys

# Fields to extract and conversion mapping
FIELDS_TO_EXTRACT = {
    "AverageLatency": lambda v: v / 1_000_000,
    "OperationThroughput": lambda v: v,
    "ErrorRate": lambda v: v,
    "Latency50thPercentile": lambda v: v / 1_000_000,
    "Latency80thPercentile": lambda v: v / 1_000_000,
    "Latency90thPercentile": lambda v: v / 1_000_000,
    "Latency95thPercentile": lambda v: v / 1_000_000,
    "Latency99thPercentile": lambda v: v / 1_000_000,
    "LatencyMin": lambda v: v / 1_000_000,
    "LatencyMax": lambda v: v / 1_000_000,
    "DurationTotal": lambda v: v / 1_000_000_000,
    "ErrorsTotal": lambda v: v,
    "OperationsTotal": lambda v: v,
    "DocumentsTotal": lambda v: v,
}

def process_ftdc_files(ftdc_dir, curator_path):
    result_map = {}
    for filename in os.listdir(ftdc_dir):
        if not filename.endswith(".ftdc"):
            continue

        file_path = os.path.join(ftdc_dir, filename)
        base_name = filename[:-5]  # Remove .ftdc
        output_path = f"/tmp/{base_name}.output"

        # Run the curator command
        try:
            subprocess.run([
                os.path.join(curator_path, "curator"),
                "calculate-rollups",
                "--inputFile", file_path,
                "--outputFile", output_path
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError:
            print(f"Error running curator on {filename}")
            continue

        # Read and filter the output
        try:
            with open(output_path, "r") as f:
                output_data = json.load(f)
                filtered = {}

                for item in output_data:
                    name = item.get("Name")
                    if name in FIELDS_TO_EXTRACT:
                        value = item.get("Value")
                        filtered[name] = round(FIELDS_TO_EXTRACT[name](value), 3)

                result_map[base_name] = filtered
        except Exception as e:
            print(f"Error reading/parsing output for {filename}: {e}")
            continue

    print(json.dumps(result_map, indent=2))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <ftdc_directory> <curator_directory>")
        sys.exit(1)

    ftdc_directory = sys.argv[1]
    curator_directory = sys.argv[2]

    process_ftdc_files(ftdc_directory, curator_directory)

