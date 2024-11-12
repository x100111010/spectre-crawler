import json
import re
from collections import defaultdict

with open("data/n2.json", "r") as f:
    data = json.load(f)["nodes"]


def extract_base_ip(ip):
    ipv4_pattern = re.compile(r"::ffff:(\d+\.\d+\.\d+\.\d+)")
    match = ipv4_pattern.match(ip)
    if match:
        return match.group(1)
    return ip.split(":")[0]


# Filter out duplicates by IP address and `id`
unique_nodes = {}
for address, node_data in data.items():
    base_ip = extract_base_ip(address)
    node_id = node_data["id"]

    # If the node ID or IP is not already in unique_nodes, add it
    if node_id not in unique_nodes or base_ip not in unique_nodes:
        unique_nodes[node_id] = node_data

# analyze the spectred versions
spectre_counts = defaultdict(int)
total_unique_nodes = 0

for node in unique_nodes.values():
    total_unique_nodes += 1
    spectre_version = node["spectred"].split(":")[1].split("/")[0]
    spectre_counts[spectre_version] += 1

# results
print(f"Total unique nodes: {total_unique_nodes}")
for version in ["0.3.14", "0.3.15", "0.3.16"]:
    print(f"Spectred v{version}: {spectre_counts[version]}")

# save the unique nodes to a new file without duplicates
with open("data/unique_nodes.json", "w") as f:
    json.dump({"nodes": unique_nodes}, f, indent=2)
