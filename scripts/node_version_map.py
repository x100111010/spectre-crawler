import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import json
import argparse

parser = argparse.ArgumentParser(description="Plots the crawling result on a map")
parser.add_argument(
    "-i", "--input", help="The JSON file with crawling results", required=True
)
parser.add_argument(
    "-o", "--output", help="The PNG output file", default="spectred_node_ver.png"
)
args = parser.parse_args()

with open(args.input, "r") as f:
    all_info = json.load(f)

nodes = all_info.get("nodes", all_info)

valid_nodes = {
    k: v for k, v in nodes.items() if v.get("error") in ("", "''") and v.get("loc")
}

if len(valid_nodes) == 0:
    parser.error("No public nodes found in JSON.")

location_df = pd.DataFrame(
    [
        {"address": k, "loc": v["loc"], "version": v.get("spectred")}
        for k, v in valid_nodes.items()
    ]
)

worldmap_path = "admin_0_countries/ne_110m_admin_0_countries.shp"
worldmap = gpd.read_file(worldmap_path)

# Create and plot the world map
fig, ax = plt.subplots(figsize=(12, 6))
worldmap.plot(color="lightgrey", ax=ax)

# Define colors based on node versions
color_map = {"0.3.14": "red", "0.3.15": "orange", "0.3.16": "green"}

for version, color in color_map.items():
    version_nodes = location_df[location_df["version"].str.contains(version)]
    if not version_nodes.empty:
        y = version_nodes["loc"].str.split(",").str[0].astype("float")  # Latitude
        x = version_nodes["loc"].str.split(",").str[1].astype("float")  # Longitude
        plt.scatter(x, y, s=5, color=color, label=f"Version {version}")

# Set plot limits, legend, and save
plt.xlim([-180, 180])
plt.ylim([-90, 90])
plt.legend(loc="upper right")
plt.savefig(args.output)
print(f"Map saved to {args.output}")
