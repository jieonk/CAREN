
import os
import json
import uuid
import getpass
from pathlib import Path
from datetime import datetime
import pandas as pd

from preprocessor import extract_file_metadata
from build_rst_graph import build_rst_graph
from infer_causal_paths import infer_causal_paths
from build_combined_graph import build_combined_graph
from graph_visualizer import visualize_graph
from config import RST_THRESHOLD, CAUSAL_THRESHOLD, WEIGHTS, OUTPUT_FOLDER
from build_rdf_graph import build_rdf_graph,visualize_rdf_graph

def process_folder(folder_path):
    folder_id = str(uuid.uuid4())
    folder_data = {
        "folderID": folder_id,
        "folderPath": str(folder_path),
        "folderOwner": getpass.getuser(),
        "files": []
    }

    for root, dirs, files in os.walk(folder_path):
        for file_name in files:
            file_path = Path(root) / file_name
            file_metadata = extract_file_metadata(file_path, folder_id)
            folder_data["files"].append(file_metadata)

    return folder_data

def save_output(data, output_path):
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_caren_actions(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        caren_data = json.load(f)

    all_actions = []
    for file in caren_data.get("files", []):
        meta = file.get("metadata", {})
        for idx, action in enumerate(file.get("actions", [])):
            if isinstance(action, dict) and action.get("action"):
                all_actions.append({
                    "A": action.get("action"),
                    "T_A": action.get("timestamp"),
                    "T_S": file.get("modifiedTime"),
                    "ID": f"{file.get('fileID')}_{idx}", # ✅ identity
                    "C": action.get("context"),
                    "M": json.dumps({
                        "device_id": meta.get("device_id"),
                        "user_id": meta.get("user_id"),
                        "address": meta.get("address"),
                        "card_number": meta.get("card_number"),
                        "ip_address": meta.get("ip_address")
                    }, ensure_ascii=False)
                })

                action_obj = {
                    "A": action.get("action"),
                    "T_A": action.get("timestamp"),
                    "T_S": file.get("modifiedTime"),
                    "ID": json.dumps({
                        "device_id": meta.get("device_id"),
                        "user_id": meta.get("user_id")}),
                    "C": action.get("context"),
                    "M": json.dumps({
                        "address": meta.get("address"),
                        "card_number": meta.get("card_number"),
                        "ip_address": meta.get("ip_address")
                    }, ensure_ascii=False)
                }

                # 출력
                print(f"[{idx}] Action: {action_obj['A']}")
                print(f"     Context: {action_obj['C']}")
                print(f"     Timestamp: {action_obj['T_A']}")
                print(f"     Metadata: {action_obj['M']}")
                print(f"     ID: {action_obj['ID']}")


                print("-" * 50)

    return pd.DataFrame(all_actions)


def main_pipeline(input_folder, output_file):

    # Step 1: CAREN preprocessing
    result = process_folder(input_folder)
    save_output(result, output_file)
    print(f"✅ CAREN preprocessed output saved to: {output_file}")

    # Step 2: Load actions and build graphs
    df = load_caren_actions(output_file)
    print(f"📦 Loaded {len(df)} actions")

    rst_graph = build_rst_graph(df, threshold=RST_THRESHOLD)
    print(f"🔎 RST graph nodes: {len(rst_graph.nodes())}, edges: {len(rst_graph.edges())}")

    # ✅ (2) 모든 엣지의 relation 값 출력 (없으면 fallback)
    for u, v, data in rst_graph.edges(data=True):
        rel = data.get("relation") or data.get("type") or "?"
        print(f"🔗 {u} → {v} | relation: {rel}")

    visualize_graph(rst_graph, output_file=os.path.join(OUTPUT_FOLDER, "rst_graph.html"), directed=False)

    causal_graph = infer_causal_paths(df, threshold=CAUSAL_THRESHOLD, weights=WEIGHTS)
    visualize_graph(causal_graph, output_file=os.path.join(OUTPUT_FOLDER, "causal_graph.html"), directed=True)

    combined_graph = build_combined_graph(rst_graph, causal_graph)
    visualize_graph(combined_graph, output_file="outputs/combined_graph.html", directed=True)

    G = build_rdf_graph("caren_preprocessed_output.json")
    visualize_rdf_graph(G, output_file="outputs/rdf_graph.html")
#    print("✅ Graph visualization saved to: outputs/combined_graph.html")


if __name__ == "__main__":
    input_folder = "scene1(coffee)"
    output_file = "caren_preprocessed_output.json"
    main_pipeline(input_folder, output_file)
