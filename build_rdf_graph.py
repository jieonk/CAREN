import json, os
import networkx as nx
from pyvis.network import Network

def build_rdf_graph(json_path):
    import os
    import json
    import networkx as nx

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    G = nx.DiGraph()

    folder_path = data.get("folderPath")
    folder_name = os.path.basename(folder_path)
    folder_node = f"folder:{folder_name}"

    for file in data.get("files", []):
        file_name = file.get("fileName")
        file_id = f"file:{file_name}"
        created = file.get("createdTime")
        modified = file.get("modifiedTime")
        meta = file.get("metadata", {})
        user = meta.get("user_id") or meta.get("device_id") or "unknown_user"
        ip = meta.get("ip_address")

        # ✅ 폴더 → 파일
        G.add_edge(folder_node, file_id, label="contains")

        # ✅ 파일 → 사용자
        G.add_edge(file_id, f"user:{user}", label="has_actor")

        # ✅ 파일 → 메타데이터
        if meta:
            meta_label = json.dumps(meta, ensure_ascii=False)
            meta_node = f"metadata:{file_name}"
            G.add_node(meta_node, label=meta_label)
            G.add_edge(file_id, meta_node, label="has_metadata")

        for action in file.get("actions", []):
            action_text = action.get("action")
            timestamp = action.get("timestamp")
            if not action_text:
                continue

            action_node = f"action:{action_text.strip()}"
            G.add_edge(file_id, action_node, label="has_action")  # ✅ 파일 → 액션

            # ✅ 액션 → 발생시각
            if timestamp:
                G.add_edge(action_node, f"time:{timestamp}", label="occurred_at")

            # ✅ 액션 → IP
            if ip:
                G.add_edge(action_node, f"ip:{ip}", label="from_ip")

    return G

def visualize_rdf_graph(G, output_file="outputs/rdf_graph.html"):
    net = Network(directed=True)
    net.set_options("""
    var options = {
      "edges": {
        "arrows": {
          "to": {
            "enabled": true
          }
        }
      }
    }
    """)

    for node in G.nodes():
        net.add_node(node, label=node, shape="ellipse")

    for u, v, data in G.edges(data=True):
        net.add_edge(u, v, label=data.get("label", ""), arrows="to")

    net.save_graph(output_file)
    print(f"✅ RDF graph saved to: {output_file}")