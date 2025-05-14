import os
from pyvis.network import Network

DEFAULT_OPTIONS = """
var options = {
  "physics": {
    "enabled": true,
    "repulsion": {
      "centralGravity": 0.01,
      "springLength": 300,
      "nodeDistance": 300,
      "springConstant": 0.02,
      "damping": 0.1
    },
    "solver": "repulsion"
  },
  "edges": {
    "arrows": {
      "to": { "enabled": true }
    }
  }
}
"""

def visualize_graph(G, output_file="graph.html", directed=False):

    os.makedirs(os.path.dirname(output_file), exist_ok=True)  # ✅ 폴더 자동 생성

    net = Network(notebook=False, directed=directed)
    net.set_options(DEFAULT_OPTIONS)

    for node, data in G.nodes(data=True):
        label = data.get("A") or data.get("label", str(node))
        net.add_node(node, label=label, size=35, font={'size': 30})

    # edge 처리 부분만 수정됨
    for u, v, data in G.edges(data=True):
        etype = data.get("type", "")
        weight = data.get("weight", 1.0)
        causal_weight = data.get("causal_weight", weight)

        if etype == "causal":
            color = "red"
            width = 1 + 5 * causal_weight
            title = f"causal: {causal_weight:.2f}"
            arrows = "to"
        elif etype == "rst":
            color = "gray"
            width = 1 + 3 * weight
            title = f"rst: {weight:.2f}"
            arrows = ""  # ✅ 무방향
        elif etype == "both":
            color = "purple"
            width = 1 + 5 * max(weight, causal_weight)
            title = f"both: rst={weight:.2f}, causal={causal_weight:.2f}"
            arrows = "to"  # ✅ 방향 표시
        else:
            color = "black"
            width = 1
            title = ""
            arrows = ""


        net.add_edge(u, v, color=color, width=width, title=title, arrows=arrows)
    net.save_graph(output_file)
    print(f"✅ Graph saved to {output_file}")


