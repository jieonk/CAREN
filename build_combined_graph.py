import networkx as nx

def build_combined_graph(rst_graph, causal_graph):
    combined = nx.DiGraph()

    # Add all nodes from RST graph
    for node, data in rst_graph.nodes(data=True):
        combined.add_node(node, label=data.get("A", str(node)))

    # Add RST edges (only if they exist)
    for u, v, data in rst_graph.edges(data=True):
        if u < v:
            combined.add_edge(u, v, type="rst", weight=data.get("weight", 0))

    # Add Causal edges with strict existence check
    for u, v, data in causal_graph.edges(data=True):
        if causal_graph.has_edge(u, v):  # Enforce causal edge validation
            if combined.has_edge(u, v):
                combined[u][v]["type"] = "both"
                combined[u][v]["causal_weight"] = data.get("weight", 0)
            else:
                combined.add_edge(u, v, type="causal", causal_weight=data.get("weight", 0))

    return combined

