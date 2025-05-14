import networkx as nx
from sentence_transformers import SentenceTransformer
from networkx.algorithms.community import greedy_modularity_communities
from relation_score_utils import compute_relation_components, compute_rst_score

model = SentenceTransformer('paraphrase-MiniLM-L6-v2')

def add_clustering_groups(G):
    communities = list(greedy_modularity_communities(G))
    for i, community in enumerate(communities):
        for node in community:
            G.nodes[node]['group'] = i
    print(f"✅ {len(communities)} communities detected")

def build_rst_graph(shard_df, threshold=0.1):
    G = nx.Graph()

    for i, shard_i in shard_df.iterrows():
        action_i = shard_i.get("A")
        if not isinstance(action_i, str) or not action_i.strip():
            print(f"⚠️ Skipping node {i} due to missing or empty 'A' field")
            continue

        G.add_node(i, label=action_i.strip(), A=action_i.strip())

        for j, shard_j in shard_df.iterrows():
            if i >= j:
                continue

            action_j = shard_j.get("A")
            if not isinstance(action_j, str) or not action_j.strip():
                continue

            f_I, f_S, f_C, f_M = compute_relation_components(shard_i, shard_j, model)
            strength = compute_rst_score(f_I, f_S, f_C, f_M)

            if strength > threshold:
                G.add_edge(i, j, weight=round(strength, 2), type="rst")

    return G

def draw_graph(G):
    import matplotlib.pyplot as plt

    pos = nx.spring_layout(G, seed=42, k=20)
    plt.figure(figsize=(18, 12))

    node_labels = {node: data.get('label', str(node)) for node, data in G.nodes(data=True)}
    print("\n✅ Node labels:")
    for node, data in G.nodes(data=True):
        print(f"{node}: A = {data.get('A')}")

    edge_labels = {(u, v): f"{d['weight']:.2f}" if 'weight' in d else "" for u, v, d in G.edges(data=True)}

    nx.draw_networkx_nodes(G, pos, node_color='skyblue', node_size=800, alpha=0.8)
    nx.draw_networkx_edges(G, pos, edge_color='gray', arrows=False)
    nx.draw_networkx_labels(G, pos, labels=node_labels, font_size=9, font_family="Arial")
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=8, font_color='red')

    plt.axis('off')
    plt.tight_layout()
    plt.show()
