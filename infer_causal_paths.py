
import networkx as nx
from dateutil.parser import parse
from sentence_transformers import SentenceTransformer
from relation_score_utils import compute_relation_components, compute_rst_score, compute_csim_score
from datetime import timezone


def safe_parse(x):
    try:
        dt = parse(str(x))
        if dt.tzinfo is None:
            # ✅ timezone-naive인 경우 UTC로 설정
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except:
        return None

def infer_causal_paths(shard_df, threshold=0.27, weights=None):
    model = SentenceTransformer('paraphrase-MiniLM-L6-v2')
    shard_df["parsed_time"] = shard_df["T_A"].fillna(shard_df["T_S"]).apply(safe_parse)

    G = nx.DiGraph()
    for i, row in shard_df.iterrows():
        G.add_node(i, **row.to_dict())

    for i in range(len(shard_df)):
        for j in range(len(shard_df)):
            if i == j:
                continue

            ti, tj = shard_df.loc[i, "parsed_time"], shard_df.loc[j, "parsed_time"]
            if not ti or not tj:
                print(f"⏳ Skip: Missing time on {i} or {j}")
                continue
            if ti >= tj:
                print(f"🕒 Skip: {i} ({ti}) !< {j} ({tj})")
                continue

            shard_i = shard_df.iloc[i]
            shard_j = shard_df.iloc[j]

            f_I, f_S, f_C, f_M = compute_relation_components(shard_i, shard_j, model)
            rst_score = compute_rst_score(f_I, f_S, f_C, f_M)
            score = compute_csim_score(f_I, f_S, f_C, f_M, rst_score, weights)

            if score >= threshold:
                print(f"🔥 ADD EDGE: {i} → {j} | score={score:.2f}")
                G.add_edge(i, j, weight=round(score, 3), type="causal")
            else:
                print(f"❌ SKIP EDGE: {i} → {j} | score={score:.2f}")

    return G
