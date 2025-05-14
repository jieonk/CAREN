
from sklearn.metrics.pairwise import cosine_similarity

def compute_relation_components(shard_i, shard_j, model):
    def jaccard_similarity(a, b):
        set_a = set(str(a).split())
        set_b = set(str(b).split())
        if not set_a or not set_b:
            return 0.0
        return len(set_a & set_b) / len(set_a | set_b)

    def semantic_similarity(a, b):
        if not a or not b:
            return 0.0
        try:
            vec_a = model.encode(str(a), convert_to_tensor=True)
            vec_b = model.encode(str(b), convert_to_tensor=True)
            return float(cosine_similarity([vec_a], [vec_b])[0][0])
        except:
            return 0.0

    f_I = jaccard_similarity(shard_i.get("ID", ""), shard_j.get("ID", ""))
    f_S = semantic_similarity(shard_i.get("A", ""), shard_j.get("A", ""))
    f_C = semantic_similarity(shard_i.get("C", ""), shard_j.get("C", ""))
    f_M = semantic_similarity(shard_i.get("M", ""), shard_j.get("M", ""))

    return f_I, f_S, f_C, f_M

def compute_rst_score(f_I, f_S, f_C, f_M, weights=None):
    weights = weights or {"I": 0.05, "C": 0.3, "S": 0.25, "M": 0.1}
    sigma = (
        weights["I"] * f_I +
        weights["C"] * f_C +
        weights["S"] * f_S +
        weights["M"] * f_M
    )
    return sigma

def compute_csim_score(f_I, f_S, f_C, f_M, rst_score, weights=None):
    weights = weights or {
        "semantic": 0.35,
        "context": 0.30,
        "metadata": 0.2,
        "identity": 0.1,
        "rst": 0.05
    }
    score = (
        weights["semantic"] * f_S +
        weights["context"] * f_C +
        weights["metadata"] * f_M +
        weights["identity"] * f_I +
        weights["rst"] * rst_score
    )
    return score
