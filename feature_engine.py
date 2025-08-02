import pandas as pd
import networkx as nx


def compute_basic_features(df: pd.DataFrame):
    df['flow_duration'] = df['end_time'] - df['start_time']
    df['byte_rate'] = df['bytes'] / (df['flow_duration'] + 1e-6)
    return df


def build_session_graph(df: pd.DataFrame):
    G = nx.DiGraph()
    for _, row in df.iterrows():
        s, d = row['src_ip'], row['dst_ip']
        G.add_edge(s, d)
    # пример: degree centrality
    dc = nx.degree_centrality(G)
    feats = [(node, dc.get(node, 0)) for node in G.nodes()]
    return feats

if __name__ == "__main__":
    df = pd.read_csv("data/train.csv")
    df = compute_basic_features(df)
    feats = build_session_graph(df)
    print(f"Computed {len(feats)} graph features")