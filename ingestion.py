import pandas as pd
from sklearn.model_selection import train_test_split

def load_flows(path: str):
    # Пример: CSV с полями [src_ip, dst_ip, src_port, dst_port, protocol, bytes, packets, timestamp]
    df = pd.read_csv(path)
    return df

if __name__ == "__main__":
    df = load_flows("data/cic_ids17_flows.csv")
    print(f"Loaded {len(df)} flows")
    train, test = train_test_split(df, test_size=0.2, random_state=42)
    train.to_csv("data/train.csv", index=False)
    test.to_csv("data/test.csv", index=False)