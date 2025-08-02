import pandas as pd
import glob
import hashlib
import time
import os

# 📌 1. Чтение всех CSV-фрагментов
def load_and_merge_parts(pattern="raw/UNSW-NB15_*.csv"):
    files = sorted(glob.glob(pattern))
    print(f"Найдено файлов: {len(files)}")

    df_list = [pd.read_csv(f) for f in files if "LIST_EVENTS" not in f]
    df_full = pd.concat(df_list, ignore_index=True)
    print(f"Склеено строк: {len(df_full)}")
    return df_full

# 📌 2. Приведение к нужному виду
def format_as_flows(df):
    df_flow = df[[
        "id", "dur", "proto", "sport", "dport", "sbytes", "dbytes",
        "srcip", "dstip", "sttl", "dttl", "service", "label", "attack_cat"
    ]].copy()

    df_flow = df_flow.rename(columns={
        "srcip": "src_ip",
        "dstip": "dst_ip",
        "sport": "src_port",
        "dport": "dst_port",
        "sbytes": "bytes_out",
        "dbytes": "bytes_in",
        "dur": "duration",
        "label": "attack_label"
    })

    # Генерация flow_id и timestamp
    df_flow["flow_id"] = df_flow.apply(
        lambda row: hashlib.md5(f"{row.src_ip}-{row.dst_ip}-{row.src_port}-{row.dst_port}".encode()).hexdigest(),
        axis=1
    )
    df_flow["timestamp"] = int(time.time())  # или pd.Timestamp.now().timestamp()
    return df_flow

# 📌 3. Основная функция
def build():
    df_raw = load_and_merge_parts()
    df_flows = format_as_flows(df_raw)

    os.makedirs("out", exist_ok=True)
    df_raw.to_csv("out/UNSW-NB15_full.csv", index=False)
    df_flows.to_csv("out/flows_unsw.csv", index=False)

    print("✅ Готово: out/UNSW-NB15_full.csv + flows_unsw.csv")

if __name__ == "__main__":
    build()
