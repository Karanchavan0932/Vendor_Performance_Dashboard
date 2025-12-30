import pandas as pd
import os
from sqlalchemy import create_engine
import logging

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/app.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

engine = create_engine('sqlite:///inventory.db')


def ingest_db(df, table_name, engine):
    df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500   # 🔥 THIS FIXES THE ERROR
    )
    logging.info(f"Table {table_name} ingested successfully.")


def raw_data():
    data_table = "data/data"   # ✅ FIXED
    chunksize = 50_000

    print("CSV files:", os.listdir(data_table))

    for file in os.listdir(data_table):
        if not file.lower().endswith(".csv"):
            continue

        path = os.path.join(data_table, file)
        table_name = file[:-4]

        for chunk in pd.read_csv(path, chunksize=chunksize):
            ingest_db(chunk, table_name, engine)

    logging.info("Raw data ingestion completed.")

# 🔥 THIS WAS MISSING
if __name__ == "__main__":
    raw_data()
