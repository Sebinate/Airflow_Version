import pandas as pd

import pyarrow as pa
from pyarrow.parquet import ParquetFile
from sqlalchemy import create_engine

from time import time
import argparse

import os

class DataIngestion:
    def __init__(self):
        try:
            parser = argparse.ArgumentParser()
            parser.add_argument("--user", required = True)
            parser.add_argument("--password", required = True)
            parser.add_argument("--host", required = True)
            parser.add_argument("--port", required = True)
            parser.add_argument("--db", required = True)
            parser.add_argument("--table_name", required = True)
            parser.add_argument("--url", required = True)

            args = parser.parse_args()

            self.user = args.user
            self.password = args.password
            self.host = args.host
            self.port = args.port
            self.db = args.db
            self.table_name = args.table_name
            self.url = args.url
            self.engine = None

        except Exception:
            print("Fatal error error has occured")

    def connect(self):
        print("Initializing SQLAlchemy engine")
        if not self.engine:
            connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
            self.engine = create_engine(connection_string)
            print(self.engine.connect())

    def read_parquet(self, url):
        print("Reading parquet file")
        df = pd.read_parquet(url)

        os.makedirs("artifacts", exist_ok = True)
        df.to_parquet("artifacts/raw.parquet", engine = 'pyarrow', index = False)

        pf = ParquetFile("artifacts/raw.parquet")
        return df, pf
    
    def read_csv(self, url):
        print("Reading csv file")
        df = pd.read_csv(url)

        os.makedirs("artifacts", exist_ok = True)
        df.to_parquet(f"artifacts/raw.parquet")

        pf = ParquetFile("artifacts/raw.parquet")

        return df, pf

    def ingestion(self):
        self.connect()

        if self.url[-3:] != "csv":
            df, pf = self.read_parquet(self.url)

        else:
            df, pf= self.read_csv(self.url)
        
        df.head(0).to_sql(name = self.table_name, con = self.engine, if_exists = 'replace')

        batches = pf.iter_batches(batch_size = 100_000)

        for batch in batches:
            s = time()

            df = batch.to_pandas()
            df.to_sql(name = self.table_name, con = self.engine, if_exists = 'append')

            e = time()
            print(f'inserted another chunk..., took {e - s:.3f} second')

if __name__ == "__main__":
    ingest = DataIngestion()
    ingest.ingestion()