import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os

DB_CONFIG = {
    "host":     "localhost",
    "port":     5434,
    "dbname":   "aml_pipeline",
    "user":     "analyst",
    "password": "pld2024",
}

CSV_PATH = os.path.join(os.path.dirname(__file__), '..', 'HI-Small_Trans.csv')

CHUNK_SIZE = 50000

COLUMNS = [
    'timestamp', 'from_bank', 'from_account',
    'to_bank', 'to_account', 'amount_received',
    'received_currency', 'amount_paid',
    'payment_currency', 'payment_format', 'is_laundering'
]

def load_bronze():
    print("Conectando ao banco...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    print("Limpando tabela bronze antes de carregar...")
    cur.execute("TRUNCATE TABLE bronze.transactions;")
    conn.commit()

    print("Iniciando carga do CSV...")
    total = 0

    for chunk in pd.read_csv(
        CSV_PATH,
        sep=',',
        skiprows=1,         # ignora o cabeçalho com ;
        header=None,
        names=COLUMNS,
        dtype=str,
        usecols=range(11),  # pega só as 11 primeiras colunas
        chunksize=CHUNK_SIZE
    ):
        # Remove espaços extras
        chunk = chunk.apply(lambda col: col.str.strip())

        rows = list(chunk.itertuples(index=False, name=None))

        execute_values(
            cur,
            """
            INSERT INTO bronze.transactions (
                timestamp, from_bank, from_account,
                to_bank, to_account, amount_received,
                received_currency, amount_paid,
                payment_currency, payment_format, is_laundering
            ) VALUES %s
            """,
            rows
        )
        conn.commit()
        total += len(rows)
        print(f"  {total:,} linhas carregadas...")

    cur.close()
    conn.close()
    print(f"Carga finalizada! Total: {total:,} linhas no Bronze.")

if __name__ == "__main__":
    load_bronze()