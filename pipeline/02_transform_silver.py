import psycopg2

DB_CONFIG = {
    "host":     "localhost",
    "port":     5434,
    "dbname":   "aml_pipeline",
    "user":     "analyst",
    "password": "pld2024",
}

def transform_silver():
    print("Conectando ao banco...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    print("Limpando tabela silver antes de transformar...")
    cur.execute("TRUNCATE TABLE silver.transactions;")
    conn.commit()

    print("Transformando Bronze → Silver...")
    cur.execute("""
        INSERT INTO silver.transactions (
            timestamp,
            from_bank,
            from_account,
            to_bank,
            to_account,
            amount_received,
            received_currency,
            amount_paid,
            payment_currency,
            payment_format,
            is_laundering
        )
        SELECT
            TO_TIMESTAMP(TRIM(timestamp), 'YYYY/MM/DD HH24:MI'),
            TRIM(from_bank),
            TRIM(from_account),
            TRIM(to_bank),
            TRIM(to_account),
            TRIM(amount_received)::NUMERIC(18,2),
            TRIM(received_currency),
            TRIM(amount_paid)::NUMERIC(18,2),
            TRIM(payment_currency),
            TRIM(payment_format),
            SPLIT_PART(TRIM(is_laundering), ';', 1)::INTEGER::BOOLEAN
        FROM bronze.transactions
        WHERE TRIM(timestamp)        IS NOT NULL
          AND TRIM(amount_received)  != ''
          AND TRIM(amount_paid)      != '';
    """)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM silver.transactions;")
    total = cur.fetchone()[0]

    cur.close()
    conn.close()
    print(f"Transformação concluída! Total: {total:,} linhas na Silver.")

if __name__ == "__main__":
    transform_silver()