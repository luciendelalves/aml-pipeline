import psycopg2

DB_CONFIG = {
    "host":     "localhost",
    "port":     5434,
    "dbname":   "aml_pipeline",
    "user":     "analyst",
    "password": "pld2024",
}

def transform_gold():
    print("Conectando ao banco...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    print("Limpando tabela gold antes de transformar...")
    cur.execute("TRUNCATE TABLE gold.transactions;")
    conn.commit()

    print("Transformando Silver → Gold...")
    cur.execute("""
        INSERT INTO gold.transactions (
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
            is_laundering,
            currency_conversion,
            high_risk_channel,
            atypical_hour,
            risk_score
        )
        SELECT
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
            is_laundering,

            -- Sinal 1: houve conversão de moeda?
            CASE WHEN payment_currency != received_currency
                 THEN TRUE ELSE FALSE END         AS currency_conversion,

            -- Sinal 2: canal de alto risco?
            CASE WHEN payment_format IN ('ACH', 'Cheque', 'Credit Card', 'Cash', 'Bitcoin')
                 THEN TRUE ELSE FALSE END         AS high_risk_channel,

            -- Sinal 3: horário atípico (madrugada)?
            CASE WHEN EXTRACT(HOUR FROM timestamp) BETWEEN 0 AND 5
                 THEN TRUE ELSE FALSE END         AS atypical_hour,

            -- Score: soma dos sinais de risco
            (
                CASE WHEN payment_currency != received_currency  THEN 1 ELSE 0 END +
                CASE WHEN payment_format IN ('ACH', 'Cheque', 'Credit Card', 'Cash', 'Bitcoin') THEN 1 ELSE 0 END +
                CASE WHEN EXTRACT(HOUR FROM timestamp) BETWEEN 0 AND 5 THEN 1 ELSE 0 END
            )                                    AS risk_score

        FROM silver.transactions;
    """)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM gold.transactions;")
    total = cur.fetchone()[0]

    cur.close()
    conn.close()
    print(f"Transformação concluída! Total: {total:,} linhas na Gold.")

if __name__ == "__main__":
    transform_gold()