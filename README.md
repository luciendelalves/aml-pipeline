# AML Pipeline — Detecção de Lavagem de Dinheiro

Pipeline de engenharia de dados para processamento e classificação de transações financeiras suspeitas, aplicando a arquitetura Medallion (Bronze → Silver → Gold) com Python e PostgreSQL.

---

## Contexto

O time de Compliance de uma fintech não conseguia detectar padrões de lavagem de dinheiro em tempo hábil. Os analistas recebiam os dados no dia seguinte, em planilha. Este pipeline automatiza o processamento e entrega os dados classificados por risco toda madrugada, antes do início do expediente.

---

## Arquitetura
```
CSV (IBM AML Dataset)
        ↓
    Bronze (raw)
    dado bruto, sem transformação
        ↓
    Silver (staging)
    limpeza, tipagem, padronização
        ↓
    Gold (analytics)
    classificação de risco por transação
```

**Por que batch e não streaming?**
O Compliance trabalha de manhã. Não é necessário dado em tempo real, basta garantir que os dados estejam prontos e organizados antes do início do expediente.

---

## Dataset

IBM Transactions for Anti Money Laundering (AML) — NeurIPS 2023  
Fonte: [Kaggle](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml)  
Arquivo utilizado: `HI-Small_Trans.csv` — 1.048.575 transações

| Coluna | Descrição |
|---|---|
| Timestamp | Data e hora da transação |
| From Bank / Account | Conta de origem |
| To Bank / Account | Conta de destino |
| Amount Paid / Received | Valor e moeda |
| Payment Format | Canal de pagamento |
| Is Laundering | 0 = legítima, 1 = lavagem |

---

## Stack

- Python 3.13
- PostgreSQL 15 (Docker)
- psycopg2
- pandas

---

## Estrutura
```
aml-pipeline/
├── docker-compose.yml
├── run_pipeline.py
├── pipeline/
│   ├── 01_load_bronze.py
│   ├── 02_transform_silver.py
│   └── 03_transform_gold.py
└── sql/
    └── analises.sql
```

---

## Como executar

**Pré-requisitos:** Docker e Python 3.10+
```bash
# 1. Clone o repositório
git clone https://github.com/luciendelalves/aml-pipeline.git
cd aml-pipeline

# 2. Baixe o dataset e coloque na raiz do projeto
# https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml

# 3. Instale as dependências
pip install pandas psycopg2-binary

# 4. Suba o banco
docker compose up -d

# 5. Execute o pipeline completo
python run_pipeline.py
```

---

## Camada Gold - Sinais de Risco

Cada transação recebe um `risk_score` baseado em três sinais:

| Sinal | Critério | Peso |
|---|---|---|
| Canal de risco | ACH, Cheque, Credit Card, Cash, Bitcoin | +1 |
| Conversão de moeda | payment_currency ≠ received_currency | +1 |
| Horário atípico | Entre 0h e 5h | +1 |

---

## Descobertas Analíticas

Os sinais de risco foram calibrados com base nos próprios dados — e contrariaram o senso comum de PLD:

**Canal de pagamento:** Wire e Cash, tradicionalmente considerados de alto risco, têm zero lavagem confirmada neste dataset. O canal ACH concentra 83% dos casos. Criminosos se ocultam no alto volume de transações legítimas.

**Horário:** A lavagem não ocorre na madrugada. Os picos estão nos horários comerciais normais, dificultando a detecção por regras de horário.

**Conversão de moeda:** 100% dos casos confirmados ocorrem sem conversão de moeda. Operadores sofisticados evitam sinais óbvios.

**Conta de maior risco:** A conta `100428660` (banco 070) movimentou mais de R$ 34 milhões com 100% das transações confirmadas como lavagem.

---

## Autor

**Luciendel Alves**  
Analista de Risco & PLD - iGaming  
[LinkedIn](https://www.linkedin.com/in/luciendel-alves-008321107/) · [GitHub](https://github.com/luciendelalves)