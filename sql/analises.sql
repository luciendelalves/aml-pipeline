-- ============================================================
-- analises.sql
-- Queries analíticas sobre a camada Gold
-- Projeto: AML Pipeline — Detecção de Lavagem de Dinheiro
-- Autor: Luciendel Alves
-- ============================================================


-- ============================================================
-- ANÁLISE 1: Validação do risk_score
-- Verifica se o score tem poder preditivo
-- ============================================================
SELECT
    risk_score,
    COUNT(*)                                           AS total_transacoes,
    SUM(CASE WHEN is_laundering THEN 1 ELSE 0 END)    AS confirmadas_lavagem,
    ROUND(
        100.0 * SUM(CASE WHEN is_laundering THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                                  AS pct_lavagem
FROM gold.transactions
GROUP BY risk_score
ORDER BY risk_score;


-- ============================================================
-- ANÁLISE 2: Lavagem por canal de pagamento
-- Identifica quais canais concentram mais casos confirmados
-- ============================================================
SELECT
    payment_format,
    COUNT(*)                                           AS total,
    SUM(CASE WHEN is_laundering THEN 1 ELSE 0 END)    AS lavagem,
    ROUND(
        100.0 * SUM(CASE WHEN is_laundering THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                                  AS pct_lavagem
FROM gold.transactions
GROUP BY payment_format
ORDER BY lavagem DESC;


-- ============================================================
-- ANÁLISE 3: Lavagem por hora do dia
-- Identifica padrões de horário suspeito
-- ============================================================
SELECT
    EXTRACT(HOUR FROM timestamp)::INT                  AS hora,
    COUNT(*)                                           AS total,
    SUM(CASE WHEN is_laundering THEN 1 ELSE 0 END)    AS lavagem
FROM gold.transactions
GROUP BY hora
ORDER BY hora;


-- ============================================================
-- ANÁLISE 4: Top 20 contas com mais transações suspeitas
-- Fila de priorização para o time de Compliance
-- ============================================================
SELECT
    from_account,
    from_bank,
    COUNT(*)                                           AS total_transacoes,
    SUM(CASE WHEN is_laundering THEN 1 ELSE 0 END)    AS confirmadas_lavagem,
    ROUND(SUM(amount_paid), 2)                         AS volume_total,
    ROUND(AVG(risk_score), 2)                          AS score_medio
FROM gold.transactions
WHERE is_laundering = TRUE
GROUP BY from_account, from_bank
ORDER BY confirmadas_lavagem DESC
LIMIT 20;


-- ============================================================
-- ANÁLISE 5: Conversão de moeda x Lavagem
-- Verifica se conversão de moeda é indicativo real
-- ============================================================
SELECT
    currency_conversion,
    COUNT(*)                                           AS total,
    SUM(CASE WHEN is_laundering THEN 1 ELSE 0 END)    AS lavagem,
    ROUND(
        100.0 * SUM(CASE WHEN is_laundering THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                                  AS pct_lavagem
FROM gold.transactions
GROUP BY currency_conversion
ORDER BY currency_conversion;