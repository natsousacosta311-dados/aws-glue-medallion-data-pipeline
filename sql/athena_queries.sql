-- 1. Top 10 produtos por faturamento no mês atual
SELECT
    produto,
    categoria_produto,
    SUM(faturamento_total)   AS receita_total,
    SUM(total_itens_vendidos) AS unidades_vendidas,
    SUM(qtd_pedidos)         AS num_pedidos
FROM "db-glue-zoop"."zoop-glue-vendas_kpi_gold"
WHERE data >= DATE_TRUNC('month', CURRENT_DATE)
GROUP BY 1, 2
ORDER BY receita_total DESC
LIMIT 10;

-- 2. Faturamento diário por região (Silver — query mais pesada, use com filtro de data)
SELECT
    data,
    regiao_cliente,
    SUM(quantidade * preco_unitario) AS faturamento,
    COUNT(DISTINCT id_cliente)       AS clientes_unicos
FROM "db-glue-zoop"."zoop-glue-vendas_zoop_silver"
WHERE data BETWEEN DATE '2024-01-01' AND DATE '2024-01-31'  -- partition pruning ativo
GROUP BY 1, 2
ORDER BY 1, faturamento DESC;

-- 3. Ticket médio por canal de venda
SELECT
    canal_venda,
    ROUND(SUM(quantidade * preco_unitario) / COUNT(DISTINCT id_venda), 2) AS ticket_medio,
    COUNT(DISTINCT id_venda) AS total_pedidos
FROM "db-glue-zoop"."zoop-glue-vendas_zoop_silver"
WHERE data >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY 1
ORDER BY ticket_medio DESC;
