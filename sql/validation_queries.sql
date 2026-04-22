-- Verifica se o job rodou hoje (monitoramento de SLA)
SELECT COUNT(*) AS registros_hoje
FROM "db-glue-zoop"."zoop-glue-vendas_zoop_silver"
WHERE data = CURRENT_DATE - INTERVAL '1' DAY;  -- D-1 esperado

-- Checa categorias nulas remanescentes após imputação
SELECT
    produto,
    COUNT(*) AS qtd_nulos
FROM "db-glue-zoop"."zoop-glue-vendas_zoop_silver"
WHERE categoria_produto IS NULL
GROUP BY 1
ORDER BY qtd_nulos DESC;

-- Detecta UFs inválidas (fora do mapeamento)
SELECT uf_cliente, COUNT(*) AS qtd
FROM "db-glue-zoop"."zoop-glue-vendas_zoop_silver"
WHERE uf_cliente IS NULL
   OR LENGTH(uf_cliente) != 2
GROUP BY 1;

-- Valida consistência Bronze → Silver (contagem deve ser igual)
-- Rodar manualmente após cada execução do job
SELECT 'bronze' AS camada, COUNT(*) AS total FROM "db-glue-zoop"."zoop-glue-vendas_zoop_bronze_parquet"
UNION ALL
SELECT 'silver' AS camada, COUNT(*) AS total FROM "db-glue-zoop"."zoop-glue-vendas_zoop_silver";
