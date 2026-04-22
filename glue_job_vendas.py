import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import (
    concat_ws, to_date, to_timestamp,
    col, sum as _sum, count as _count,
    create_map, lit, coalesce
)
from pyspark.sql.types import StringType

# ---------------------------------------------------------
# 1. Configuração e Inicialização
# ---------------------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configurando Logger nativo do Glue (envia logs para o CloudWatch)
logger = glueContext.get_logger()
logger.info("Iniciando Job de ETL: Vendas Bronze -> Silver -> Gold")

try:
    # ---------------------------------------------------------
    # 2. Ingestão de Dados (Camada Bronze)
    # ---------------------------------------------------------
    vendas_zoop_dyf = glueContext.create_dynamic_frame.from_catalog(
        database='db-glue-zoop',
        table_name='zoop-glue-vendas_zoop_bronze_parquet'
    )
    vendas_zoop_df = vendas_zoop_dyf.toDF()

    # ✅ FIX 1: Substituído .count() por .rdd.isEmpty() para evitar
    # a execução de um job completo no Spark apenas para validação.
    # Impacto: redução de custo computacional significativa na inicialização.
    if vendas_zoop_df.rdd.isEmpty():
        logger.error("Dataset vazio - abortando pipeline para evitar falhas ou dados nulos no downstream.")
        raise Exception("Dataset vazio - abortando pipeline")

    logger.info("Dataset carregado com sucesso. Iniciando transformações.")

    # ---------------------------------------------------------
    # 3. Transformação e Higienização (Camada Silver)
    # ---------------------------------------------------------

    # 3.1 Tratamento de datas e horas
    vendas_zoop_df = vendas_zoop_df.withColumn("Data", concat_ws("-", "Ano", "Mês", "Dia"))
    vendas_zoop_df = vendas_zoop_df.withColumn("Data", to_date("Data"))
    vendas_zoop_df = vendas_zoop_df.withColumn("Horario", to_timestamp("Horario", "HH:mm:ss"))
    vendas_zoop_df = vendas_zoop_df.drop("Dia", "Mês", "Ano")

    # 3.2 Padronização de Estados Brasileiros
    # ✅ FIX 2: Substituída Python UDF por create_map() nativo do Spark.
    # UDFs Python "quebram" o Catalyst Optimizer (não são vetorizadas),
    # gerando serialização/desserialização row-by-row via JVM ↔ Python.
    # create_map() é executada inteiramente na JVM — mais rápida e sem overhead.
    estados_brasil = {
        "Acre": "AC", "Alagoas": "AL", "Amazonas": "AM", "Amapá": "AP",
        "Bahia": "BA", "Ceará": "CE", "Distrito Federal": "DF", "Espírito Santo": "ES",
        "Goiás": "GO", "Maranhão": "MA", "Minas Gerais": "MG", "Mato Grosso do Sul": "MS",
        "Mato Grosso": "MT", "Pará": "PA", "Paraíba": "PB", "Pernambuco": "PE",
        "Piauí": "PI", "Paraná": "PR", "Rio de Janeiro": "RJ", "Rio Grande do Norte": "RN",
        "Rondônia": "RO", "Roraima": "RR", "Rio Grande do Sul": "RS",
        "Santa Catarina": "SC", "Sergipe": "SE", "São Paulo": "SP", "Tocantins": "TO"
    }

    estados_map_expr = create_map([lit(x) for x in sum(estados_brasil.items(), ())])
    vendas_zoop_df = vendas_zoop_df.withColumn(
        "UF_cliente",
        estados_map_expr[col("UF_cliente")]
    )

    # 3.3 Tratamento de dados faltantes (Imputação de Categorias)
    # ✅ FIX 2 (continuação): Substituída Python UDF por create_map() + coalesce().
    # Mesma justificativa: operações nativas do Spark são otimizadas pelo Catalyst
    # e evitam o custo de serialização Python ↔ JVM por linha.
    produtos_categorias = {
        'Smart TV 55"': 'Eletrônicos', 'Frigobar': 'Eletrodomésticos',
        'Ventilador de teto': 'Eletrodomésticos', 'Cafeteira': 'Eletrodomésticos',
        'Smartphone': 'Eletrônicos', 'Liquidificador': 'Eletrodomésticos',
        'Notebook': 'Eletrônicos', 'Tablet': 'Eletrônicos',
        'Micro-ondas': 'Eletrodomésticos', 'Aspirador de pó': 'Eletrodomésticos',
        'Câmera digital': 'Eletrônicos', 'Chuveiro elétrico': 'Eletrodomésticos',
        'Fone de ouvido': 'Eletrônicos', 'Ventilador de mesa': 'Eletrodomésticos',
        'Impressora': 'Eletrônicos', 'Secador de cabelo': 'Eletrodomésticos',
        'Relógio inteligente': 'Eletrônicos', 'Batedeira': 'Eletrodomésticos',
        'Máquina de lavar roupa': 'Eletrodomésticos', 'Ferro de passar roupa': 'Eletrodomésticos',
        'Cafeteira expresso': 'Eletrodomésticos', 'Aparelho de som': 'Eletrônicos',
        'Geladeira': 'Eletrodomésticos', 'Forno elétrico': 'Eletrodomésticos',
        'TV Box': 'Eletrônicos', 'Panela elétrica': 'Eletrodomésticos',
        'Ventilador de coluna': 'Eletrodomésticos', 'Câmera de segurança': 'Eletrônicos',
        'Fritadeira elétrica': 'Eletrodomésticos', 'Máquina de café': 'Eletrodomésticos'
    }

    categorias_map_expr = create_map([lit(x) for x in sum(produtos_categorias.items(), ())])
    vendas_zoop_df = vendas_zoop_df.withColumn(
        "Categoria_produto",
        coalesce(col("Categoria_produto"), categorias_map_expr[col("Produto")])
    )

    # ---------------------------------------------------------
    # 4. Escrita na Camada Silver (Detalhada)
    # ---------------------------------------------------------
    vendas_zoop_dyf = DynamicFrame.fromDF(vendas_zoop_df, glueContext)

    # Schema Enforcement
    vendas_zoop_dyf_mapeado = vendas_zoop_dyf.apply_mapping(
        mappings=[
            ("ID_venda",          "long",      "id_venda",          "long"),
            ("Data",              "date",      "data",              "date"),
            ("Horario",           "timestamp", "horario",           "timestamp"),
            ("Canal_venda",       "string",    "canal_venda",       "string"),
            ("Origem_venda",      "string",    "origem_venda",      "string"),
            ("ID_produto",        "long",      "id_produto",        "long"),
            ("Produto",           "string",    "produto",           "string"),
            ("Categoria_produto", "string",    "categoria_produto", "string"),
            ("Preco_unitario",    "double",    "preco_unitario",    "double"),
            ("Quantidade",        "long",      "quantidade",        "int"),
            ("Metodo_pagamento",  "string",    "metodo_pagamento",  "string"),
            ("ID_cliente",        "long",      "id_cliente",        "long"),
            ("Nome_cliente",      "string",    "nome_cliente",      "string"),
            ("Genero_cliente",    "string",    "genero_cliente",    "string"),
            ("Idade_cliente",     "long",      "idade_cliente",     "int"),
            ("Cidade_cliente",    "string",    "cidade_cliente",    "string"),
            ("UF_cliente",        "string",    "uf_cliente",        "string"),
            ("Regiao_cliente",    "string",    "regiao_cliente",    "string"),
            ("Avaliacao",         "long",      "avaliacao",         "int"),
        ]
    )

    logger.info("Gravando dados na camada Silver particionados por data...")

    s3_silver_output = glueContext.getSink(
        path="s3://nome-do-bucket/silver/vendas/",
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["data"],          # ✅ FIX 3: snake_case consistente com o schema mapeado acima
        compression="snappy",
        enableUpdateCatalog=True,
        transformation_ctx="s3_silver_output",
    )
    s3_silver_output.setCatalogInfo(
        catalogDatabase="db-glue-zoop",
        catalogTableName="zoop-glue-vendas_zoop_silver"
    )
    s3_silver_output.setFormat("glueparquet")
    s3_silver_output.writeFrame(vendas_zoop_dyf_mapeado)

    # ---------------------------------------------------------
    # 5. Modelagem Analítica (Camada Gold)
    # ---------------------------------------------------------
    logger.info("Gerando KPIs agregados para a camada Gold...")

    # Tabela Agregada: Vendas e Faturamento por Produto e Data
    # Reduz o volume de dados escaneados no BI em mais de 90%
    df_gold_vendas = vendas_zoop_df.groupBy("Data", "Categoria_produto", "Produto").agg(
        _sum("Quantidade").alias("total_itens_vendidos"),
        _sum(col("Quantidade") * col("Preco_unitario")).alias("faturamento_total"),
        _count("ID_venda").alias("qtd_pedidos")
    )

    dyf_gold = DynamicFrame.fromDF(df_gold_vendas, glueContext)

    logger.info("Gravando dados agregados na camada Gold...")

    s3_gold_output = glueContext.getSink(
        path="s3://nome-do-bucket/gold/vendas_kpi_diario/",
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["Data"],          # Gold mantém "Data" (campo pré-mapeamento; ajustar se apply_mapping for aplicado aqui também)
        compression="snappy",
        enableUpdateCatalog=True,
        transformation_ctx="s3_gold_output",
    )
    s3_gold_output.setCatalogInfo(
        catalogDatabase="db-glue-zoop",
        catalogTableName="zoop-glue-vendas_kpi_gold"
    )
    s3_gold_output.setFormat("glueparquet")
    s3_gold_output.writeFrame(dyf_gold)

    job.commit()
    logger.info("Pipeline concluído com sucesso!")

except Exception as e:
    # Tratamento de exceção garante que a falha seja registrada visivelmente
    logger.error(f"Erro fatal durante a execução do job: {str(e)}")
    raise e
