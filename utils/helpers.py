from pyspark.sql import DataFrame
from pyspark.sql.functions import create_map, lit, coalesce, col
from awsglue.context import GlueContext

# Dicionários de lookup centralizados aqui — um único lugar para manutenção
ESTADOS_BRASIL = {
    "Acre": "AC", "Alagoas": "AL", "Amazonas": "AM", # ... etc
}

PRODUTOS_CATEGORIAS = {
    'Smartphone': 'Eletrônicos', 'Geladeira': 'Eletrodomésticos', # ... etc
}

def validar_dataset(df: DataFrame, logger, nome: str) -> None:
    """Aborta o pipeline se o dataset estiver vazio."""
    if df.rdd.isEmpty():
        logger.error(f"Dataset '{nome}' vazio — abortando pipeline.")
        raise Exception(f"Dataset vazio: {nome}")

def converter_estados(df: DataFrame, coluna: str) -> DataFrame:
    """Converte nome completo do estado para sigla UF via create_map."""
    mapping = create_map([lit(x) for x in sum(ESTADOS_BRASIL.items(), ())])
    return df.withColumn(coluna, mapping[col(coluna)])

def imputar_categorias(df: DataFrame, col_produto: str, col_categoria: str) -> DataFrame:
    """Preenche categorias nulas com base no nome do produto."""
    mapping = create_map([lit(x) for x in sum(PRODUTOS_CATEGORIAS.items(), ())])
    return df.withColumn(col_categoria, coalesce(col(col_categoria), mapping[col(col_produto)]))

def escrever_camada(glueContext, df, path: str, partition_keys: list,
                    db: str, table: str, ctx: str):
    """Wrapper para escrita padronizada em S3 com atualização do Glue Catalog."""
    from awsglue.dynamicframe import DynamicFrame
    dyf = DynamicFrame.fromDF(df, glueContext, ctx)
    sink = glueContext.getSink(
        path=path, connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partition_keys, compression="snappy",
        enableUpdateCatalog=True, transformation_ctx=ctx,
    )
    sink.setCatalogInfo(catalogDatabase=db, catalogTableName=table)
    sink.setFormat("glueparquet")
    sink.writeFrame(dyf)
