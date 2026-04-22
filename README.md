# aws-glue-medallion-data-pipeline
 Pipeline de Dados em AWS Glue: Arquitetura Medalhão (Bronze → Silver → Gold)

## 📌 Contexto do Projeto

Este projeto implementa um pipeline de dados analítico escalável para o processamento de grandes volumes de informações de vendas de um e-commerce. O objetivo é transformar logs brutos transacionais em inteligência de negócio pronta para consumo (Business Intelligence e Machine Learning).

**Problema resolvido:** A área de negócios sofria com dados pulverizados, inconsistências de preenchimento (ausência de categorias, formatos de data inválidos) e extrema lentidão para geração de relatórios de vendas consolidados.

**Solução:** Foi desenhada uma arquitetura baseada em **Data Lakehouse**, utilizando **AWS Glue (PySpark)** para processamento distribuído. O fluxo segue o padrão **Medalhão (Bronze → Silver → Gold)**, garantindo higienização de dados, enriquecimento com funções nativas do Spark e a disponibilização de tabelas agregadas prontas para tomada de decisão no Power BI.

---

## 🏗️ Arquitetura e Orquestração

O projeto não se resume a um job isolado. A infraestrutura é planejada para execução recorrente e monitorada:

```
graph LR
    subgraph "Orquestração (EventBridge / AWS Step Functions)"
        CRON[Trigger Diário] --> GLUE
    end

    subgraph "Data Lakehouse (Amazon S3)"
        S3_B[Bronze / Dados Brutos] -->|Ingestão| GLUE
        GLUE[AWS Glue Jobs / PySpark] -->|Limpeza| S3_S[Silver / Dados Tratados]
        GLUE -->|Agregação| S3_G[Gold / KPIs para BI]
    end

    subgraph "Catálogo e Governança"
        GLUE -->|Updates| CATALOG[Glue Data Catalog]
        CATALOG -.-> ATHENA
        S3_S -.-> ATHENA
        S3_G -.-> ATHENA[Amazon Athena]
    end

    ATHENA --> PBI[Power BI]
    GLUE -->|Logs e Erros| CW[CloudWatch]
```

### ⚙️ Orquestração

O acionamento do pipeline é gerenciado conceitualmente por regras do **Amazon EventBridge**, disparando a rotina de ETL diariamente às 02:00 AM (D-1). Para pipelines mais complexos com múltiplas dependências, a arquitetura é extensível utilizando o **AWS Step Functions**.

---

## 🧠 Maturidade Técnica e Decisões Arquiteturais

### 1. Confiabilidade e Observabilidade

- **Validação de Dados:** O pipeline utiliza `rdd.isEmpty()` para validação preventiva do dataset. Essa abordagem é conscientemente preferida a `.count()`, pois evita disparar um job completo no Spark apenas para verificar se há dados — reduzindo custo computacional na inicialização.
- **Logging Estruturado:** Integrado com o CloudWatch nativo do AWS Glue. Qualquer erro durante as transformações é detectado em blocos `try/except` e logado, facilitando a vida da operação (DataOps).

### 2. Performance: Funções Nativas vs. Python UDFs

As transformações de lookup (conversão de UF e preenchimento de categorias) utilizam `create_map()` + `coalesce()` — funções nativas do Spark — em vez de Python UDFs.

**Por que isso importa:**
- Python UDFs interrompem o Catalyst Optimizer: os dados precisam ser serializados da JVM para o processo Python, processados linha a linha, e devolvidos à JVM.
- Funções nativas (`create_map`, `when`, `coalesce`) são executadas inteiramente na JVM, otimizadas pelo Catalyst, e vetorizadas pelo Tungsten.
- Em datasets de produção com milhões de linhas, essa diferença representa ganhos de performance de 2x a 10x.

```python
# ❌ Antes: UDF com overhead de serialização JVM ↔ Python por linha
converter_estado_udf = udf(lambda e: estados_brasil.get(e), StringType())
df = df.withColumn("UF_cliente", converter_estado_udf("UF_cliente"))

# ✅ Depois: create_map() nativo — executado 100% na JVM
estados_map_expr = create_map([lit(x) for x in sum(estados_brasil.items(), ())])
df = df.withColumn("UF_cliente", estados_map_expr[col("UF_cliente")])
```

### 3. Modelagem Analítica (Camada Gold)

Além de tratar os dados, o projeto agrega valor direto ao negócio criando tabelas materializadas na **Camada Gold**.

Em vez do Power BI precisar ler 10 milhões de linhas detalhadas de vendas (Silver), o pipeline entrega uma **Tabela Agregada Diária** contendo apenas a volumetria consolidada e faturamento por categoria e produto.

### 4. Engenharia de Performance (Otimização)

- **Particionamento por Data (`partitionKeys=["data"]`):** Isolamento de diretórios no S3. Permite o mecanismo de *Partition Pruning*, fazendo o Athena varrer apenas a data alvo da query.
- **Formato Parquet + Compressão Snappy:** Abordagem colunar ideal para Big Data analítico.
- **Schema Enforcement:** Mapeamento explícito de tipos antes da persistência para garantir que nenhum sistema downstream sofra com mudanças inesperadas no formato da origem (Schema Drift). A chave de partição `data` é mantida em snake_case consistente com o schema mapeado.

### 5. Versionamento e Governança de Schema (Schema Evolution)

O pipeline está preparado para evoluir sem quebrar consumidores downstream:

**Estratégia atual — Glue Data Catalog:**
O Glue Data Catalog atua como o catálogo central de metadados. A flag `updateBehavior="UPDATE_IN_DATABASE"` e `enableUpdateCatalog=True` garantem que novas colunas adicionadas na origem sejam registradas automaticamente no catálogo sem interromper queries existentes no Athena ou no Power BI.

**Evolução natural — Apache Iceberg ou Delta Lake:**
Para ambientes que exigem controle de versão de dados completo (time travel, rollback, ACID transactions em larga escala), a arquitetura é extensível para formatos de tabela abertos:

| Recurso | Glue Catalog (atual) | Apache Iceberg / Delta Lake |
|---|---|---|
| Schema evolution | ✅ Additive | ✅ Additive + Column rename/drop |
| Time travel | ❌ | ✅ `AS OF TIMESTAMP` |
| ACID transactions | Parcial | ✅ Full |
| Rollback de partição | ❌ | ✅ |
| Custo de adoção | Baixo | Médio |

A migração para Iceberg no AWS Glue requer apenas a alteração do formato de escrita:
```python
# Ativação de Iceberg no job (zero mudança no código de transformação)
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.aws.glue.GlueCatalog")
```

---

## 📈 Métricas de Impacto Estimadas

A implementação destas melhores práticas traz um impacto financeiro e operacional mensurável:

- **Redução no Tempo de Consulta (Athena):** ~85% mais rápido ao apontar para as tabelas particionadas (Silver/Gold) contra as tabelas Raw (Bronze).
- **Economia de Armazenamento:** Redução de ~70% nos custos de storage do Amazon S3 graças à mudança de formatos textuais brutos para **Parquet + Snappy**.
- **Custo Computacional de BI Minimizado:** O Power BI consome a tabela *Gold* (já agrupada), economizando capacidade de processamento do Gateway e CPU do serviço de BI.
- **Ganho de Performance nas Transformações:** Substituição de Python UDFs por funções nativas do Spark resulta em ganhos de 2x–10x em datasets de produção.

---

## 💻 Pipeline Principal (Job de Vendas)

O script principal `glue_job_vendas.py` realiza:

1. **Ingestão** a partir do Glue Data Catalog (Bronze)
2. **Validação** com `rdd.isEmpty()` (sem custo de job completo)
3. **Tratamento de datas** — concatenação de colunas separadas (`Ano`, `Mês`, `Dia`)
4. **Padronização de Estados** — `create_map()` nativo (sem UDF)
5. **Imputação de Categorias** — `coalesce()` + `create_map()` nativo (sem UDF)
6. **Schema Enforcement** via `apply_mapping()` com tipagem explícita
7. **Escrita Silver** — Parquet + Snappy, particionado por `data` (snake_case)
8. **Agregação Gold** — KPIs diários por produto e categoria
9. **Escrita Gold** — Parquet + Snappy, particionado por `Data`

### Convenções de Nomenclatura

| Camada | Padrão de Colunas | Partição |
|---|---|---|
| Bronze | PascalCase (origem) | — |
| Silver | snake_case (após `apply_mapping`) | `data` |
| Gold | snake_case | `Data` (pré-mapeamento) |

---

## 🔧 Melhorias Implementadas (Changelog)

| # | Problema | Solução |
|---|---|---|
| 1 | `.count()` disparava job completo para validação | Substituído por `rdd.isEmpty()` |
| 2 | Python UDFs com overhead de serialização JVM ↔ Python | Substituídas por `create_map()` + `coalesce()` nativos |
| 3 | `partitionKeys=["Data"]` inconsistente com schema snake_case na Silver | Corrigido para `partitionKeys=["data"]` |
| 4 | Ausência de estratégia de versionamento de schema | Documentada evolução para Apache Iceberg / Delta Lake |
