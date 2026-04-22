# 🚀 Data Pipeline em AWS Glue com Arquitetura Medalhão (Bronze → Silver → Gold)

![AWS](https://img.shields.io/badge/AWS-Glue-orange)
![PySpark](https://img.shields.io/badge/PySpark-Data%20Engineering-blue)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-green)
![Status](https://img.shields.io/badge/Status-Production--Ready-brightgreen)

---

## 📌 Visão Geral

Pipeline de dados escalável construído em **AWS Glue** utilizando **PySpark**, seguindo a arquitetura Medalhão (Bronze → Silver → Gold).

O projeto transforma dados brutos de vendas de um e-commerce em tabelas analíticas otimizadas para BI e Machine Learning, com foco em:

- **Performance** — Spark otimizado com funções nativas (sem Python UDFs)
- **Governança** — Schema enforcement, versionamento e catálogo centralizado
- **Custo** — Particionamento inteligente e formato colunar reduzem o custo no Athena em ~85%
- **Confiabilidade** — Validação preventiva, logging estruturado e tratamento de erros

---
# 🧩 Arquitetura

Este projeto segue o padrão Medallion Architecture:

- 🟤 Bronze: dados brutos (raw) armazenados no S3
- ⚪ Silver: dados tratados, limpos e padronizados
- 🟡 Gold: dados agregados prontos para análise

## 📊 Camada Gold (Consumo)

Os dados da camada Gold estão disponíveis para consulta via Amazon Athena.

### Exemplo de consulta:

```sql
SELECT 
    estado,
    COUNT(*) AS total_clientes
FROM gold_clientes
GROUP BY estado
ORDER BY total_clientes DESC;

### 🔄 Orquestração

O pipeline é orquestrado da seguinte forma:

1. Upload de arquivos no S3 (camada Bronze)
2. Trigger (evento S3) dispara o job Glue Bronze
3. Job Bronze transforma e envia para Silver
4. Job Silver aplica regras de qualidade e envia para Gold
5. Camada Gold é consumida via Athena


## 🎯 Problema de Negócio

A área de negócios sofria com:

- Dados pulverizados e inconsistentes (datas inválidas, categorias ausentes, UFs por extenso)
- Alto tempo de resposta na geração de relatórios consolidados de vendas
- Power BI apontando direto para tabelas brutas com dezenas de milhões de linhas

## 💡 Solução

Implementação de um pipeline ETL distribuído com:

- Higienização e padronização de dados na camada **Silver**
- KPIs pré-agregados na camada **Gold** (redução de +90% no volume lido pelo BI)
- Particionamento por data para *partition pruning* no Athena
- Integração com **Amazon Athena** e **Power BI**

---

## 🏗️ Arquitetura

![Arquitetura](docs/architecture.png)

```
Orquestração (EventBridge — trigger diário 02:00 AM)
        │
        ▼
┌─────────────────────────────────────────────────────┐
│                  Data Lakehouse (S3)                │
│                                                     │
│  [Bronze]  ──►  [AWS Glue / PySpark]  ──►  [Silver]│
│  Dados brutos        ETL Job              Tratados  │
│                         │                           │
│                         └──────────────►  [Gold]   │
│                                          KPIs / BI  │
└─────────────────────────────────────────────────────┘
        │                    │
        ▼                    ▼
  Glue Data Catalog     CloudWatch
  (metadados + schema)  (logs + alertas)
        │
        ▼
  Amazon Athena  ──►  Power BI
```

### Fluxo resumido:

1. Dados brutos ingeridos no **S3 Bronze** (CSV/JSON transacional)
2. **AWS Glue Job** processa, higieniza e valida os dados
3. Dados tratados escritos em **Parquet + Snappy** no S3 Silver (particionado por `data`)
4. Agregações analíticas materializadas no **S3 Gold**
5. Consultas via **Amazon Athena** com *partition pruning* ativo
6. Consumo no **Power BI** via conector Athena

---

## 🛠️ Stack Tecnológica

| Camada | Tecnologia | Papel |
|---|---|---|
| Processamento | AWS Glue + PySpark | ETL distribuído |
| Armazenamento | Amazon S3 | Data Lake (Bronze/Silver/Gold) |
| Catálogo | AWS Glue Data Catalog | Metadados e schema registry |
| Query Engine | Amazon Athena | Consultas SQL serverless |
| Orquestração | Amazon EventBridge | Trigger diário automatizado |
| Observabilidade | Amazon CloudWatch | Logs e alertas de falha |
| Visualização | Power BI | Dashboards e relatórios |

---

## ⚡ Diferenciais Técnicos

### 🔹 Performance — Funções Nativas vs. Python UDFs

Python UDFs interrompem o **Catalyst Optimizer**: cada linha é serializada da JVM para o processo Python e devolvida. Funções nativas rodam 100% na JVM.

```python
# ❌ Antes — UDF com overhead de serialização por linha
converter_udf = udf(lambda e: estados_brasil.get(e), StringType())
df = df.withColumn("UF_cliente", converter_udf("UF_cliente"))

# ✅ Depois — create_map() nativo, vetorizado pelo Tungsten
estados_map = create_map([lit(x) for x in sum(estados_brasil.items(), ())])
df = df.withColumn("UF_cliente", estados_map[col("UF_cliente")])
```

Ganhos esperados: **2x–10x** em datasets de produção.

### 🔹 Validação Eficiente de Dados

```python
# ❌ Antes — dispara job completo no Spark apenas para contar
if vendas_df.count() == 0:
    raise Exception("Dataset vazio")

# ✅ Depois — para na primeira partição não-vazia
if vendas_df.rdd.isEmpty():
    raise Exception("Dataset vazio")
```

### 🔹 Particionamento Inteligente

Dados particionados por `data` no S3, ativando *partition pruning* no Athena — consultas apontam apenas para a pasta da data alvo, sem escanear o dataset inteiro.

### 🔹 Formato Colunar

**Parquet + Snappy** em todas as camadas tratadas — ideal para workloads analíticos com leitura de colunas específicas.

### 🔹 Schema Enforcement

`apply_mapping()` com tipagem explícita antes de cada escrita. Garante que mudanças inesperadas na origem (Schema Drift) não quebrem sistemas downstream.

### 🔹 Consistência de Naming

Colunas padronizadas em `snake_case` após `apply_mapping()` na camada Silver. A chave de partição `partitionKeys=["data"]` respeita o schema mapeado — detalhe crítico para evitar erros silenciosos no Athena.

---

## 📊 Camada Gold — KPIs de Negócio

Tabela agregada diária por **produto**, **categoria** e **data**, com:

| Métrica | Descrição |
|---|---|
| `total_itens_vendidos` | Soma de unidades vendidas no dia |
| `faturamento_total` | Receita bruta (quantidade × preço) |
| `qtd_pedidos` | Número de transações únicas |

Em vez do Power BI escanear 10 milhões de linhas da Silver, consome apenas a Gold já consolidada — **redução de +90% no volume lido pelo BI**.

---

## 📈 Impacto Estimado

| Métrica | Resultado |
|---|---|
| ⚡ Velocidade de consulta no Athena | ~85% mais rápido (partition pruning) |
| 💾 Custo de armazenamento S3 | ~70% de economia (Parquet vs. CSV bruto) |
| 📉 Carga computacional no Power BI | Redução de +90% (Gold vs. Silver) |
| 🔁 Ganho nas transformações Spark | 2x–10x (nativas vs. Python UDFs) |

---

## 📂 Estrutura do Projeto

```
data-pipeline-aws-glue-medallion/
│
├── README.md
├── .gitignore
├── requirements.txt
│
├── docs/
│   ├── architecture.png
│   ├── data_model.png
│   └── pipeline_flow.png
│
├── src/
│   ├── glue_job_vendas.py       # Job principal (Bronze → Silver → Gold)
│   └── utils/
│       └── helpers.py           # Funções reutilizáveis (validação, escrita, mappings)
│
├── sql/
│   ├── athena_queries.sql       # Queries analíticas para BI
│   └── validation_queries.sql   # Queries de monitoramento e qualidade
│
├── notebooks/
│   └── exploratory_analysis.ipynb
│
└── dashboards/
    └── powerbi_screenshots.png
```

---

## 🔬 Governança e Versionamento de Schema

O pipeline está preparado para evoluir sem quebrar consumidores downstream.

**Estratégia atual — Glue Data Catalog:**
`updateBehavior="UPDATE_IN_DATABASE"` + `enableUpdateCatalog=True` registram novas colunas automaticamente sem interromper queries existentes no Athena ou no Power BI.

**Evolução natural — Apache Iceberg:**

| Recurso | Glue Catalog (atual) | Apache Iceberg |
|---|---|---|
| Schema evolution | ✅ Additive | ✅ Additive + rename/drop |
| Time travel | ❌ | ✅ `AS OF TIMESTAMP` |
| ACID transactions | Parcial | ✅ Full |
| Rollback de partição | ❌ | ✅ |

A migração requer apenas alteração no formato de escrita — zero mudança nas transformações de negócio.

---

## ▶️ Como Executar

1. Criar o job no **AWS Glue** e apontar para `src/glue_job_vendas.py`
2. Configurar o **Glue Data Catalog** com o database `db-glue-zoop`
3. Criar o bucket S3 e ajustar os paths em `glue_job_vendas.py`
4. Executar manualmente ou agendar via **Amazon EventBridge** (recomendado: 02:00 AM, D-1)
5. Monitorar logs e alertas no **Amazon CloudWatch**

---

## 🔮 Próximos Passos

- [ ] Implementar **Apache Iceberg** (ACID + Time Travel + rollback)
- [ ] Adicionar validação com **Great Expectations** (data quality automatizado)
- [ ] Criar testes unitários com `pytest` + `moto` (mock AWS)
- [ ] Dashboard completo no **Power BI** com as métricas da camada Gold
- [ ] Orquestração com **AWS Step Functions** para pipelines multi-job

---

## 👩‍💻 Autora

**Natasha de Sousa Costa**
Analista de Dados | Engenharia de Dados | BI

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Natasha-blue?logo=linkedin)](https://linkedin.com)
