# Projeto de Engenharia de Dados — Arquitetura Medalhão

## 1. Visão geral

Este projeto implementa um pipeline de engenharia de dados com arquitetura medalhão (Bronze, Silver e Gold), simulando localmente serviços de uma infraestrutura de dados em nuvem.

O objetivo é ingerir dados de diferentes fontes, preservar os dados brutos, aplicar regras de qualidade, construir um modelo analítico e responder a perguntas de negócio.

O desenvolvimento é incremental: cada etapa é considerada concluída somente após sua execução e validação no ambiente local.

## 2. Objetivos de negócio

O pipeline deverá responder às seguintes perguntas:

1. Qual é o ticket médio mensal por categoria e qual categoria apresenta o maior crescimento?
2. Como se comporta o funil de conversão (`product_view` → `add_to_cart` → `checkout_start` → `purchase`) por tipo de dispositivo?
3. Quais são os dez produtos com maior receita e os dez com maior margem?
4. Como se comporta a retenção de clientes por coorte do mês da primeira compra?
5. Qual é o ROAS por canal de mídia?
6. Quantos pedidos ou registros relacionados a pedidos foram rejeitados na Silver, por motivo, e qual receita potencial está associada a eles?

## 3. Tecnologias

- Docker e Docker Compose
- MySQL 8
- LocalStack, simulando o Amazon S3
- Airbyte OSS
- Apache Spark / PySpark
- Python
- Parquet, JSONL e CSV

Tecnologias planejadas para etapas posteriores incluem o mecanismo de consultas SQL, a orquestração e a camada de visualização analítica.

## 4. Arquitetura medalhão

### 4.1 Bronze — ingestão de dados

A camada Bronze preserva os dados brutos para rastreabilidade e reprocessamento.

Fontes trabalhadas:

- MySQL: banco `techflow_oltp`
- JSONL: eventos de clickstream
- CSV: custos de produtos e investimento em mídia

Destino: LocalStack, compatível com a API do Amazon S3.

Bucket principal: `techflow-bronze`.

#### MySQL

A ingestão MySQL → Airbyte → LocalStack foi executada manualmente pela interface do Airbyte.

| Tabela | Registros reportados |
|---|---:|
| `clientes` | 5.000 |
| `enderecos` | 5.000 |
| `itens_pedido` | 99.938 |
| `pedidos` | 40.000 |
| `produtos` | 200 |

Os arquivos JSONL das cinco tabelas foram encontrados no bucket Bronze.

#### JSONL e CSV

Os arquivos de clickstream em JSONL e os arquivos `custos_produtos.csv` e `investimento_midia.csv` foram carregados na Bronze.

Também foram gerados arquivos Parquet na Bronze para facilitar o processamento com Spark.

Resultados validados:

| Fonte | Registros processados |
|---|---:|
| Clickstream | 300.000 |
| Custos de produtos | 501 |
| Investimento em mídia | 90 |

**Status Bronze:** ingestão inicial das três fontes validada. A configuração reproduzível e a automação de todas as ingestões ainda precisam ser concluídas.

### 4.2 Silver — tratamento e qualidade

A camada Silver utiliza Spark para padronizar dados, converter tipos, aplicar regras de qualidade e separar registros válidos dos rejeitados.

Os dados válidos são gravados em Parquet no bucket `techflow-silver`. Os registros rejeitados são preservados em caminhos de auditoria, quando aplicável.

#### Processamentos implementados e validados

| Fonte | Registros válidos | Registros rejeitados |
|---|---:|---:|
| `itens_pedido` | 97.965 | 1.973 |
| `pedidos` | 40.000 | 0 |
| `produtos` | 200 | 0 |
| `clientes` | 5.000 | 0 |
| `enderecos` | 5.000 | 0 |
| `clickstream` | 300.000 | 0 |
| `custos_produtos` | 500 | 1 |
| `investimento_midia` | 90 | 0 |
| **Total** | **448.755** | **1.974** |

Os totais representam registros processados nas oito fontes e não pedidos únicos.

#### Regras de qualidade validadas

- `itens_pedido`: rejeição de preços nulos e negativos.
  - 979 registros com preço nulo.
  - 994 registros com preço negativo.
- `custos_produtos`: rejeição da linha de totalização (`sku = TOTAL`), que não representa um produto individual.
- `pedidos`: 40.000 registros válidos, sem rejeições nas regras implementadas.
- `produtos`, `clientes` e `enderecos`: registros processados sem rejeições nas regras implementadas.
- `clickstream`: 300.000 eventos processados sem rejeições nas regras implementadas.
- `investimento_midia`: 90 registros processados sem rejeições nas regras implementadas.

Os dados válidos e os registros rejeitados foram gravados em Parquet no LocalStack, permitindo continuidade do processamento e auditoria.

**Status Silver:** transformações das oito fontes executadas e validadas. As regras implementadas cobrem os tratamentos descritos acima; isso não significa que todas as possíveis regras de qualidade estejam concluídas.

### 4.3 Gold — modelagem analítica

A camada Gold deverá organizar os dados em um modelo dimensional para responder às seis perguntas de negócio.

A modelagem deverá considerar dimensões e fatos relacionados a produtos, clientes, datas, vendas, eventos de navegação e investimento em mídia.

As métricas comerciais deverão considerar os status de pedido definidos para vendas efetivas (`pago`, `enviado` e `entregue`). Pedidos cancelados e pendentes deverão permanecer disponíveis para análises apropriadas, mas não deverão compor as métricas de vendas efetivas.

**Status Gold:** pendente de implementação.

## 5. Organização do projeto

A estrutura principal inclui:

```text
pipeline-medalhao/
├── airbyte/
├── analytics/
├── data_generator/
├── infra/
├── jobs/
│   └── silver/
├── landing/
├── logs/
├── pipeline/
├── quality/
├── tests/
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```

A estrutura real pode conter outros arquivos auxiliares e scripts de validação.

## 6. Execução e validação

### 6.1 Serviços locais

O Docker Compose é utilizado para executar o MySQL, o LocalStack e o ambiente Spark.

O Airbyte foi instalado separadamente por meio do `abctl`, e a sincronização inicial MySQL → Bronze foi executada pela interface do Airbyte.

### 6.2 Conferir os objetos da Bronze

No PowerShell:

```powershell
docker exec medalhao-localstack awslocal s3 ls s3://techflow-bronze/ --recursive
```

### 6.3 Conferir os objetos da Silver

Para listar os arquivos da camada Silver:

```powershell
docker exec medalhao-localstack awslocal s3 ls s3://techflow-silver/ --recursive
```

Os diretórios de saída contêm arquivos Parquet e marcadores de conclusão do Spark, conforme validado nas execuções realizadas.

### 6.4 Executar os jobs

Os scripts de processamento estão organizados em `jobs/` e `jobs/silver/`.

A execução de cada job depende dos serviços Docker e dos dados de entrada disponíveis no LocalStack. Os comandos operacionais específicos deverão ser mantidos junto aos respectivos scripts ou documentados quando a orquestração for implementada.

## 7. Qualidade e auditoria

A Silver já separa registros válidos e rejeitados para as fontes tratadas. As quantidades e os motivos de rejeição são registrados nos resultados dos jobs e preservados nos caminhos de auditoria.

Ainda será necessário consolidar essas informações em uma solução de observabilidade e em uma estrutura analítica que permita responder integralmente à pergunta de auditoria da camada Gold.

## 8. Histórico de desenvolvimento

| Etapa | Status |
|---|---|
| Validação do ambiente Windows e Docker | Concluída |
| Estrutura inicial do projeto | Concluída |
| Configuração inicial de MySQL e LocalStack | Concluída |
| Instalação e validação inicial do Airbyte | Concluída |
| Ingestão inicial de MySQL para Bronze | Concluída e validada |
| Carga de arquivos JSONL e CSV na Bronze | Concluída e validada |
| Conversão de fontes CSV e clickstream para Parquet na Bronze | Concluída e validada |
| Processamento Silver das oito fontes | Concluído e validado |
| Separação de registros rejeitados para auditoria | Implementada nas regras descritas |
| Modelagem dimensional Gold | Pendente |
| Respostas analíticas às seis perguntas de negócio | Pendente |
| Orquestração reproduzível de todas as ingestões e jobs | Pendente |
| Disponibilização de consultas SQL e visualização analítica | Pendente |
| Testes integrados e documentação final | Pendente |

## 9. Próximas etapas

1. Revisar e versionar os scripts e a documentação da Bronze e da Silver.
2. Implementar a modelagem dimensional Gold.
3. Desenvolver e validar as métricas para as seis perguntas de negócio.
4. Consolidar a auditoria e os indicadores de qualidade.
5. Automatizar a execução e as dependências do pipeline.
6. Implementar a camada de consulta SQL e a visualização analítica.
7. Validar a reprodução do ambiente e documentar a entrega final.

## 10. Segurança e versionamento

- Não versionar `.env`, senhas, tokens ou credenciais.
- Não versionar arquivos locais de configuração que contenham credenciais ou dados sensíveis.
- Preservar os dados brutos da Bronze para rastreabilidade e reprocessamento.
- Registrar a evolução por meio de commits pequenos e descritivos.
- Considerar uma etapa concluída somente após execução e validação no ambiente local.
