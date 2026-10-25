# Projeto de Engenharia de Dados — Arquitetura Medalhão

## 1. Visão geral

Este projeto tem como objetivo desenvolver um pipeline de engenharia de dados utilizando uma arquitetura medalhão, simulando localmente uma infraestrutura de dados em nuvem.

O pipeline será responsável pela ingestão, processamento, qualidade, modelagem e disponibilização dos dados para análises de negócio.

## 2. Objetivos de negócio

O projeto deverá responder às seguintes perguntas:

1. Qual é o ticket médio mensal por categoria e qual categoria apresenta o maior crescimento?
2. Como se comporta o funil de conversão (`product_view` → `add_to_cart` → `checkout_start` → `purchase`) por tipo de dispositivo?
3. Quais são os dez produtos com maior receita e os dez com maior margem?
4. Como se comporta a retenção de clientes por coorte do mês da primeira compra?
5. Qual é o ROAS por canal de mídia?
6. Quantos pedidos foram rejeitados na camada Silver, quais foram os motivos e qual receita potencial está associada a eles?

## 3. Tecnologias

Tecnologias previstas para o projeto:

- Docker e Docker Compose
- MySQL
- LocalStack, simulando o Amazon S3
- Airbyte
- Apache Spark
- Hive Metastore
- Trino
- Python

## 4. Arquitetura medalhão

### 4.1 Bronze — ingestão de dados

A camada Bronze armazena os dados brutos provenientes das fontes, preservando-os para rastreabilidade e reprocessamento.

Fontes previstas:

- MySQL
- Arquivos JSONL
- Arquivos CSV

**Implementação validada:**

- Fonte MySQL: `techflow_oltp`
- Conector Airbyte: MySQL
- Destino: S3 compatível do LocalStack
- Bucket: `techflow-bronze`
- Prefixo dos arquivos: `mysql/techflow_oltp/`
- Formato de saída: JSON Lines (JSONL)

A primeira sincronização foi concluída com cinco tabelas:

| Tabela | Registros reportados pelo Airbyte |
|---|---:|
| `clientes` | 5.000 |
| `enderecos` | 5.000 |
| `itens_pedido` | 99.938 |
| `pedidos` | 40.000 |
| `produtos` | 200 |

Os arquivos das cinco tabelas foram encontrados no bucket `techflow-bronze` por meio do comando `awslocal`.

**Status:** ingestão inicial MySQL → Airbyte → LocalStack validada. A validação de outras fontes e a automação recorrente ainda estão pendentes.

### 4.2 Silver — tratamento e qualidade

A camada Silver deverá contemplar:

- Deduplicação
- Padronização de campos e formatos
- Tratamento de dados inválidos
- Validação de integridade
- Quarentena de registros rejeitados
- Relatórios de qualidade e rastreabilidade

**Status:** pendente de implementação.

### 4.3 Gold — modelagem analítica

A camada Gold deverá organizar os dados para consultas de negócio, utilizando modelagem dimensional, fatos, dimensões e agregações.

As tabelas analíticas serão disponibilizadas para consultas SQL por meio do Trino.

**Status:** pendente de implementação.

## 5. Organização do projeto

```text
pipeline-medalhao/
├── infra/
├── data_generator/
├── airbyte/
├── jobs/
│   ├── silver/
│   └── gold/
├── pipeline/
├── analytics/
├── tests/
├── landing/
├── logs/
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```

## 6. Execução e validação da Bronze

### 6.1 Serviços locais

O ambiente local utiliza Docker Compose para executar o MySQL e o LocalStack. O Airbyte é executado localmente por meio do `abctl`.

### 6.2 Conferir os arquivos no bucket

No PowerShell, execute:

```powershell
docker exec medalhao-localstack awslocal s3 ls s3://techflow-bronze/ --recursive
```

O comando lista os objetos gravados no bucket Bronze.

### 6.3 Sincronização

A sincronização inicial foi executada manualmente pela interface do Airbyte. O resultado reportou as cinco tabelas como sincronizadas, e os respectivos arquivos JSONL foram encontrados no LocalStack.

A execução de novas sincronizações e a validação automatizada dos dados deverão ser documentadas nas próximas etapas.

## 7. Qualidade e observabilidade

O projeto deverá registrar métricas de processamento, duração das execuções, quantidade de registros lidos, aprovados e rejeitados, motivos de rejeição e erros encontrados.

**Status:** estrutura de métricas e relatórios ainda pendente de implementação.

## 8. Histórico de desenvolvimento

| Etapa | Status |
|---|---|
| Validação do ambiente Windows | Concluída |
| Estrutura inicial de diretórios | Concluída |
| Configuração inicial do Docker Compose para MySQL e LocalStack | Concluída |
| Inicialização e validação de MySQL e LocalStack | Concluída |
| Instalação e validação inicial do Airbyte | Concluída |
| Ingestão inicial MySQL → Bronze no LocalStack | Concluída e validada |
| Ingestão de outras fontes JSONL e CSV | Pendente |
| Processamento e qualidade Silver com Spark | Pendente |
| Modelagem analítica Gold | Pendente |
| Disponibilização de consultas no Trino | Pendente |
| Testes, documentação final e entrega | Pendente |

## 9. Observações

Este documento registra o estado atual do projeto. As etapas são consideradas concluídas somente após execução e validação no ambiente local.

Os dados brutos da Bronze devem ser preservados para permitir rastreabilidade e reprocessamento. Credenciais, arquivos `.env`, backups locais e dados brutos não devem ser adicionados ao Git.

A evolução do projeto será registrada por meio de commits no Git e publicada no GitHub.