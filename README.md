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

A implementação seguirá as tecnologias definidas no enunciado:

- Docker e Docker Compose
- MySQL
- LocalStack, simulando o Amazon S3
- Airbyte
- Apache Spark
- Hive Metastore
- Trino
- Python

## 4. Arquitetura medalhão

### Bronze

Camada de ingestão e armazenamento dos dados brutos provenientes das fontes.

Fontes previstas:
- MySQL
- Arquivos JSONL
- Arquivos CSV

Os dados brutos deverão ser preservados para rastreabilidade e reprocessamento.

### Silver

Camada de tratamento e qualidade dos dados, contemplando:
- Deduplicação
- Padronização de campos e formatos
- Tratamento de dados inválidos
- Validação de integridade
- Quarentena de registros rejeitados
- Relatórios de qualidade e rastreabilidade

### Gold

Camada analítica com dados organizados para consultas de negócio, utilizando modelagem dimensional, fatos, dimensões e agregações.

As tabelas analíticas serão disponibilizadas para consultas SQL por meio do Trino.

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

## 6. Execução

Os comandos de instalação, inicialização, execução do pipeline, testes e encerramento serão documentados após a implementação e validação de cada etapa.

## 7. Qualidade e observabilidade

O projeto deverá registrar métricas de processamento, duração das execuções, quantidade de registros lidos, aprovados e rejeitados, motivos de rejeição e erros encontrados.

## 8. Histórico de desenvolvimento

As etapas serão registradas por meio de commits no Git e publicadas no GitHub, permitindo acompanhar a evolução do projeto.

| Etapa | Status |
|---|---|
| Validação do ambiente Windows | Concluída |
| Estrutura inicial de diretórios | Concluída |
| Configuração inicial do Docker Compose | Em preparação |
| Inicialização e validação dos serviços | Pendente |
| Ingestão Bronze com Airbyte | Pendente |
| Processamento e qualidade Silver | Pendente |
| Modelagem analítica Gold | Pendente |
| Consultas de negócio no Trino | Pendente |
| Testes, documentação final e entrega | Pendente |

## 9. Observações

Este é o documento inicial do projeto. A arquitetura, os procedimentos de execução, os resultados analíticos e as evidências de validação serão detalhados conforme o desenvolvimento avançar.
