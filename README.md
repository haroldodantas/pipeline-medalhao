
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
- Git e GitHub

Tecnologias planejadas para etapas posteriores incluem o mecanismo de consultas SQL, a orquestração completa e a camada de visualização analítica.

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

A ingestão MySQL → Airbyte → LocalStack foi executada pela interface do Airbyte.

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
| `clickstream` | 291.000 | 9.000 |
| `custos_produtos` | 500 | 1 |
| `investimento_midia` | 90 | 0 |
| **Total** | **439.755** | **10.974** |

Os totais representam registros processados nas oito fontes e não pedidos únicos.

#### Regras de qualidade validadas

- `itens_pedido`: rejeição de preços nulos e negativos.
  - 979 registros com preço nulo.
  - 994 registros com preço negativo.
- `custos_produtos`: rejeição da linha de totalização (`sku = TOTAL`).
- `pedidos`: 40.000 registros válidos, sem rejeições nas regras implementadas.
- `produtos`, `clientes` e `enderecos`: registros processados sem rejeições nas regras implementadas.
- `clickstream`: 291.000 eventos válidos e 9.000 eventos rejeitados por duplicidade de `event_id`.
- `investimento_midia`: 90 registros processados sem rejeições nas regras implementadas.

No Clickstream, a nova geração produz jornadas associadas a sessões e pedidos reais. A deduplicação é realizada na Silver, utilizando `event_id`, e os eventos duplicados são enviados para auditoria com o motivo `event_id_duplicado`.

Os dados válidos e os registros rejeitados foram gravados em Parquet no LocalStack.

**Status Silver:** transformações das oito fontes executadas e validadas. As regras implementadas cobrem os tratamentos descritos acima; isso não significa que todas as possíveis regras de qualidade estejam concluídas.

### 4.3 Gold — modelagem analítica

A camada Gold organiza os dados em um modelo dimensional para responder às perguntas de negócio.

#### Dimensões implementadas

| Dimensão | Registros | Descrição |
|---|---:|---|
| `dim_cliente` | 5.000 | Dados dos clientes e chave substituta |
| `dim_produto` | 200 | Dados dos produtos, categorias e chave substituta |
| `dim_tempo` | 91 | Calendário de 14/07/2026 a 12/10/2026 |

#### Fato de vendas

A tabela `fato_vendas` possui granularidade de item de pedido.

Regras comerciais implementadas:

- Considerar pedidos com status `pago`, `enviado` e `entregue` como receita realizada.
- Excluir pedidos `cancelado` e `pendente` das métricas de vendas efetivas.
- Calcular receita bruta, descontos e receita líquida.
- Não incluir frete na receita dos produtos.
- Relacionar o custo do produto à data da venda, respeitando sua vigência.
- Calcular custo e margem somente quando existir exatamente um registro de custo aplicável.

Resultado da execução:

| Indicador | Resultado |
|---|---:|
| Itens na fato de vendas | 58.777 |
| Receita bruta | R$ 471.918.961,56 |
| Descontos | R$ 17.746.251,30 |
| Receita líquida | R$ 454.172.710,26 |
| Itens com custo válido | 17.135 |
| Itens com custo ambíguo | 11.208 |
| Itens fora da vigência do custo | 21.693 |
| Itens sem cadastro de custo | 8.741 |

A cobertura de receita com custo válido é de aproximadamente 29,25%.

**Limitação importante:** existem intervalos de vigência de custos sobrepostos e produtos sem cadastro de custo. Por isso, a margem calculada não representa toda a operação. Os itens sem custo único válido permanecem na fato, mas sem custo e margem calculados. Não são atribuídos custos estimados.

**Status Gold:** dimensões básicas e fato de vendas implementadas e executadas. A implementação completa do modelo dimensional previsto no exercício, incluindo SCD2 funcional, dimensão de canal e demais estruturas analíticas, ainda precisa ser avaliada e concluída.

## 5. Análises de negócio

Cada questão deve apresentar consulta, resultado numérico, gráfico e interpretação.

### 5.1 Q1 — Ticket médio mensal por categoria

**Status:** concluída e executada.

Critérios utilizados:

- Receita líquida dos pedidos realizados.
- Contagem de pedidos distintos por mês e categoria.
- Ticket médio calculado pela divisão da receita líquida pela quantidade de pedidos distintos naquela categoria.
- Comparação de crescimento entre julho e setembro de 2026, ambos meses completos.

Resultado principal:

| Categoria | Crescimento do ticket médio |
|---|---:|
| Áudio | +4,26% |
| Periféricos | +4,13% |
| Acessórios | +1,79% |
| Games | +0,77% |
| Informática | -0,34% |
| Celulares | -2,54% |

**Interpretação:** a categoria Áudio apresentou o maior crescimento do ticket médio entre julho e setembro de 2026, com alta de 4,26%.

Outubro aparece na série mensal, mas é um período parcial, com dados disponíveis até 12/10/2026. Por isso, não foi utilizado na comparação de crescimento.

Arquivos gerados:

- `analytics/queries/q1_ticket_medio.py`
- `analytics/results/q1_ticket_medio_mensal.csv`
- `analytics/results/q1_crescimento_categoria.csv`
- `analytics/results/q1_ticket_medio_mensal.png`

### 5.2 Q2 — Funil de conversão por dispositivo

**Status:** consulta, resultados e validação implementados e executados.

A análise considera as etapas em sequência cronológica por sessão:

`product_view` → `add_to_cart` → `checkout_start` → `purchase`

Resultados por dispositivo:

| Indicador | Desktop | Mobile | Tablet |
|---|---:|---:|---:|
| Visualizações | 19.666 | 19.842 | 19.513 |
| Adições ao carrinho | 12.998 | 13.116 | 12.907 |
| Checkouts | 9.673 | 9.729 | 9.619 |
| Compras | 8.045 | 8.049 | 7.927 |
| Conversão visualização → carrinho | 66,09% | 66,10% | 66,15% |
| Conversão carrinho → checkout | 74,42% | 74,18% | 74,53% |
| Conversão checkout → compra | 83,17% | 82,73% | 82,41% |
| Conversão total | 40,91% | 40,57% | 40,62% |

Validação da associação entre eventos de compra e pedidos Silver:

- 24.021 sessões com compra.
- 24.021 `pedido_id` distintos referenciados.
- 24.021 pedidos encontrados.
- 24.021 pedidos com status elegível (`pago`, `enviado` ou `entregue`).
- Zero pedidos sem correspondência.

**Interpretação:** o desktop apresentou a maior conversão total (40,91%), mas a diferença entre dispositivos é pequena. A maior perda proporcional ocorre entre visualização e adição ao carrinho. A conversão de checkout para compra ficou acima de 82% nos três dispositivos.

Arquivos gerados:

- `analytics/queries/diagnostico_q2.py`
- `analytics/queries/q2_funil_conversao.py`
- `analytics/results/q2_funil_por_dispositivo.csv`
- `analytics/results/q2_validacao_pedidos.csv`
- `analytics/results/q2_funil_conversao.png`

A geração do Clickstream foi ajustada para produzir jornadas por sessão e associar eventos `purchase` a pedidos reais. O teste `tests/validar_clickstream.py` valida a quantidade de eventos e identificadores repetidos antes do processamento Silver.


### 5.3 Q3 — Top 10 produtos por receita versus margem

**Status:** consulta, resultados e gráfico implementados e executados.

A análise compara:

- Os dez produtos com maior receita líquida.
- Os dez produtos com maior margem bruta calculada.
- Os produtos presentes nos dois rankings.
- A cobertura dos custos utilizados no cálculo da margem.

Resultados principais:

- Apenas dois produtos aparecem simultaneamente nos dois rankings:
  - SKU-0052 — BytePro Produto 52.
  - SKU-0176 — SmartWave Produto 176.
- O produto SKU-0073 apresentou a maior receita líquida, com R$ 2.789.487,49.
- O produto SKU-0038 apresentou a maior margem bruta, com R$ 1.908.422,67.
- O ranking de receita considera todos os itens de vendas realizadas.
- O ranking de margem considera somente itens com custo válido e único.
- A cobertura de receita com custo válido é de aproximadamente 29,25%.

**Interpretação:** os produtos que mais geram receita não são necessariamente os que mais geram margem. O SKU-0073 lidera em receita, mas não aparece entre os dez maiores em margem. Já o SKU-0038 lidera em margem, embora não esteja entre os dez maiores em receita.

A análise de margem é parcial devido às limitações identificadas no cadastro e na vigência dos custos. Portanto, os resultados não representam a rentabilidade de toda a operação.

Arquivos gerados:

- `analytics/queries/q3_top_produtos.py`
- `analytics/results/q3_top10_receita.csv`
- `analytics/results/q3_top10_margem.csv`
- `analytics/results/q3_comparacao_rankings.csv`
- `analytics/results/q3_top10_receita_vs_margem.png`

### 5.4 Q4 — Retenção por coorte

**Status:** consulta, resultados, matriz e gráfico implementados, executados e validados.

A análise agrupa os clientes pelo mês da primeira compra observada na base disponível e calcula o percentual de clientes que realizaram compras em cada mês posterior.

**Critérios utilizados:**

- Identificação da primeira compra observada por cliente.
- Agrupamento por mês de entrada na coorte.
- Contagem distinta de clientes ativos em cada mês.
- Retenção calculada em relação ao total de clientes da coorte.
- Meses futuros sem dados representados como células vazias.
- Meses observados sem recompra representados como 0%.

**Resultados da retenção:**

| Coorte | Clientes iniciais | Mês 1 | Mês 2 | Mês 3 |
|---|---:|---:|---:|---:|
| Julho/2026 | 3.026 | 80,37% | 79,21% | 47,75% |
| Agosto/2026 | 1.609 | 78,87% | 47,36% | — |
| Setembro/2026 | 299 | 46,49% | — | — |
| Outubro/2026 | 24 | — | — | — |

**Interpretação:** a coorte de julho apresentou a maior retenção no primeiro mês, com 80,37%, seguida por agosto, com 78,87%. Setembro apresentou 46,49%. A redução da retenção ao longo dos meses indica oportunidade de desenvolver ações de recompra e relacionamento com clientes.

**Limitações metodológicas:**

- A primeira compra é a primeira observada na janela disponível, não necessariamente a primeira compra histórica do cliente.
- Os dados terminam em 12/10/2026.
- Os resultados referentes a outubro representam períodos parciais e devem ser interpretados com cautela.
- A retenção mensal considera clientes que compraram naquele mês específico, não necessariamente clientes que permaneceram ativos continuamente.

**Arquivos gerados:**

- `analytics/queries/q4_retencao_coorte.py`
- `analytics/results/q4_retencao_detalhada.csv`
- `analytics/results/q4_matriz_retencao.csv`
- `analytics/results/q4_retencao_coorte.png`

### 5.5 Q5 — ROAS por canal de mídia

**Status:** pendente de implementação.

A análise deverá relacionar a receita atribuída ao canal, por meio do `utm_source` do clickstream, com o investimento registrado no CSV de mídia, por canal e semana.

Será necessário padronizar os nomes dos canais e validar as regras de atribuição antes de apresentar os resultados.

### 5.6 Q6 — Auditoria de dados

**Status:** pendente de implementação.

A análise deverá identificar quantos pedidos ou registros relacionados a pedidos foram rejeitados na Silver, os respectivos motivos e a receita potencial associada.

Será necessário consolidar os registros de auditoria e relacioná-los aos dados de origem.

## 6. Organização do projeto

A estrutura principal inclui:

```text
pipeline-medalhao/
├── airbyte/
├── analytics/
│   ├── queries/
│   ├── results/
│   └── notebooks/
├── data_generator/
├── infra/
├── jobs/
│   ├── gold/
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

## 7. Execução e validação

### 7.1 Serviços locais

O Docker Compose é utilizado para executar o MySQL, o LocalStack e o ambiente Spark.

O Airbyte foi instalado separadamente por meio do `abctl`.

### 7.2 Conferir os objetos da Bronze

No PowerShell:

```powershell
docker exec medalhao-localstack awslocal s3 ls s3://techflow-bronze/ --recursive
```

### 7.3 Conferir os objetos da Silver

```powershell
docker exec medalhao-localstack awslocal s3 ls s3://techflow-silver/ --recursive
```

### 7.4 Conferir os objetos da Gold

```powershell
docker exec medalhao-localstack awslocal s3 ls s3://techflow-gold/ --recursive
```

### 7.5 Executar análises

Exemplo de execução da Q1:

```powershell
docker exec medalhao-spark python /home/jovyan/work/analytics/queries/q1_ticket_medio.py
```

Exemplo de execução da Q2:

```powershell
docker exec medalhao-spark python /home/jovyan/work/analytics/queries/q2_funil_conversao.py
```

Exemplo de execução da Q3:

```powershell
docker exec medalhao-spark python /home/jovyan/work/analytics/queries/q3_top_produtos.py
```

Exemplo de execução da Q4:

```powershell
docker exec medalhao-spark python /home/jovyan/work/analytics/queries/q4_retencao_coorte.py
```

Os resultados exportados são gravados em `analytics/results/` no ambiente local, conforme o volume configurado no Docker Compose.

## 8. Qualidade e auditoria

A Silver separa registros válidos e rejeitados para as fontes tratadas.

As quantidades e os motivos de rejeição são registrados nos resultados dos jobs e preservados nos caminhos de auditoria.

Na Gold, foram realizadas validações de integridade referencial e duplicidade:

- Nenhum pedido duplicado na validação realizada.
- Nenhum SKU duplicado na dimensão de produtos.
- Nenhum item sem pedido correspondente.
- Nenhum item sem produto correspondente.
- Nenhum pedido sem cliente correspondente.

Também foram identificadas limitações na cobertura temporal dos custos, documentadas na seção da Gold e na análise Q3.

Ainda será necessário consolidar essas informações em uma solução de observabilidade e na análise final de auditoria.

## 9. Histórico de desenvolvimento

| Etapa | Status |
|---|---|
| Validação do ambiente Windows e Docker | Concluída |
| Estrutura inicial do projeto | Concluída |
| Configuração inicial de MySQL e LocalStack | Concluída |
| Instalação e validação inicial do Airbyte | Concluída |
| Ingestão inicial de MySQL para Bronze | Concluída e validada |
| Carga de arquivos JSONL e CSV na Bronze | Concluída e validada |
| Conversão de clickstream e CSV para Parquet na Bronze | Concluída e validada |
| Processamento Silver das oito fontes | Concluído e validado |
| Separação de registros rejeitados para auditoria | Implementada nas regras descritas |
| Dimensões básicas da Gold | Implementadas |
| Fato de vendas Gold | Implementada e validada |
| Q1 — Ticket médio por categoria | Concluída e versionada |
| Q2 — Funil de conversão | Concluída e versionada |
| Q3 — Top 10 por receita e margem | Concluída e versionada |
| Q4 — Retenção por coorte | Concluída e validada; aguardando versionamento |
| Q5 — ROAS por canal | Pendente |
| Q6 — Auditoria de dados | Pendente |
| Orquestração reproduzível de todas as ingestões e jobs | Pendente |
| Consultas SQL e visualização analítica completa | Pendente |
| Testes integrados e documentação final | Em andamento |

## 10. Próximas etapas

1. Implementar a Q5 — ROAS por canal de mídia.
2. Implementar a Q6 — Auditoria de dados.
3. Revisar o modelo dimensional, incluindo os requisitos de SCD2.
4. Consolidar as regras de qualidade e observabilidade.
5. Automatizar a execução e as dependências do pipeline.
6. Validar a reprodução do ambiente e documentar a entrega final.

## 11. Segurança e versionamento

- Não versionar `.env`, senhas, tokens ou credenciais.
- Não versionar arquivos locais de configuração que contenham credenciais ou dados sensíveis.
- Preservar os dados brutos da Bronze para rastreabilidade e reprocessamento.
- Registrar a evolução por meio de commits pequenos e descritivos.
- Considerar uma etapa concluída somente após execução e validação no ambiente local.
