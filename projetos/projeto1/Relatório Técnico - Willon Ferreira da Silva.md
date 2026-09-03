![](https://www.infnet.edu.br/infnet/wp-content/uploads/sites/18/2021/10/infnet-30-horizontal-padrao@300x-8-1024x265.png)

# MBA em Engenharia de Dados: Big Data e IA

## Processamento de Big Data com Apache Spark e Spark SQL [26E3_2]

### Relatório Técnico do Projeto da Disciplina

**Autor:** Willon Ferreira da Silva
**Data:** 30/08/2026
**Dataset:** Instacart Market Basket Analysis (Kaggle)

---

## 1. Introdução

Este relatório documenta, de forma técnica, o projeto de processamento de dados desenvolvido para a disciplina em PySpark/Spark SQL sobre a plataforma Databricks, utilizando o dataset público *Instacart Market Basket Analysis*. O objetivo do projeto foi construir um pipeline de dados em camadas (Raw, Bronze, Silver e Gold), aplicando boas práticas de engenharia de dados como tipagem, padronização, deduplicação e carga incremental via Delta Lake

---



## 2. Descrição da Arquitetura

O pipeline segue o padrão conhecido como Arquitetura Medalhão, amplamente utilizado em projetos de engenharia de dados. Nesse padrão, os dados evoluem em qualidade e granularidade de forma progressiva através de camadas, até chegarem a um formato pronto para consumo analítico.

Todas as camadas foram organizadas dentro de um único catálogo do Unity Catalog chamado `instacart`, com um schema (banco de dados) para cada camada: `instacart.raw`, `instacart.bronze`, `instacart.silver` e `instacart.gold`. Essa separação por schema facilita a governança de acesso (quem pode ler/escrever em cada camada) e deixa explícito em que estágio de maturidade aquele dado se encontra.

### 2.1. Camada Raw

A camada Raw é a porta de entrada dos dados brutos. Os seis arquivos CSV originais do dataset (`aisles.csv`, `departments.csv`, `orders.csv`, `products.csv`, `order_products__prior.csv` e `order_products__train.csv`) são baixados do Kaggle via biblioteca `kagglehub` e copiados, sem qualquer alteração, para volumes do Unity Catalog (`instacart.raw.aisle`, `instacart.raw.department`, etc.). Volumes foram escolhidos por serem o mecanismo do Unity Catalog voltado para armazenamento de arquivos não tabulares (nesse caso, os CSVs originais), preservando o dado exatamente como foi entregue pela fonte — o que é importante caso seja necessário reprocessar tudo do zero no futuro.

### 2.2. Camada Bronze

Na camada Bronze, os CSVs são lidos com Spark (`spark.read.csv`) e persistidos em tabelas Delta, também sem transformação de conteúdo — apenas a conversão do formato de armazenamento (CSV → Delta). Todas as colunas são mantidas como `STRING`, refletindo fielmente o dado de origem. O objetivo dessa camada é ter uma cópia histórica, auditável e versionada (graças ao Delta Lake) dos dados brutos, já em um formato performático para leitura.

### 2.3. Camada Silver

A camada Silver é onde ocorre a limpeza e padronização dos dados: conversão de tipos (`STRING` → `INTEGER`/`FLOAT`), remoção de duplicatas, padronização de texto (letras maiúsculas), renomeação de colunas para nomes mais claros (por exemplo, `aisle` → `description`) e tratamento de valores nulos com valores-padrão (`"DESCONHECIDO"` para texto e `-1.0` para a coluna numérica `days_since_prior_order`). As tabelas `order_product_prior` e `order_product_train` também são unificadas em uma única tabela `order_product`, marcadas por uma coluna `eval_set` que identifica a origem do registro.

### 2.4. Camada Gold

A camada Gold combina e modela os dados da Silver em tabelas prontas para consumo analítico — por exemplo, a tabela `gold.order`, que já traz o pedido (`order`) unido ao produto comprado (`product_id`), eliminando a necessidade de quem for consumir o dado (um analista, um dashboard, um modelo de machine learning) ter que fazer esse join novamente.

### 2.5. Estratégia de carga entre camadas

Um ponto central da arquitetura é a função `merge_data`, reutilizada em todas as transições entre camadas (Bronze → Silver → Gold). Ela executa um `MERGE INTO` do Delta Lake com a cláusula `whenNotMatchedInsertAll`, usando as chaves de negócio de cada tabela (`aisle_id`, `department_id`, `order_id`, `product_id`, ou o conjunto de colunas de `order_product`) como critério de correspondência. Na prática, isso significa que registros cuja chave já existe no destino são ignorados, e apenas registros novos são inseridos. Essa configuração permite o pipeline poder ser executado novamente sem duplicar dados e preparado para um cenário de **ingestão contínua**, em que novos arquivos podem chegar periodicamente à camada Raw sem exigir nenhuma alteração no código.

Todas as tabelas Delta foram criadas com as propriedades `delta.enableChangeDataFeed`, `delta.autoOptimize.optimizeWrite` e `delta.autoOptimize.autoCompact` habilitadas, o que já antecipa necessidades operacionais discutidas na Seção 4 (rastreamento de mudanças e compactação automática de arquivos pequenos).

### 2.6. Por que não foi utilizado Streaming

Optei por não utilizar Spark Structured Streaming neste projeto porque a fonte de dados é um dataset histórico do Kaggle, baixado uma única vez, sem novos arquivos chegando em tempo real — não havia, portanto, um requisito que justificasse um pipeline contínuo. O processamento em batch, combinado com o padrão de merge já atende ao cenário de ingestão contínua por reprocessamento periódico (ex.: um job agendado), sem necessidade de streaming. Caso no futuro surgisse a necessidade de dados quase em tempo real, a migração seria natural: bastaria trocar `spark.read.csv` por `spark.readStream` e envolver o `merge_data` em um `foreachBatch`, já que a lógica de MERGE do Delta Lake é compatível com streams.

---



## 3. Justificativa das Estratégias de Join

O pipeline possui dois momentos em que operações de junção (join) acontecem: (i) implicitamente, dentro de cada `MERGE INTO` executado pela função `merge_data`; e (ii) explicitamente, no join realizado na camada Gold entre as tabelas `order` e `order_product`.

### 3.1. Join implícito nas operações de MERGE

Cada chamada a `merge_data` compara o DataFrame de origem com a tabela Delta de destino usando as chaves de negócio da tabela. Internamente, o Spark precisa casar as linhas dos dois lados por essa chave — o que é, na prática, um join. A estratégia física escolhida pelo Spark para esse "join" varia de acordo com o tamanho das tabelas envolvidas:

- Para as tabelas de dimensão pequenas — `aisle` (134 corredores) e `department` (21 departamentos), segundo a documentação pública do dataset — o volume de dados fica muito abaixo do limite padrão de *broadcast* do Spark (`spark.sql.autoBroadcastJoinThreshold`, 10 MB por padrão). Isso permite que o otimizador Catalyst distribua (*broadcast*) a tabela inteira para todos os executores, evitando uma operação de shuffle e tornando o merge praticamente instantâneo.
- Para as tabelas de fato maiores — `order` (cerca de 3,4 milhões de pedidos) e principalmente `order_product` (a união de `prior` e `train` soma mais de 33 milhões de linhas, sempre segundo a documentação pública do dataset) — nenhum dos lados cabe no limite de broadcast. Nesses casos, o Spark recorre a um **Sort-Merge Join**: ambos os lados são reparticionados pela chave de junção e depois ordenados/casados. É uma operação mais custosa, mas necessária quando não há uma tabela pequena o suficiente para ser transmitida.

Optei por **não** forçar nenhum hint de broadcast (`broadcast()`) no código. A razão é que o volume de dados de cada camada tende a crescer ao longo do tempo (principalmente em um cenário de ingestão contínua, como discutido na Seção 2.5), e um hint fixo poderia deixar de fazer sentido — ou até prejudicar a performance — assim que uma tabela hoje pequena passasse a crescer.

### 3.2. Join explícito na camada Gold

Na construção da tabela `gold.order`, é feito um `inner join` entre `silver.order` e `silver.order_product`, pela chave `order_id`:

```python
orders_df = (orders_df
 .join(orders_products_df.drop("eval_set"), on="order_id", how="inner")
 .select("order_id", "user_id", "product_id", "order_day_of_week")
 .sort("order_id", "product_id")
)
```

A escolha do `inner join` (em vez de `left join`) foi deliberada: para o propósito analítico da camada Gold — analisar o comportamento de compra por produto — só fazem sentido pedidos que de fato têm produtos associados. Um pedido sem nenhum produto vinculado (o que não deveria ocorrer no dataset original, mas poderia acontecer em um cenário real de dados inconsistentes) não agregaria valor a essa tabela e seria descartado naturalmente pelo inner join, funcionando também como uma checagem implícita de integridade referencial entre as duas tabelas.

Do ponto de vista de estratégia física, esse é novamente um join grande-grande (`order` com ~3,4 milhões de linhas contra `order_product` com mais de 33 milhões de linhas), então, pelas mesmas razões explicadas na seção anterior, o Spark deve optar por um Sort-Merge Join via shuffle, e não por broadcast.

---



## 4. Análise das Métricas de Monitoramento

Como o foco do projeto foi a construção do pipeline batch, o monitoramento implementado até este ponto é voltado principalmente para **qualidade dos dados**, e não para métricas de infraestrutura (tempo de execução, uso de cluster, etc.). Ainda assim, algumas métricas relevantes já foram coletadas e outras estão preparadas na arquitetura para uso futuro:

- **Contagem de registros por tabela na leitura da camada Raw.** Logo após a leitura dos CSVs, o notebook imprime a quantidade de linhas de cada uma das seis tabelas. Essa contagem funciona como uma primeira checagem de sanidade (o arquivo foi lido por completo?) e também como uma métrica de referência (*baseline*) para comparar com o volume de linhas restante após a deduplicação feita na camada Silver — uma eventual queda muito maior do que a esperada nesse número indicaria um problema na etapa de limpeza.
- **Contagem de valores nulos por coluna**, executada logo após as transformações de cada uma das cinco tabelas da camada Silver (`aisle`, `department`, `order`, `product` e `order_product`). Essa é a principal métrica de qualidade do pipeline: ela confirma que o tratamento de nulos (preenchimento com `"DESCONHECIDO"` ou `-1.0`, conforme a coluna) funcionou como esperado e que nenhuma coluna crítica ficou com dados ausentes de forma inesperada.
- **Propriedades do Delta Lake configuradas em todas as tabelas**, que embora não sejam "métricas" no sentido estrito, dão suporte à observabilidade operacional do pipeline:
  - `delta.enableChangeDataFeed = true` permite consultar, a qualquer momento, exatamente quais linhas foram inseridas, atualizadas ou removidas em cada operação (via `table_changes()`), o que é essencial para auditar o comportamento incremental do `merge_data`.
  - `delta.autoOptimize.optimizeWrite` e `delta.autoOptimize.autoCompact` mantêm o tamanho dos arquivos Parquet por trás das tabelas Delta otimizado automaticamente, evitando o chamado "problema dos arquivos pequenos", que é uma das causas mais comuns de degradação de performance em pipelines que fazem cargas incrementais frequentes.

Como próximo passo — não implementado neste projeto, mas recomendado para uma versão produtiva do pipeline — seria interessante instrumentar o monitoramento operacional nativo do Databricks: o **Query History** e o **Spark UI** para acompanhar tempo de execução e volume de shuffle de cada join/merge, e o comando `DESCRIBE HISTORY` sobre cada tabela Delta, que registra automaticamente métricas de cada operação de `MERGE` (linhas inseridas, atualizadas, deletadas, arquivos escritos), permitindo montar um painel de acompanhamento da saúde do pipeline ao longo do tempo.

---



## 5. Visualização da Linhagem de Dados

A linhagem de dados (*data lineage*) representa o caminho percorrido por cada informação, desde a fonte original até a tabela final consumida. No pipeline construído, essa linhagem segue exatamente a sequência das camadas da arquitetura, sendo praticamente uma leitura direta do fluxo de execução do notebook:

```
Kaggle (CSV)
   │  kagglehub.dataset_download()
   ▼
instacart.raw.*  (Volumes — arquivos CSV originais)
   │  spark.read.csv()
   ▼
instacart.bronze.*  (Delta — dado bruto, tipado como STRING)
   │  limpeza, tipagem, deduplicação, tratamento de nulos
   ▼
instacart.silver.*  (Delta — dado limpo e padronizado)
   │  join order ⋈ order_product (inner, por order_id)
   ▼
instacart.gold.*  (Delta — dado modelado para consumo analítico)
```

Cada seta representa uma transformação registrada no notebook, e cada caixa corresponde a um conjunto de tabelas (`aisle`, `department`, `order`, `product` e `order_product`, quando aplicável a cada camada).

É possível visualizar a linhagem real e atualizada de qualquer tabela do projeto diretamente no Catalog Explorer do Databricks, na aba **Lineage** de cada tabela sem necessidade de nenhuma configuração ou código adicional.

---



## 6. Considerações Finais

O pipeline desenvolvido aplica os principais conceitos trabalhados na disciplina: arquitetura em camadas, uso de Delta Lake para cargas incrementais e idempotentes, escolha consciente de estratégias de join com base no volume de dados, checagens básicas de qualidade de dados e um design compatível com governança e linhagem automática via Unity Catalog. Os pontos levantados como próximos passos (monitoramento operacional via `DESCRIBE HISTORY`/Query History e suporte a atualização de registros já existentes via `whenMatchedUpdate`) ficam como evolução natural do projeto para um cenário de produção com ingestão contínua.