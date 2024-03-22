# Engenharia de dados - Introdução às boas práticas de desenvolvimento com Python

Este repositório tem como objetivo principal oferecer um guia de boas práticas no desenvolvimento de software utilizando Python, especialmente voltado para a área de engenharia de dados. Procuro demonstrar metodologias eficazes para a organização e estruturação de código, além de fornecer técnicas para manter o código esteticamente agradável e organizado. Um foco especial é dado à implementação de padrões de desenvolvimento para jobs de ETL, visando aprimorar a qualidade e a eficiência dos processos de extração, transformação e carga de dados.

# PEP8

PEP8 é um guia de estilo para escrita de código em Python. Ele é mantido pela comunidade e oferece diretrizes sobre como escrever código legível e organizado. Seguir as recomendações do PEP8 é uma prática comum e recomendada para todos os desenvolvedores Python. O PEP8 é composto por um conjunto de regras que abrangem desde a formatação do código até a nomenclatura de variáveis e funções. A seguir, apresentamos algumas das principais regras do PEP8.

***ATENÇÃO:*** O PEP8 é um guia de estilo e não uma regra rígida. Em alguns casos, pode ser necessário abrir mão de algumas recomendações para manter a legibilidade do código. Foque em deixar um código esteticamente limpo, bonito e organizado, mas não se preocupe em seguir todas as recomendações à risca.

## Formatação do Código

- Indentação: Utilizar 4 espaços por nível de indentação.
- Linhas: Limitar o comprimento das linhas a 79 caracteres para código e 72 caracteres para comentários e docstrings.
Uso de espaços em expressões e instruções: Deve-se seguir um padrão na utilização de espaços em torno de operadores e após vírgulas para melhorar a legibilidade.

## Convenções de Nomes
- snake_case para funções, variáveis e atributos: Utilizar letras minúsculas e sublinhados para separar palavras.
- CamelCase para classes: Iniciar nomes de classes com uma letra maiúscula e não usar sublinhados.
- Constantes: Escrever em letras maiúsculas, com palavras separadas por sublinhados.

## Expressões e Declarações
- Evitar comparações desnecessárias: Por exemplo, usar if some_list em vez de if len(some_list) > 0.
- Uso de operadores de comparação encadeados: Utilizar encadeamentos de comparação quando apropriado, como em a < b < c.

## Importações
- Ordem: Importações devem ser agrupadas em três blocos: bibliotecas padrão, bibliotecas de terceiros e importações locais/modificações, cada bloco separado por uma linha em branco.

- Evitar importações com *: Isso pode tornar o código menos claro, pois não é evidente quais nomes são importados para o namespace.

## Espaçamento
- Espaços em branco em expressões e instruções: Não usar espaços em excesso; por exemplo, dentro de parênteses, colchetes ou chaves, e entre um sinal de igualdade e um argumento padrão em definições de função.

## Comentários
- Comentários relevantes: Comentar o código sempre que isso ajudar na compreensão do funcionamento do mesmo. Comentários devem ser atualizados se o código que eles descrevem mudar.

## Uso de Docstrings
- Docstrings para módulos, classes, métodos e funções: São importantes para documentar o código e facilitar a compreensão de sua funcionalidade.

```python

def compute_cep_with_wrangler(source_path: str):
    """Computa um Polars DataFrame com dados de CEPS do arquivo CSV TB_CEP_BR_2018.csv

    Args:
        source_path (str): Diretório onde o arquivo TB_CEP_BR_2018.csv está armazenado

    Returns:
        pl.DataFrame: Polars DataFrame com todos os dados do arquivo TB_CEP_BR_2018.csv
    """
    FILEPATH = f"{source_path}/TB_CEP_BR_2018.csv"
    COLUMNS = ["CEP", "UF", "CIDADE", "BAIRRO", "RUA"]
    DTYPES = {
        "CEP": "str", 
        "UF": "str", 
        "CIDADE": "str", 
        "BAIRRO": "str", 
        "RUA": "str"
    }
    
    df = wr.s3.read_csv(FILEPATH, sep=";", header=None, names=COLUMNS, dtype=DTYPES)
    return pl.from_dataframe(df)
```

# Padronização de codificação de jobs de ETL

A implementação de um padrão de codificação para jobs de ETL é fundamental para assegurar a qualidade e eficiência desses processos. Ao longo da minha experiência profissional, notei que muitos engenheiros de dados, por não terem uma formação sólida em engenharia de software, frequentemente produzem códigos que são desorganizados, ineficientes, difíceis de manter, compreender e inconsistentes. Com o intuito de mitigar esse desafio, sugiro a adoção de um padrão de codificação para jobs de ETL que seja aplicável, independentemente das ferramentas utilizadas no desenvolvimento desses jobs (como Airflow, Prefect, Dagster, Glue + Step Functions, entre outras).

## Origem da ideia

Recentemente, o campo da engenharia de dados tem testemunhado a introdução de soluções inovadoras e robustas destinadas a resolver desafios comuns da área. Entre essas soluções, o Dagster destaca-se como uma ferramenta promissora de orquestração de pipelines, apresentando-se como uma alternativa ao Airflow. O grande destaque do Dagster reside na sua abordagem para a padronização de código através de uma funcionalidade conhecida como Assets.

Os Assets do Dagster oferecem uma forma eficiente e organizada de gerenciar dados em um pipeline de ETL. Simplificando, um Asset no Dagster é qualquer item de dados criado ou utilizado durante um pipeline sendo representado por uma função Python. Isso pode ser desde arquivos de dados e registros em bancos de dados até tabelas, métricas e relatórios. Este conceito é fundamental no Dagster, pois facilita a construção, teste e monitoramento de pipelines de dados de maneira mais eficiente.

A proposta de padronizar a codificação de jobs de ETL visa trazer a organização e a gestão de código, típicas do Dagster, para o desenvolvimento em Python puro. Isso significa criar funções que representem Assets, como tabelas, arquivos, métricas, relatórios etc., que geralmente retornam DataFrames, listas, dicionários ou outros tipos de dados correspondentes a um Asset. A definição clara de Assets torna a lógica do ETL mais transparente e facilita os testes, transformando desafios complexos em problemas menores e isolados que podem ser avaliados individualmente. Essa abordagem não só permite a reutilização dessas funções em diferentes jobs de ETL, mas também simplifica a orquestração do job, tornando-a mais compreensível.

## Estrutura de um job de ETL padronizado

A estrutura de um código de ETL padronizado pode ser dividida em tres partes principais: 

1. **Importação e inicialização de variáveis**: importação de módulos, funcoes e bibliotecas necessárias assim como a inicializacao de variaveis importantes para o processo
2. **Assets**: definicao de funcoes que representam os Assets do job (no código de exemplos, sao representadas pelas funcoes com prefixo "compute_")
3. **Execução do JOB**: Execução das funcoes que representam os Assets do job, orquestrando-as de acordo com a lógica do processo de ETL

### Código completo

```python

#%% Importing the libraries
import logging

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from internal.mssql_handler import merge_into_mssql
from internal.pyspark_helper import get_pre_configured_spark_session_builder, get_jdbc_options

#%% Initialize the SparkSession
SPARK = get_pre_configured_spark_session_builder() \
    .appName("Load Programacao Pagamento Table") \
    .getOrCreate()

logging.basicConfig()
LOGGER = logging.getLogger("pyspark")
LOGGER.setLevel(logging.INFO)

def compute_notas_fiscais_de_entrada(spark: SparkSession):
    """Compute the dataframe using a query"""
    SQL = """
    SELECT
        A.ID_NF_ENTRADA, 
        A.DATA_EMISSAO,
        A.VALOR_TOTAL,
        B.QTD_PARCELAS
    FROM
        NOTAS_FISCAIS_ENTRADA A
        INNER JOIN CONDICAO_PAGAMENTO B  
            ON A.ID_CONDICAO = B.ID_CONDICAO
    """
    
    return spark.read \
        .format("jdbc") \
        .options(**get_jdbc_options()) \
        .option("query", SQL) \
        .load()

def compute_programacao_pagamento_pendente(spark: SparkSession):
    """Compute the programcao_pagamento dataframe"""
    
    SQL = """
        SELECT ID_NF_ENTRADA, DATA_VENCIMENTO, NUM_PARCELA, VALOR_PARCELA 
        FROM PROGRAMACAO_PAGAMENTO 
        WHERE STATUS_PAGAMENTO = 'PENDENTE'
    """
    return spark.read \
        .format("jdbc") \
        .options(**get_jdbc_options()) \
        .option("query", SQL) \
        .load()

#%% Load the function to compute the stage data for the table 'tipo_pagamento'
def compute_programacao_pagamento(df_notas_fiscais_entrada: DataFrame, df_programacao_pagamento_pendente: DataFrame):
    """Compute the Programacao Pagamento dataframe using the Notas Fiscais de Entrada dataframe"""
    
    df = (
        df_notas_fiscais_entrada
        .withColumn("PARCELAS", F.sequence(F.lit(1), F.col("QTD_PARCELAS")))
        .withColumn("NUM_PARCELA", F.explode(F.col("PARCELAS")))
        .drop("PARCELAS")
    )
    
    df = df.withColumn("VALOR_PARCELA", F.col("VALOR_TOTAL") / F.col("QTD_PARCELAS"))
    # I using the expr function because the F.add_months function not accept a column as integer parameter to increment the date 
    df = df.withColumn("DATA_VENCIMENTO", F.expr("add_months(DATA_EMISSAO, NUM_PARCELA - 1)"))
    
    df = df.select(
        'ID_NF_ENTRADA',
        'DATA_VENCIMENTO',
        'NUM_PARCELA',
        'VALOR_PARCELA'
    )
    
    df = df.union(df_programacao_pagamento_pendente)
    
    # All dates equal or minor than today are considered as "PAGO", else "PENDENTE"
    df = (
        df
        .withColumn("STATUS_PAGAMENTO",   
                    F.when(F.col("DATA_VENCIMENTO") <= F.current_date(), F.lit("PAGO"))
                    .otherwise(F.lit("PENDENTE")))
    )
    
    return df
#%% Job execution
if __name__ == "__main__":
    df_notas_fiscais_entrada = compute_notas_fiscais_de_entrada(SPARK)
    df_programacao_pendente = compute_programacao_pagamento_pendente(SPARK)
    df = compute_programacao_pagamento(df_notas_fiscais_entrada, df_programacao_pendente)
    
    statiscs = merge_into_mssql(df, 'PROGRAMACAO_PAGAMENTO', ['ID_NF_ENTRADA', 'NUM_PARCELA'])
    
    LOGGER.info(f"Inserted {statiscs['INSERT']} rows")
    LOGGER.info(f"Updated {statiscs['UPDATE']} rows")

```

### Importação e inicialização de variáveis
Este trecho de código tem como objetivo realizar as importações necessárias para o funcionamento do nosso ETL, além de definir algumas constantes e variáveis que serão usadas ao longo do processo, tanto nos assets quanto na execução do job. Aqui, inicializamos a SparkSession, configuramos um logger para acompanhar a execução do job e importamos as funções específicas para realizar operações com o banco de dados.

```python

#%% Importing the libraries
import logging

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from internal.mssql_handler import merge_into_mssql
from internal.pyspark_helper import get_pre_configured_spark_session_builder, get_jdbc_options

#%% Initialize the SparkSession
SPARK = get_pre_configured_spark_session_builder() \
    .appName("Load Programacao Pagamento Table") \
    .getOrCreate()

logging.basicConfig()
LOGGER = logging.getLogger("pyspark")
LOGGER.setLevel(logging.INFO)

```

### Assets


As funções que representam Assets devem ser funções puras, isto é, funções sem efeitos colaterais, que sempre retornam o mesmo resultado para os mesmos argumentos fornecidos. Em um job de ETL, essas funções tipicamente retornam DataFrames, listas, dicionários ou outros tipos de dados que simbolizam um Asset. A definição clara de assets torna a lógica do ETL mais transparente e facilita os testes, transformando desafios complexos em problemas menores e isolados que podem ser avaliados individualmente. Essa abordagem não só permite a reutilização dessas funções em diferentes jobs de ETL, mas também simplifica a orquestração do job, tornando-a mais compreensível.

No exemplo citado, há três funções que exemplificam os assets de um job: compute_notas_fiscais_de_entrada, compute_programacao_pagamento_pendente e compute_programacao_pagamento. A última função mencionada, compute_programacao_pagamento, atua como o asset final do job, representando o resultado conclusivo do processo de ETL, e depende dos outros dois assets anteriores para ser calculado.

```python

def compute_notas_fiscais_de_entrada(spark: SparkSession):
    """Compute the dataframe using a query"""
    SQL = """
    SELECT
        A.ID_NF_ENTRADA, 
        A.DATA_EMISSAO,
        A.VALOR_TOTAL,
        B.QTD_PARCELAS
    FROM
        NOTAS_FISCAIS_ENTRADA A
        INNER JOIN CONDICAO_PAGAMENTO B  
            ON A.ID_CONDICAO = B.ID_CONDICAO
    """
    
    return spark.read \
        .format("jdbc") \
        .options(**get_jdbc_options()) \
        .option("query", SQL) \
        .load()

def compute_programacao_pagamento_pendente(spark: SparkSession):
    """Compute the programcao_pagamento dataframe"""
    
    SQL = """
        SELECT ID_NF_ENTRADA, DATA_VENCIMENTO, NUM_PARCELA, VALOR_PARCELA 
        FROM PROGRAMACAO_PAGAMENTO 
        WHERE STATUS_PAGAMENTO = 'PENDENTE'
    """
    return spark.read \
        .format("jdbc") \
        .options(**get_jdbc_options()) \
        .option("query", SQL) \
        .load()

#%% Load the function to compute the stage data for the table 'tipo_pagamento'
def compute_programacao_pagamento(df_notas_fiscais_entrada: DataFrame, df_programacao_pagamento_pendente: DataFrame):
    """Compute the Programacao Pagamento dataframe using the Notas Fiscais de Entrada dataframe"""
    
    df = (
        df_notas_fiscais_entrada
        .withColumn("PARCELAS", F.sequence(F.lit(1), F.col("QTD_PARCELAS")))
        .withColumn("NUM_PARCELA", F.explode(F.col("PARCELAS")))
        .drop("PARCELAS")
    )
    
    df = df.withColumn("VALOR_PARCELA", F.col("VALOR_TOTAL") / F.col("QTD_PARCELAS"))
    # I using the expr function because the F.add_months function not accept a column as integer parameter to increment the date 
    df = df.withColumn("DATA_VENCIMENTO", F.expr("add_months(DATA_EMISSAO, NUM_PARCELA - 1)"))
    
    df = df.select(
        'ID_NF_ENTRADA',
        'DATA_VENCIMENTO',
        'NUM_PARCELA',
        'VALOR_PARCELA'
    )
    
    df = df.union(df_programacao_pagamento_pendente)
    
    # All dates equal or minor than today are considered as "PAGO", else "PENDENTE"
    df = (
        df
        .withColumn("STATUS_PAGAMENTO",   
                    F.when(F.col("DATA_VENCIMENTO") <= F.current_date(), F.lit("PAGO"))
                    .otherwise(F.lit("PENDENTE")))
    )
    
    return df
```


### Execução do JOB

A etapa de execução do job representa o momento final no código, onde ocorre a execução e organização das funções que simbolizam os assets, seguindo a estratégia definida para o processo de ETL. No caso em questão, as funções compute_notas_fiscais_de_entrada e compute_programacao_pagamento_pendente são inicialmente acionadas para gerar os DataFrames necessários. Posteriormente, esses DataFrames são utilizados na função compute_programacao_pagamento. Após a conclusão dessa etapa, o resultado final é armazenado em um banco de dados SQL Server, e informações referentes à execução do job são devidamente registradas.

```python

#%% Job execution
if __name__ == "__main__":
    df_notas_fiscais_entrada = compute_notas_fiscais_de_entrada(SPARK)
    df_programacao_pendente = compute_programacao_pagamento_pendente(SPARK)
    df = compute_programacao_pagamento(df_notas_fiscais_entrada, df_programacao_pendente)
    
    statiscs = merge_into_mssql(df, 'PROGRAMACAO_PAGAMENTO', ['ID_NF_ENTRADA', 'NUM_PARCELA'])
    
    LOGGER.info(f"Inserted {statiscs['INSERT']} rows")
    LOGGER.info(f"Updated {statiscs['UPDATE']} rows")

```


# Explicação do código "legacy"

Na pasta "legacy" deste repositório, você encontrará duas subpastas, cada uma representando um projeto distinto. Esses projetos servem como exemplos de códigos de ETL que não adotam as boas práticas de desenvolvimento abordadas neste guia. Originalmente, estes projetos visam à integração de dados, tratando especificamente de informações de artigos científicos com o objetivo de consolidá-los em uma única base de dados. Utilizando esses projetos como referência, planejamos reescrever o código de ETL para alinhá-lo com as boas práticas mencionadas. O novo código será disponibilizado na pasta "src". Embora o foco seja em um ambiente AWS, para efeitos demonstrativos, o desenvolvimento será realizado em um ambiente local.

## python-for-data-engineers-grupo-4-master

Segue a explicação dos diretórios e arquivos do projeto "**python-for-data-engineers-grupo-4-master**":

```bash
`api_src` - Contém o código fonte da API REST construída com os dados disponibilizados pela pipeline (pasta `src`).
├── `api_resources` - Contém os recursos auxiliares usados na codificação de rotas da API.
├── `database.py` - Módulo responsável por mediar a comunicação entre a aplicação e o banco de dados
├── `main.py` - Módulo principal da aplicação, responsável por iniciar o servidor da API
└── `routes` - Contém os módulos que definem as rotas da API.
`pyproject.toml` - Arquivo de configuração do Poetry
`README.md` - Documentação do projeto
`src` - Contém o código fonte da pipeline de ETL.
├── `config.yaml` - Arquivo que armazena as configurações da pipeline (filtros customizados e tipo de extensão de arquivo de saída)
├── `data` - Pasta que armazena os dados de origem da pipeline
│   ├──  `acm` - Pasta que armazena os arquivos .bib extraídos da base de dados ACM
│   ├──  `ieee` - Pasta que armazena os arquivos .bib extraídos da base de dados IEEE
│   ├──  `science_direct` - Pasta que armazena os arquivos .bib extraídos da base de dados Science Direct
│   ├──  `jcs_2020.csv` - Arquivo CSV que contém os dados da base de dados Journal Citation Reports
│   └──  `scimagojr 2020.csv` - Arquivo CSV que contém os dados da base de dados Scimago Journal & Country Rank
├── `main.ipynb` - Notebook Jupyter que contém o código fonte da pipeline
├── `modules` - Pasta que contém os módulos auxiliares da pipeline
│   ├── `bibtex_reader.py` - Módulo responsável por abstrair a leitura e manipulação dos arquivos .bib
│   ├── `config.py` - Módulo responsável por abstrair a leitura do arquivo de configuração da pipeline
│   ├── `folder_reader.py` - Módulo responsável por abstrair a leitura dos arquivos de uma pasta
│   ├── `ieee_api_handler.py` - Módulo responsável por abstrair a comunicação com a API da IEEE
│   ├── `operations.py` - Módulo responsável por abstrair as operações de transformação dos dados
│   ├── `science_direct_api_handler.py` - Módulo responsável por abstrair a comunicação com a API da Science Direct
│   ├── `specific_operations.py` - Módulo responsável por abstrair as operações específicas de transformação dos dados
│   └── `sqlite3_loader.py` - Módulo responsável por abstrair a manipulação e carga dos dados no banco de dados SQLite3
├── `output` - Pasta que armazena os arquivos de saída da pipeline
└── `sandbox` - Pasta que armazena os arquivos de teste da pipeline
`todo.md` - Arquivo que contém as tarefas a serem realizadas no projeto
```

## spark-for-data-enginners-main

Segue a explicação dos diretórios e arquivos do projeto **spark-for-data-enginners-main**":

```bash
`consumer` - Contém o código fonte do consumidor de mensagens do Kafka
├── `consumer.ipynb` - Notebook Jupyter que contém o código fonte do consumidor de mensagens do Kafka
└── `data` - Pasta que armazena os arquivos de saída do consumidor
    └── `db.sqlite` - Arquivo do banco de dados SQLite3
`docker-compose.yml` - Arquivo de configuração do Docker Compose
`migrations` - Pasta que contém os arquivos de migração do banco de dados
└── migration_1.sql - Arquivo de migração que cria a tabela de dados
`on_docker_compose_up.md` - Documento que contém as instruções para execução do projeto
`outras atividades` - Pasta que contém os arquivos de outras atividades realizadas durante o curso
├── `Spark_For_Data_Enginners_Execicio_Aula_3.ipynb` - Notebook Jupyter que contém o código fonte da atividade 3 (equivalente à pipeline do projeto **python-for-data-engineers-grupo-4-master**)
└── `Word_Count.ipynb` - Notebook Jupyter que contém o código fonte da primeira atividade do curso
`producer` - Contém o código fonte do produtor de mensagens do Kafka
└── `producer.py` - Módulo Python que contém o código fonte do produtor de mensagens do Kafka
`pyproject.toml` - Arquivo de configuração do Poetry
`README.md` - Documentação do projeto
```


# TODO 

- [ ] Implementar o código refatorado no AWS Glue 
- [ ] Orquestrar o código refatorado com o AWS Step Functions
- [ ] Quebrar o código refatorado em um JOB responsável por gerar uma etapa de stage armazenando e concatenando os arquivos bib em um Dataframe e disponibilizando em Parquet
- [ ] Implementar o código refatorado no AWS Glue e AWS Step Functions