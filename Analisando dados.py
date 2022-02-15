# Databricks notebook source
# DBTITLE 1,Analisando dados com PySpark
# visulaizando datasets de exemplos do databricks
display(dbutils.fs.ls("/databricks-datasets"))

# COMMAND ----------

# Criando uma variavel para conter o caminho de onde se encontra o aquivo CSV
arquivo = "dbfs:/databricks-datasets/flights/"

# COMMAND ----------

# Verificando o conteudo da variavel
arquivo

# COMMAND ----------

# Lendo o arquivo de dados 
df =  spark \
.read.format("csv")\
.option("inferSchema", "True")\
.option("header", "True")\
.csv(arquivo)


# COMMAND ----------

# Imprimir os datatypes das colunas do dataframe
df.printSchema()

# COMMAND ----------

type(df)

# COMMAND ----------

# Retorna as primieras linas de acordo com o parametro passado em formato  array.
df.take(5)

# COMMAND ----------

# Usando o comando display ele mostra os dados em forma de tabela
display(df.show(10))

# COMMAND ----------

# Imprimir a qtdade de linhas
df.count()

# COMMAND ----------

# DBTITLE 1,Consultando dados do dataframe
from pyspark.sql.functions import max
df.select(max("delay")).take(1)

# COMMAND ----------

# filtrando linhas de um dataframe usando filter

df.filter("delay < 2").show(10)

# COMMAND ----------

# podemos utilizar como filtro o WHERE
df.where("delay < 2").show(10)

# COMMAND ----------

# Ordenando o dataframe pela coluna delay

df.sort("delay").show(4)

# COMMAND ----------

# Ordenando por ordem crescente
from pyspark.sql.functions import desc, asc, expr

df.orderBy(expr("delay desc")).show(5)

# COMMAND ----------

# iterando sobre todas as linhas do dataframe
for i in df.collect():
    print(i)
    print(i[0], i[1])

# COMMAND ----------

# adicionando coluna ao dataframe
df = df.withColumn('nova coluna', df['delay'] + 2)
df.show(10)

# COMMAND ----------

# Removendo uma coluna do dataframe
df = df.drop('nova coluna')
df.show(10)

# COMMAND ----------

#  Renomeando uma coluna no dataframe
df.withColumnRenamed('nova coluna', 'new column').show(10)

# COMMAND ----------

# DBTITLE 1,Trabalhando com mossing values
df.filter("delay is null").show()

# COMMAND ----------

# Filtrando valores missing
df.filter(df.delay.isNull()).show(10)

# COMMAND ----------

# preenchendo os dados missing com o valor 0

df.na.fill(value=0).show()

# COMMAND ----------

# preenchendo valores missing em uma coluna especifica com o valor 0

df.na.fill(value=0, subset=['delay']).show()

# COMMAND ----------

# Preenchendo os  dados com valores de string vazia
df.na.fill("").show()

# COMMAND ----------

# Removendo qualquer linha nula de qualquer coluna
df.na.drop().show()

# COMMAND ----------


