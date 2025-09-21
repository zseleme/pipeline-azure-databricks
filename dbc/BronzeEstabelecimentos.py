# Databricks notebook source
# MAGIC %run /Projeto_EmpregaDados_Ampev/00.config/Configurações

# COMMAND ----------

# Importações
from pyspark.sql.functions import current_date, current_timestamp, expr

# COMMAND ----------

#Nome do database
database = "bronze"
tabela = "estabelecimentos"

#path
caminho_arquivo = 'dbfs:/FileStore/Ampev/estabelecimentos.csv'

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load(caminho_arquivo)

# COMMAND ----------

# Importações
from pyspark.sql.functions import current_date, current_timestamp, expr

# COMMAND ----------

# incluir colunas de controle
df = df.withColumn("data_carga", current_date())
df = df.withColumn("data_hora_carga", expr("current_timestamp() - INTERVAL 3 HOURS"))

# COMMAND ----------

# Grava os dados no formato Delta
df.write \
    .format('delta') \
    .mode('overwrite') \
    .option('mergeSchema', 'true') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f'{database}.{tabela}')
print("Dados gravados com sucesso!")
