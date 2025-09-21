# Databricks notebook source
# MAGIC %md
# MAGIC ## Informações Gerais
# MAGIC  | Informações | Detalhes |
# MAGIC  |------------|-------------|
# MAGIC  |Nome Tabela | gold.venda_total_estabelecimentos |
# MAGIC  |Origem | silver.pedidos |
# MAGIC
# MAGIC ## Histórico de Atualizações
# MAGIC  | Data | Desenvolvido por | Motivo |
# MAGIC  |:----:|--------------|--------|
# MAGIC  |16/07/2024 | Patrick Diorio  | Criação do notebook |

# COMMAND ----------

# Importações
from pyspark.sql.functions import current_date, current_timestamp, expr

# COMMAND ----------

# Nome do esquema e tabela no catálogo
database = "gold"
tabela = "venda_total_estabelecimentos"

# COMMAND ----------

# Funções

#Função que aplica os comentários na tabela
def adicionaComentariosTabela(a,b,c,d):
    spark.sql(f"COMMENT ON TABLE {a}.{b} IS '{c}'")
    for key,value in d.items():
        sqlaux = f"ALTER TABLE {a}.{b} CHANGE COLUMN {key} COMMENT '{value}'"
        spark.sql(sqlaux)

# COMMAND ----------

df_pedidos = spark.sql(
f""" 
SELECT
    CAST(id_estabelecimento AS INT),
    nome_estabelecimento,
    produto AS nome_produto,
    CAST(SUM(quantidade) AS INT) AS quantidade_total,
    CAST(SUM(preco) AS DECIMAL(20,2)) AS valor_total
FROM
    silver.pedidos
GROUP BY all
""")

# COMMAND ----------

# incluir colunas de controle
df_pedidos = df_pedidos.withColumn("data_carga", current_date())
df_pedidos = df_pedidos.withColumn("data_hora_carga", expr("current_timestamp() - INTERVAL 3 HOURS"))

# COMMAND ----------

#Comentario Tabela

comentario_tabela = 'Entidade os produtos mais vendidos por estabelecimento'

lista_comentario_colunas = {
'id_estabelecimento' : 'Id do estabelecimento.',
'nome_estabelecimento' : 'Nome do estabelecimento.',
'nome_produto' : 'Nome do produto.',
'quantidade_total' : 'Quantidade total vendida.',
'valor_total' : 'Valor total vendido',
'data_carga' : 'Data que o registro foi carregado',
'data_hora_carga' : 'Data e hora que o registro foi carregado'
}

# COMMAND ----------

# Salvar o DataFrame no formato Delta
df_pedidos.write \
          .format('delta') \
          .mode('overwrite') \
          .clusterBy("id_estabelecimento") \
          .option('overwriteSchema', 'true') \
          .saveAsTable(f'{database}.{tabela}')
adicionaComentariosTabela(database, tabela, comentario_tabela, lista_comentario_colunas)
print("Dados gravados com sucesso!")

# COMMAND ----------

# Otimização das tabelas
spark.sql(f"OPTIMIZE {database}.{tabela}")
print(f"Processo de otimização finalizado!.")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from gold.venda_total_estabelecimentos
# MAGIC order by quantidade_total desc
# MAGIC limit 5
