# Databricks notebook source
# MAGIC %md
# MAGIC ## Informações Gerais
# MAGIC  | Informações | Detalhes |
# MAGIC  |------------|-------------|
# MAGIC  |Nome Tabela | silver.pedidos |
# MAGIC  |Origem | bronze.pedidos / bronze.estabelecimentos |
# MAGIC
# MAGIC ## Histórico de Atualizações
# MAGIC  | Data | Desenvolvido por | Motivo |
# MAGIC  |:----:|--------------|--------|
# MAGIC  |16/07/2024 | Patrick Diorio  | Criação do notebook |
# MAGIC  |20/08/2024 | Patrick Diorio  | inclusão coluna valor |

# COMMAND ----------

# Importações
from pyspark.sql.functions import current_date, current_timestamp, expr

# COMMAND ----------

# Funções
#Função que aplica os comentários na tabela
def adicionaComentariosTabela(a,b,c,d):
    spark.sql(f"COMMENT ON TABLE {a}.{b} IS '{c}'")
    for key,value in d.items():
        sqlaux = f"ALTER TABLE {a}.{b} CHANGE COLUMN {key} COMMENT '{value}'"
        spark.sql(sqlaux)

# COMMAND ----------

database = "silver"
tabela = "pedidos"

# COMMAND ----------

df_pedidos = spark.sql(
f"""
WITH pedidos AS (
    SELECT
        PedidoID AS id_pedido,
        EstabelecimentoID AS id_estabelecimento,
        Produto AS produto,
        quantidade_vendida AS quantidade,
        Preco_Unitario AS preco,
        data_venda AS data_pedido
    FROM
        bronze.pedidos
),

estabelecimentos AS (
    SELECT
        Local AS nome_estabelecimento,
        Email AS email, 
        EstabelecimentoID AS id_estabelecimento,
        Telefone AS telefone
    FROM
        bronze.estabelecimentos
)
SELECT
    CAST(ped.id_pedido AS INT),
    CAST(ped.id_estabelecimento AS INT),
    ped.produto,
    CAST(ped.quantidade AS INT),
    CAST(ped.preco AS DECIMAL(20,2)),
    est.nome_estabelecimento,
    est.email,
    est.telefone,
    CAST(ped.data_pedido AS DATE)
FROM
    pedidos ped
LEFT JOIN
    estabelecimentos est
ON ped.id_estabelecimento = est.id_estabelecimento

"""
)

# COMMAND ----------

# incluir colunas de controle
df_pedidos = df_pedidos.withColumn("data_carga", current_date())
df_pedidos = df_pedidos.withColumn("data_hora_carga", expr("current_timestamp() - INTERVAL 3 HOURS"))

# COMMAND ----------

#Comentario Tabela
comentario_tabela = 'Esta tabela é uma entidade corporativa que contém a relação dos pedidos'

lista_comentario_colunas = {
'id_pedido' : 'Id do pedido.',
'id_estabelecimento' :  'Id do estabelecimento.',
'produto' : 'Nome do produto.',
'quantidade' : 'quantidade do pedido.',
'preco' : 'Perço do produto unitario.',
'nome_estabelecimento' : 'Nome do estabelecimento.',
'email' :  'E-mail do estabelecimento.',
'telefone' : 'Telefone do estabelecimento.',
'data_pedido' : 'Data do pedido.',
'data_carga' : 'Data que o registro foi carregado',
'data_hora_carga' : 'Data e hora que o registro foi carregado'
}

# COMMAND ----------

df_pedidos.write \
          .format('delta') \
          .mode('overwrite') \
          .clusterBy("id_pedido") \
          .option('overwriteSchema', 'true') \
          .saveAsTable(f'{database}.{tabela}')
adicionaComentariosTabela(database, tabela, comentario_tabela, lista_comentario_colunas)
print("Dados gravados com sucesso!")

# COMMAND ----------

spark.sql(f"OPTIMIZE {database}.{tabela}")
print(f"Processo de otimização finalizado!.")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail silver.pedidos
