# Databricks notebook source
# MAGIC %md
# MAGIC order

# COMMAND ----------



from pyspark.sql import SparkSession

# Iniciar a Spark Session (com suporte a Hive se necessário)
spark = SparkSession.builder \
    .appName("CSV to Spark Table") \
    .getOrCreate()

# Caminho para o arquivo CSV
csv_path = "dbfs:/FileStore/tables/case_ifood/order-2.csv"

# Ler o CSV
order_df = spark.read.option("header", "true").option("inferSchema", "true").option("sep", ";").csv(csv_path)

display(order_df)


# COMMAND ----------

# MAGIC %md
# MAGIC rest

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CSV to Spark Table") \
    .getOrCreate()

csv_rest = "dbfs:/FileStore/tables/case_ifood/restaurant.csv"


restaurant_df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_rest)

display(restaurant_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ab test

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CSV to Spark Table") \
    .getOrCreate()

csv_ab = "dbfs:/FileStore/tables/case_ifood/ab_test_ref.csv"
ab_test_df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_ab)

display(ab_test_df)

# COMMAND ----------

# MAGIC %md
# MAGIC client

# COMMAND ----------


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CSV to Spark Table") \
    .getOrCreate()

csv_cons = "dbfs:/FileStore/tables/case_ifood/consumer_.csv"

consumer_df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_cons)

display(consumer_df)

# COMMAND ----------

print("Número de linhas:", order_df.count())
print("Número de colunas:", len(order_df.columns))

from pyspark.sql.functions import count

# Contar CPFs que aparecem mais de uma vez
pedidos_duplicates_df = order_df.groupBy("order_id").agg(count("*").alias("order_id_count")).filter("order_id > 1")
pedidos_duplicates_count = pedidos_duplicates_df.count()

# Total de CPFs únicos
pedidos_unique_count = order_df.select("order_id").distinct().count()

print(f"\nTotal de pedidos duplicados: {pedidos_duplicates_count} de {pedidos_unique_count} únicos")



# COMMAND ----------

from pyspark.sql.functions import count, round, col

total = ab_test_df.count()
ab_dist = (
    ab_test_df.groupBy("is_target")
    .agg(count("*").alias("qtd"))
    .withColumn("percentual", round((col("qtd") / total) * 100, 2))
)

ab_dist.show()

# COMMAND ----------

from pyspark.sql.functions import count

# CPFs com mais de 1 ocorrência
cpf_dupes = (
    order_df.groupBy("cpf")
    .agg(count("*").alias("qtd_pedidos"))
    .filter("qtd_pedidos > 1")
)
order_with_dup_cpfs = order_df.join(cpf_dupes, on="cpf", how="inner")

order_with_dup_cpfs.select("cpf", "customer_id", "order_id", "order_created_at", "order_total_amount", "qtd_pedidos") \
    .orderBy("cpf", "order_created_at") \
    .show(50, truncate=False)

# COMMAND ----------

print(order_df.columns)


# COMMAND ----------


cols_to_drop = [
    'origin_platform',
    'customer_name',
    'delivery_address_latitude',
    'delivery_address_external_id',
    'delivery_address_longitude',
    'delivery_address_zip_code',
    'items', 
    'merchant_latitude',
    'merchant_longitude'
]

existing_cols_to_drop = [col for col in cols_to_drop if col in order_df.columns]

order_df_clean = order_df.drop(*existing_cols_to_drop)

print("\nColunas após limpeza:")
order_df_clean.printSchema()


# COMMAND ----------

display(order_df_clean)
display(ab_test_df);

# COMMAND ----------

# Join entre pedidos e teste A/B
order_ab_df = order_df_clean.join(ab_test_df, on='customer_id', how='inner')

display(order_ab_df)

# COMMAND ----------

print("Número de linhas:", order_ab_df.count())
print("Número de colunas:", len(order_ab_df.columns))

# COMMAND ----------

from pyspark.sql.functions import col

# Contar todos os pedidos
total_pedidos = order_df_clean.count()

order_ab_df2 = order_df_clean.join(ab_test_df, on='customer_id', how='left')
participantes = order_ab_df2.filter(col("is_target").isNotNull()).count()

print(f"Total de pedidos: {total_pedidos}")
print(f"Pedidos com is_target preenchido: {participantes}")

# COMMAND ----------

order_fora_ab_df = order_ab_df.filter(col("is_target").isNull())

print("Pedidos sem marcação de grupo A/B:", order_fora_ab_df.count())

display(order_fora_ab_df)

# COMMAND ----------

print(order_ab_df.columns)
print(consumer_df.columns)

# COMMAND ----------

#usar inner para termos os dados corretos
order_with_consumer_df = order_ab_df.join(
    consumer_df,
    on=order_ab_df.customer_id == consumer_df.customer_id,
    how='left'
).select(
    order_ab_df["customer_id"],  
       order_ab_df["order_id"],  
             order_ab_df["delivery_address_country"], 
              order_ab_df["delivery_address_district"], 
               order_ab_df["delivery_address_state"], 
                order_ab_df["merchant_id"],  
                 order_ab_df["order_created_at"],  
                  order_ab_df["order_total_amount"],  
                   order_ab_df["is_target"],  
        consumer_df["created_at"],  
            consumer_df["active"] ) 
print("Número de linhas:", order_with_consumer_df.count())
print("Número de colunas:", len(order_with_consumer_df.columns))


# COMMAND ----------

print(restaurant_df.columns)

# COMMAND ----------

order_final_df = order_with_consumer_df.join(
    restaurant_df,
    on=order_with_consumer_df.merchant_id == restaurant_df.id,
    how='left'
).select(
    order_with_consumer_df["*"],  
    restaurant_df["id"].alias("restaurant_id"), 
        restaurant_df["price_range"],
             restaurant_df["average_ticket"],
              restaurant_df["delivery_time"],
               restaurant_df["minimum_order_value"],
                restaurant_df["average_ticket"]
)
display(order_final_df)
print("Número de linhas:", order_final_df.count())
print("Número de colunas:", len(order_final_df.columns))

# COMMAND ----------

print("Pedidos totais na base final:", order_final_df.count())
print("Pedidos únicos:", order_final_df.select("order_id").distinct().count())
print("Usuários únicos:", order_final_df.select("customer_id").distinct().count())

# COMMAND ----------

import pandas as pd  
import numpy as np   

sample_df = order_final_df.select("*").toPandas()    #.sample(fraction=0.3, seed=42).toPandas()

sample_df['order_total_amount'] = pd.to_numeric(sample_df['order_total_amount'], errors='coerce')


sample_df = sample_df.dropna(subset=['order_total_amount', 'is_target'])

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Gráfico
plt.figure(figsize=(6, 5))
sns.barplot(
    data=df_perf,
    x='grupo',
    y='ticket_medio',
    palette={'Control': 'gray', 'Target': 'red'}
)

# Rótulos nas barras
for index, row in df_perf.iterrows():
    plt.text(x=index, y=row['ticket_medio'] + 1, s=f"R$ {row['ticket_medio']:.2f}", ha='center')

# Estilo
plt.title('Ticket Médio por Grupo – Target vs Control')
plt.ylabel('Ticket Médio (R$)')
plt.xlabel('Grupo')
plt.ylim(0, df_perf['ticket_medio'].max() + 10)
plt.tight_layout()
plt.show()


# COMMAND ----------

# Garantir que os campos necessários não tenham nulos
df_limp = sample_df.dropna(subset=['customer_id', 'order_id', 'is_target'])

# Padronizar grupo se necessário
df_limp['is_target'] = df_limp['is_target'].astype(str).str.lower()
df_limp['grupo'] = df_limp['is_target'].map({'0': 'Control', '1': 'Target', 'control': 'Control', 'target': 'Target'})

# Contar clientes únicos que fizeram pedidos por grupo
clientes_com_pedido = df_limp.groupby('grupo')['customer_id'].nunique().reset_index()
clientes_com_pedido.columns = ['Grupo', 'Clientes Únicos com Pedido']

# Exibir
print(clientes_com_pedido)

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

# Agrupamento
agrupado = sample_df.groupby('is_target').agg(
    qtd_clientes=('customer_id', pd.Series.nunique),
    valor_total=('order_total_amount', 'sum')
).reset_index()

agrupado['valor_total_mil'] = agrupado['valor_total'] / 1000

labels = agrupado['is_target']
qtd_clientes = agrupado['qtd_clientes']
valor_total_mil = agrupado['valor_total_mil']

x = np.arange(len(labels))
width = 0.35

fig, ax = plt.subplots(figsize=(8, 5))

# Cores: cinza para clientes, vermelho para valor
bars1 = ax.bar(x - width/2, qtd_clientes, width, label='Qtd. de Clientes', color='gray')
bars2 = ax.bar(x + width/2, valor_total_mil, width, label='Valor Total (mil R$)', color='red')

# Eixos e rótulos
ax.set_xlabel('Grupo')
ax.set_ylabel('Valores')
ax.set_title('Clientes e Valor Total por Grupo (Control vs Target)')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()

# Rótulos no topo
for bar in bars1 + bars2:
    yval = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2, yval, f'{yval:,.0f}', ha='center', va='bottom')

plt.tight_layout()
plt.show()


# COMMAND ----------

# Limpar dados
df_limp = sample_df.dropna(subset=['is_target', 'order_id', 'order_total_amount'])

# Agrupamento por grupo com pedidos únicos
df_perf = df_limp.groupby('is_target').agg(
    pedidos_unicos=('order_id', 'nunique'),
    valor_total=('order_total_amount', 'sum')
).reset_index()

# Calcular ticket médio com base em pedidos únicos
df_perf['ticket_medio'] = df_perf['valor_total'] / df_perf['pedidos_unicos']

# Mapear nomes dos grupos se necessário
df_perf['grupo'] = df_perf['is_target'].map({'target': 'Target', 'control': 'Control'})

# Exibir resultado
print(df_perf[['grupo', 'pedidos_unicos', 'valor_total', 'ticket_medio']])

# COMMAND ----------

df_estado = df_limp.groupby(['delivery_address_state', 'is_target']).agg(
    qtd_pedidos=('order_id', 'count'),
    valor_total=('order_total_amount', 'sum'),
    clientes=('customer_id', 'nunique')
).reset_index()  

# ticket médio
df_estado['ticket_medio'] = df_estado['valor_total'] / df_estado['qtd_pedidos']

df_estado['grupo'] = df_estado['is_target'].map({'control': 'Control', 'target': 'Target'})

print(df_estado.shape)
print(df_estado.head())


# COMMAND ----------

import pandas as pd


df_limp = sample_df.dropna(subset=['delivery_address_state', 'order_id', 'customer_id', 'is_target'])


df_limp['is_target'] = df_limp['is_target'].astype(str).str.lower()
df_limp['grupo'] = df_limp['is_target'].map({'0': 'Control', '1': 'Target', 'control': 'Control', 'target': 'Target'})


df_estado_agregado = df_limp.groupby(['delivery_address_state', 'grupo']).agg(
    clientes_unicos=('customer_id', 'nunique'),
    pedidos_unicos=('order_id', 'nunique')
).reset_index()

clientes_pivot = df_estado_agregado.pivot_table(
    index='delivery_address_state',
    columns='grupo',
    values='clientes_unicos',
    aggfunc='sum'
).fillna(0).astype(int).reset_index()

pedidos_pivot = df_estado_agregado.pivot_table(
    index='delivery_address_state',
    columns='grupo',
    values='pedidos_unicos',
    aggfunc='sum'
).fillna(0).astype(int).reset_index()

clientes_pivot.columns.name = None
pedidos_pivot.columns.name = None

clientes_pivot = clientes_pivot.rename(columns={
    'Control': 'Clientes Control',
    'Target': 'Clientes Target'
})

pedidos_pivot = pedidos_pivot.rename(columns={
    'Control': 'Pedidos Control',
    'Target': 'Pedidos Target'
})


tabela_estado_final = pd.merge(clientes_pivot, pedidos_pivot, on='delivery_address_state')

tabela_estado_final = tabela_estado_final.sort_values(by='Clientes Target', ascending=False)




# COMMAND ----------

display(tabela_estado_final)

# COMMAND ----------


df_limp = sample_df.dropna(subset=['delivery_address_state', 'order_id', 'customer_id', 'is_target'])

df_limp['is_target'] = df_limp['is_target'].astype(str).str.lower()
df_limp['grupo'] = df_limp['is_target'].map({'0': 'Control', '1': 'Target', 'control': 'Control', 'target': 'Target'})
estado_para_regiao = {
    'SP': 'Sudeste', 'RJ': 'Sudeste', 'MG': 'Sudeste', 'ES': 'Sudeste',
    'RS': 'Sul', 'SC': 'Sul', 'PR': 'Sul',
    'DF': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste',
    'BA': 'Nordeste', 'PE': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste', 'PB': 'Nordeste',
    'RN': 'Nordeste', 'AL': 'Nordeste', 'SE': 'Nordeste', 'PI': 'Nordeste',
    'AM': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte', 'AC': 'Norte', 'AP': 'Norte'
}

df_limp['regiao'] = df_limp['delivery_address_state'].map(estado_para_regiao)

df_regiao_agregado = df_limp.groupby(['regiao', 'grupo']).agg(
    clientes_unicos=('customer_id', 'nunique'),
    pedidos_unicos=('order_id', 'nunique')
).reset_index()

clientes_pivot = df_regiao_agregado.pivot_table(
    index='regiao',
    columns='grupo',
    values='clientes_unicos',
    aggfunc='sum'
).fillna(0).astype(int).reset_index()

pedidos_pivot = df_regiao_agregado.pivot_table(
    index='regiao',
    columns='grupo',
    values='pedidos_unicos',
    aggfunc='sum'
).fillna(0).astype(int).reset_index()


clientes_pivot.columns.name = None
pedidos_pivot.columns.name = None

clientes_pivot = clientes_pivot.rename(columns={
    'Control': 'Clientes Control',
    'Target': 'Clientes Target'
})

pedidos_pivot = pedidos_pivot.rename(columns={
    'Control': 'Pedidos Control',
    'Target': 'Pedidos Target'
})

tabela_regiao_final = pd.merge(clientes_pivot, pedidos_pivot, on='regiao')

tabela_regiao_final = tabela_regiao_final.sort_values(by='Clientes Target', ascending=False)
print(tabela_regiao_final)


# COMMAND ----------

display(tabela_regiao_final)

# COMMAND ----------


ticket_por_regiao = df_limp.groupby(['regiao', 'grupo']).agg(
    pedidos=('order_id', 'nunique'),
    valor_total=('order_total_amount', 'sum')
).reset_index()
ticket_por_regiao['ticket_medio'] = ticket_por_regiao['valor_total'] / ticket_por_regiao['pedidos']


# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

plt.figure(figsize=(10, 6))

sns.barplot(
    data=ticket_por_regiao,
    x='regiao',
    y='ticket_medio',
    hue='grupo',
    palette={'Control': 'gray', 'Target': 'red'}
)

plt.title('Ticket Médio por Região – Target vs Control')
plt.xlabel('Região')
plt.ylabel('Ticket Médio (R$)')
plt.xticks(rotation=45)
plt.legend(title='Grupo')
plt.tight_layout()
plt.show()
