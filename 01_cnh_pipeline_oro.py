# PASO 1: Mostrar archivos cargados

display(dbutils.fs.ls("dbfs:/FileStore/tables"))

# PASO 2: Librerías necesarias
from pyspark.sql.functions import col, round, sum, avg, year, desc, countDistinct
from pyspark.sql.window import Window
import os

# PASO 3: Crear carpetas necesarias
paths = [
    "dbfs:/FileStore/datalake/mini_proyecto_cnh/bronze/",
    "dbfs:/FileStore/datalake/mini_proyecto_cnh/silver/",
    "dbfs:/FileStore/datalake/mini_proyecto_cnh/gold/",
    "dbfs:/FileStore/datalake/mini_proyecto_cnh/export/"
]
for path in paths:
    dbutils.fs.mkdirs(path)

# PASO 4: Cargar CSV

df_aceite_raw = spark.read.option("header", True).option("inferSchema", True).csv("dbfs:/FileStore/tables/aceite_condensado.csv")
df_gas_raw = spark.read.option("header", True).option("inferSchema", True).csv("dbfs:/FileStore/tables/gas_natural.csv")

# PASO 5: Guardar como Parquet - Bronze

df_aceite_raw.write.mode("overwrite").parquet("dbfs:/FileStore/datalake/mini_proyecto_cnh/bronze/aceite_condensado.parquet/")
df_gas_raw.write.mode("overwrite").parquet("dbfs:/FileStore/datalake/mini_proyecto_cnh/bronze/gas_natural.parquet/")

# PASO 6: Leer desde Bronze

df_aceite = spark.read.parquet("dbfs:/FileStore/datalake/mini_proyecto_cnh/bronze/aceite_condensado.parquet/")
df_gas = spark.read.parquet("dbfs:/FileStore/datalake/mini_proyecto_cnh/bronze/gas_natural.parquet/")

# PASO 7: Guardar como Parquet - Silver

df_aceite_limpio.write.mode("overwrite").parquet("dbfs:/FileStore/datalake/mini_proyecto_cnh/silver/aceite_condensado.parquet/")
df_gas_limpio.write.mode("overwrite").parquet("dbfs:/FileStore/datalake/mini_proyecto_cnh/silver/gas_natural.parquet/")

# PASO 8: Leer desde Silver

df_aceite_limpio = spark.read.parquet("dbfs:/FileStore/datalake/mini_proyecto_cnh/silver/aceite_condensado.parquet/")
df_gas_limpio = spark.read.parquet("dbfs:/FileStore/datalake/mini_proyecto_cnh/silver/gas_natural.parquet/")

# PASO 9: Verificación de ceros antes de limpiar
print("Aceite - Registros con producción cero:")
df_aceite_limpio.filter(col("petroleo_mbd") == 0).display()

print("Gas - Registros con producción cero:")
df_gas_limpio.filter(col("gas_natural_sin_nitrogeno_mmpcd") == 0).display()

# PASO 10: Eliminar ceros en producción
df_aceite_limpio = df_aceite_limpio.filter(col("petroleo_mbd") > 0)
df_gas_limpio = df_gas_limpio.filter(col("gas_natural_sin_nitrogeno_mmpcd") > 0)

# PASO 11: Generar columna de año desde la fecha
df_aceite_limpio = df_aceite_limpio.withColumn("anio", year(col("fecha")))
df_gas_limpio = df_gas_limpio.withColumn("anio", year(col("fecha")))

# PASO 12: Cálculo de insights (Gold)

# Producción total por año
df_total_anual_petroleo = df_aceite_limpio.groupBy("anio").agg(
    round(sum("petroleo_mbd"), 2).alias("total_petroleo_mbd")
)

df_total_anual_gas = df_gas_limpio.groupBy("anio").agg(
    round(sum("gas_natural_sin_nitrogeno_mmpcd"), 2).alias("total_gas_mmpcd")
)

# Promedio por operador
df_promedio_operador_petroleo = df_aceite_limpio.groupBy("operador").agg(
    round(avg("petroleo_mbd"), 2).alias("promedio_petroleo_mbd")
)

df_promedio_operador_gas = df_gas_limpio.groupBy("operador").agg(
    round(avg("gas_natural_sin_nitrogeno_mmpcd"), 2).alias("promedio_gas_mmpcd")
)

# Top 10 campos productores
df_top10_campos_petroleo = df_aceite_limpio.groupBy("campo", "cuenca", "operador").agg(
    round(sum("petroleo_mbd"), 2).alias("total_petroleo_mbd")
).orderBy(desc("total_petroleo_mbd")).limit(10)

df_top10_campos_gas = df_gas_limpio.groupBy("campo", "cuenca", "operador").agg(
    round(sum("gas_natural_sin_nitrogeno_mmpcd"), 2).alias("total_gas_mmpcd")
).orderBy(desc("total_gas_mmpcd")).limit(10)

# Operadores distintos
df_operadores_distintos_petroleo = df_aceite_limpio.select("operador").distinct()
df_operadores_distintos_gas = df_gas_limpio.select("operador").distinct()

# PASO 13: Operador top 1 anual por volumen de gas

df_operador_anual_gas = df_gas_limpio.groupBy("anio", "operador").agg(
    round(sum("gas_natural_sin_nitrogeno_mmpcd"), 2).alias("total_gas_mmpcd")
)

window_anio_gas = Window.partitionBy("anio").orderBy(desc("total_gas_mmpcd"))

df_top1_operador_anual_gas = (
    df_operador_anual_gas
    .withColumn("rank", row_number().over(window_anio_gas))
    .filter(col("rank") == 1)
    .drop("rank")
    .orderBy(desc("total_gas_mmpcd"))
)

display(df_top1_operador_anual_gas)

# PASO 14: Operador top 1 anual por volumen de petróleo

df_operador_anual = df_aceite_limpio.groupBy("anio", "operador").agg(
    round(sum("petroleo_mbd"), 2).alias("total_petroleo_mbd")
)

window_anio = Window.partitionBy("anio").orderBy(desc("total_petroleo_mbd"))

df_top1_operador_anual = (
    df_operador_anual
    .withColumn("rank", row_number().over(window_anio))
    .filter(col("rank") == 1)
    .drop("rank")
    .orderBy(desc("total_petroleo_mbd"))
)

display(df_top1_operador_anual)

# PASO 15: Guardado final en capa Gold

# Aceite - operador top 1 anual
df_top1_operador_anual.write.mode("overwrite").parquet("dbfs:/FileStore/datalake/mini_proyecto_cnh/gold/top1_operador_petroleo_anual.parquet")

# Gas - operador top 1 anual
df_top1_operador_anual_gas.write.mode("overwrite").parquet("dbfs:/FileStore/datalake/mini_proyecto_cnh/gold/top1_operador_gas_anual.parquet")

# Datos limpios completos

df_aceite_limpio.write.mode("overwrite").parquet("dbfs:/FileStore/datalake/mini_proyecto_cnh/gold/aceite_limpio.parquet")
df_gas_limpio.write.mode("overwrite").parquet("dbfs:/FileStore/datalake/mini_proyecto_cnh/gold/gas_limpio.parquet")

# Mostrar nuevamente resultados

display(df_top1_operador_anual)
display(df_top1_operador_anual_gas)
