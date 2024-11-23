from typing import List, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql import functions as F

def q3_memory(file_path: str) -> List[Tuple[str, int]]:

    spark = SparkSession.builder \
        .appName("TopInfluentialUsers") \
        .getOrCreate()

    # Cargar los datos en un DataFrame
    df = spark.read.option("encoding", "UTF-8").json(file_path)

    # Desempaquetar las menciones (assumiendo que 'mentionedUsers.username' es una lista)
    flattened_df = df.withColumn("mention", explode(col("mentionedUsers.username")))

    # Contar las menciones por usuario
    mention_count_df = flattened_df.groupBy("mention").agg(F.count("*").alias("mention_count"))

    # Ordenar por número de menciones y obtener los top 10 usuarios más influyentes
    top_users = mention_count_df.orderBy("mention_count", ascending=False).limit(10)

    result = top_users.collect()

    # Formatear la salida como una lista de tuplas (username, count)
    formatted_result = [(row["mention"], row["mention_count"]) for row in result]

    spark.stop()

    return formatted_result