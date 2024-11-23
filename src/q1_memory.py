from typing import List, Tuple
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, rank, broadcast
from pyspark.sql.window import Window
from memory_profiler import profile


@profile
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    spark = None
    try:
        # Crea sesión de Spark
        spark = SparkSession.builder.appName("Top10Tweets").getOrCreate()

        # Carga los datos en dataframe
        try:
            df = spark.read.option("encoding", "UTF-8").json(file_path)
        except Exception as e:
            raise ValueError(f"Error leyendo el archivo JSON: {e}")

        # Selecciona solo columnas necesarias y valores no nulos
        df = df.select(col("date"), col("user.username")).filter(
            col("date").isNotNull() & col("username").isNotNull()
        )
        #df2.printSchema()


        # Filtra registros con valores nulos en date y username
        #df = df.filter(col("date").isNotNull() & col("user.username").isNotNull())

        # Si el dataframe está vacío después del filtrado, retorna una lista vacía
        if df.rdd.isEmpty():
            return []

        # se crea columna tweet_date como tipo date
        df = df.withColumn("tweet_date", col("date").cast("date"))

        # Obtiene Cantidad de tweets por día
        date_counts = df.groupBy("tweet_date").agg(count("*").alias("tweet_count"))

        # Obiene Top 10 fechas con más tweets
        top_dates = date_counts.orderBy(col("tweet_count").desc()).limit(10)

        # Obtiene numero de Tweets por usuario en cada día
        user_tweet_counts = df.groupBy("tweet_date", "username").agg(count("*").alias("tweet_count_user"))

        # join con top 10 fechas
        top_users = user_tweet_counts.join(broadcast(top_dates), on="tweet_date")

        # Rank para encontrar el usuario con más tweets en cada fecha
        window_spec = Window.partitionBy("tweet_date").orderBy(col("tweet_count_user").desc())
        ranked_users = top_users.withColumn("rank", rank().over(window_spec))

        # Usuario con más tweets por fecha
        result = ranked_users.filter(col("rank") == 1).select("tweet_date", "username", "tweet_count_user")

        # Armar lista de tuplas
        result_list = result.take(10)  # Solo tomar las primeras 10 filas

        # Formatear la salida para que sea como una lista de tuplas con la fecha y el usuario
        formatted_result = [(row["tweet_date"], row["username"]) for row in result_list]

        return formatted_result

    except Exception as e:
        print(f"Error procesando los datos: {e}")
        return []
    finally:
        if spark is not None:
            spark.stop()
