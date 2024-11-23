import re
from typing import List, Tuple
from pyspark.sql import SparkSession
from memory_profiler import profile


@profile
def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    # rango unicode emojis
    emoji_pattern = (
        r"[\U0001F600-\U0001F64F]|"  # Emoticons
        r"[\U0001F300-\U0001F5FF]|"  # Miscellaneous Symbols and Pictographs
        r"[\U0001F680-\U0001F6FF]|"  # Transport and Map Symbols
        r"[\U00002600-\U000026FF]|"  # Miscellaneous Symbols
        r"[\U00002700-\U000027BF]|"  # Dingbats
        r"[\U0001F700-\U0001F77F]|"  # Alchemical Symbols
        r"[\U0001F780-\U0001F7FF]|"  # Geometric Shapes Extended
        r"[\U0001F800-\U0001F8FF]|"  # Supplemental Arrows-C
        r"[\U0001F900-\U0001F9FF]|"  # Supplemental Symbols and Pictographs
        r"[\U0001FA00-\U0001FA6F]|"  # Chess Symbols
        r"[\U0001FA70-\U0001FAFF]"   # Symbols and Pictographs Extended-A
    )

    # modificadores tono de piel
    skin_tone_modifiers = r"[\U0001F3FB-\U0001F3FF]"

    spark = SparkSession.builder.appName("TopEmojis-MemoryOptimized").getOrCreate()

    # Lee json
    df = spark.read.option("encoding", "UTF-8").json(file_path)

    def extract_emojis(content):
        content = re.sub(skin_tone_modifiers, "", content)
        return re.findall(emoji_pattern, content)

    rdd = df.select("content").rdd.flatMap(lambda row: extract_emojis(row["content"]))

    emoji_counts = (
        rdd.filter(lambda emoji: emoji)
        .map(lambda emoji: (emoji, 1))
        .reduceByKey(lambda a, b: a + b)
    )

    # Toma top 10
    top_10_emojis = emoji_counts.takeOrdered(10, key=lambda x: -x[1])

    spark.stop()

    return top_10_emojis
