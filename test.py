from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
print('before spark')
spark = SparkSession \
    .builder.getOrCreate()
print('spark made')
# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", "22222") \
    .load()
print('lines made')
# Split the lines into words
# words = lines.select(
#    explode(
#        split(lines.value, " ")
#    ).alias("word")
# )

# Generate running word count
# wordCounts = words.groupBy("word").count()

 # Start running the query that prints the running counts to the console
query = lines \
    .writeStream \
    .outputMode(outputMode = "append") \
    .format(source = "console") \
    .start()
print('query made')
query.awaitTermination()