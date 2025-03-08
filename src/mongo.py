from pyspark.sql import SparkSession

# Initialisation de Spark avec le connecteur MongoDB
spark = SparkSession.builder \
    .appName('MongoDBTest') \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.1.1') \
    .getOrCreate()

# Données de test simples
data = [("Alice", 25), ("Bob", 30)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# Écriture dans MongoDB avec le bon format
df.write \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .mode("append") \
    .option("uri", "mongodb://localhost:27017/user_db.test_collection") \
    .save()

print("Données écrites dans MongoDB (user_db.test_collection)")

# Lecture depuis MongoDB pour vérification
loaded_df = spark.read \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .option("uri", "mongodb://localhost:27017/user_db.test_collection") \
    .load()

loaded_df.show()

# Arrêt de Spark
spark.stop()