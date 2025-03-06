from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, concat_ws, current_timestamp, avg, count
from pyspark.sql.types import StringType, IntegerType, ArrayType, StructType, StructField, TimestampType

# Initialisation de SparkSession avec les connecteurs nécessaires
spark = SparkSession.builder \
    .appName('UserDataStreaming') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,'
            'org.mongodb.spark:mongo-spark-connector_2.12:10.1.1,'
            'com.datastax.spark:spark-cassandra-connector_2.12:3.4.1') \
    .config('spark.mongodb.output.uri', 'mongodb://localhost:27017/user_db.raw_users') \
    .config('spark.cassandra.connection.host', 'localhost') \
    .getOrCreate()

# Schéma complet des données JSON (basé sur Random User API)
schema = ArrayType(StructType([
    StructField("gender", StringType(), True),
    StructField("name", StructType([
        StructField("title", StringType(), True),
        StructField("first", StringType(), True),
        StructField("last", StringType(), True)
    ]), True),
    StructField("location", StructType([
        StructField("country", StringType(), True)
    ]), True),
    StructField("dob", StructType([
        StructField("age", IntegerType(), True)
    ]), True),
    StructField("login", StructType([
        StructField("uuid", StringType(), True)
    ]), True)
]))

# a) Connexion à Kafka
kafka_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'user-stream') \
    .option('startingOffsets', 'latest') \
    .load()

# Conversion de la colonne value (string) en JSON structuré
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("users")
)

# c) Explosion du tableau d'utilisateurs
exploded_df = parsed_df.select(explode(col("users")).alias("user"))

# d) Enrichissements
enriched_df = exploded_df.select(
    col("user.login.uuid").alias("user_id"),
    col("user.gender").alias("gender"),
    concat_ws(" ", col("user.name.first"), col("user.name.last")).alias("full_name"),
    col("user.name.first").alias("first_name"),
    col("user.name.last").alias("last_name"),
    col("user.dob.age").alias("age"),
    col("user.location.country").alias("country"),
    current_timestamp().alias("ingestion_time")
)

# e) Calcul des agrégations (par pays)
agg_df = enriched_df.groupBy("country").agg(
    count("*").alias("count_users"),
    avg("age").alias("avg_age")
)

# f) Fonction pour écrire dans MongoDB et Cassandra dans un foreachBatch
def write_to_sinks(batch_df, batch_id):
    try:
       

        # Écriture dans Cassandra (random_user_table)
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="random_user_table", keyspace="user_keyspace") \
            .save()
        print(f"Batch {batch_id} écrit dans random_user_table (Cassandra).")

        # Agrégations dans Cassandra (country_stats)
        batch_agg_df = batch_df.groupBy("country").agg(
            count("*").alias("count_users"),
            avg("age").alias("avg_age")
        ).withColumn("last_update", current_timestamp())
        batch_agg_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="country_stats", keyspace="user_keyspace") \
            .save()
        print(f"Batch {batch_id} d'agrégations écrit dans country_stats (Cassandra).")
    except Exception as e:
        print(f"Erreur lors de l'écriture du batch {batch_id} : {str(e)}")
        import traceback
        traceback.print_exc()  # Affiche la trace complète
# Streaming Query pour les données enrichies
enriched_query = enriched_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_sinks) \
    .option("checkpointLocation", "/tmp/kafka-checkpoint-enriched") \
    .start()

# Streaming Query pour afficher les agrégations en console (optionnel, pour debug)
agg_query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("checkpointLocation", "/tmp/kafka-checkpoint-agg") \
    .start()

# Attendre la fin des streams
enriched_query.awaitTermination()
agg_query.awaitTermination()