from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, max as spark_max, count, sum

spark = SparkSession.builder.appName("Pipeline Vehicle - Cleaning").master("spark://spark-master:7077").getOrCreate()
# spark = SparkSession.builder.appName("Pipeline Vehicle - Cleaning").master("local").getOrCreate()
df = spark.read.option("header","true").option("inferSchema","true").csv("/app/data/vehicule_data.csv")
print(df.show(5))
df.describe().show()
df_clean = df.filter((col("vitesse_kmh") >= 0) 
                     & (col("rpm") >= 0)
                     & (col("temperature_moteur").isNotNull()))
df_clean.describe().show()
df_enriched = df_clean.withColumn("vitesse_ms",col("vitesse_kmh")/3.6).withColumn("stress_moteur",when((col("rpm") > 4000) | (col("temperature_moteur")>100),"élevé").otherwise("normal")).withColumn("stress_numeric",when((col("rpm") > 4000) | (col("temperature_moteur")>100),1).otherwise(0))
df_enriched.show(5)

df_indicateur = df_enriched.groupby("vehicule_id").agg(avg(col("vitesse_kmh")).alias("average_vitesse_kmh"),
                                                      spark_max(col("temperature_moteur")).alias("max_temp_moteur"),
                                                      avg(col("rpm")).alias("average_rpm"),
                                                      count("*").alias("total_registery"),
                                                     sum(col("stress_numeric")).alias("nb_stress"))
df_indicateur.show()
df_indicateur = df_indicateur.withColumn("abnormal_comportment",when((col("nb_stress") > 3) | (col("average_rpm") > 4500) | (col("max_temp_moteur") > 110),"yes").otherwise("no"))
df_indicateur.show()

# Save parquet (When use locally, use master("local"))
df_clean.write.mode("overwrite").parquet("/app/output/clean.parquet")
df_enriched.write.mode("overwrite").parquet("/app/output/enriched.parquet")
df_indicateur.write.mode("overwrite").parquet("/app/output/indicateurs.parquet")
spark.stop()

