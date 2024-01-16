from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, first, row_number
from pyspark.sql import Window

spark = SparkSession.builder.appName("SparkTopCountCBSA").getOrCreate()

csv_path = "gs://todb/tedsa_puf_2000_2019.csv"
df = spark.read.csv(csv_path, header=True, inferSchema=True)
df = df[df['CBSA2010'] != -9]
grouped_df = df.groupBy("CBSA2010").count()
top5_df = grouped_df.sort(['Count'], ascending = False).limit(5)
top5_df.show()

admission_df = df.groupBy(['CBSA2010', 'ADMYR']).count()
admission_df.show()

admission_df.createOrReplaceTempView("admission")
top5_df.createOrReplaceTempView("top5")

ad_query = 'select * from admission where CBSA2010 in (select CBSA2010 from top5)'
top5admission_df = spark.sql(ad_query)
top5admission_df.show()

minWindow = Window.partitionBy("CBSA2010").orderBy(col("count"))
rankedAdmission_df = top5admission_df.withColumn("Rank", row_number().over(minWindow))
result_df = rankedAdmission_df.filter(col("Rank") == 1).drop("Rank")
result_df = result_df[['CBSA2010', 'ADMYR']]
result_df.show()

spark.stop()