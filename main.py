# 1. загрузить данные из файла в датафрейм
# 2. исправить ошибку в данных (колонка Age )
# 3. разделить клиентов на группы и подсчитать среднее значение   для каждой группы
# 4. результат расчетов записать в новый датафрейм
# 5. в   создать колонку gender_code , в которую поместить значение 1 (для клиентов мужского пола ) или 0 (для
# клиентов женского пола)
# 6. сохранить датафрейм в формате parquet


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as f
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("mall_customers").master("local").getOrCreate()

mall_customers_schema = StructType(fields=[
    StructField("CustomerID", StringType()),
    StructField("Gender", StringType()),
    StructField("Age", IntegerType()),
    StructField("Annual Income (k$)", IntegerType()),
    StructField("Spending Score (1-100)", IntegerType()),
])

mall_customersDB = (spark.read
                   .format("csv")
                   .option("header", "true")
                   .schema(mall_customers_schema)
                   .load("/Users/zheneva/Desktop/1 df files/mall_customers.csv"))

incomeDF = mall_customersDB.groupBy("Gender").agg(f.avg("Annual Income (k$)"))

incomeDF = incomeDF.withColumn("gender_code", f.when(col("Gender") == "Male", 1).otherwise(0))

mall_customersDB.show(5)
mall_customersDB.printSchema()
incomeDF.show(3)

incomeDF.write.parquet("incomeDF", mode="append")
