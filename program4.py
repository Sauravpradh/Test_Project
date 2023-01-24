from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

class Program4:
    def __init__(self, spark):
        self.spark = spark
    def top_vehicle(self):
        """Veh_make with higest number of injuries and death"""
        df = self.spark.read.csv(r"C:\Users\Saurav\Desktop\Project\Data\Units_use.csv",header=True)
        df = df.withColumn("Injuries", col("TOT_INJRY_CNT")+col("DEATH_CNT")) \
               .groupBy("VEH_MAKE_ID").count()
        window =Window.orderBy(col("count").desc())
        df= df.withColumn("seq", dense_rank().over(window))
        df = df.filter((col("seq")>=5) & (col("seq") <= 15))
        
        df.select(col("VEH_MAKE_ID")).show()