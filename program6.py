from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, when, lower

class Program6:
    def __init__(self, spark):
        self.spark = spark
    def Car_crashed(self):
        """zip code with Heighest number of crash with alcohol"""
        df1 = self.spark.read.csv(r"C:\Users\Saurav\Desktop\Project\Data\Units_use.csv",header=True)
        df1 = df1.filter(lower(col("VEH_BODY_STYL_ID")).contains('car')) \
                 .withColumn("combine_factr_alcohol", when((col("CONTRIB_FACTR_1_ID") == "UNDER INFLUENCE - ALCOHOL") | (col("CONTRIB_FACTR_2_ID") == "UNDER INFLUENCE - ALCOHOL") | (col("CONTRIB_FACTR_P1_ID") == "UNDER INFLUENCE - ALCOHOL"), "UNDER INFLUENCE - ALCOHOL")
                 .otherwise(col("CONTRIB_FACTR_1_ID")))
        
        df2 = self.spark.read.csv(r"C:\Users\Saurav\Desktop\Project\Data\Primary_Person_use.csv",header=True)
        df2 = df2.withColumnRenamed("CRASH_ID", "CRASH_ID_2")
        df_join = df1.join(df2, df1.CRASH_ID == df2.CRASH_ID_2, "left")
        
        
        df_join = df_join.groupBy("DRVR_ZIP", "combine_factr_alcohol").count()
        
        window = Window.partitionBy("DRVR_ZIP").orderBy(col("count").desc())
        df_join = df_join.withColumn("seq_rnk", dense_rank().over(window)) \
                         .filter((col("combine_factr_alcohol") == "UNDER INFLUENCE - ALCOHOL") & (col("seq_rnk") == 1))
        
        df_join.select("DRVR_ZIP").limit(5).show()