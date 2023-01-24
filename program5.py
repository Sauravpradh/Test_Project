from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

class Program5:
    def __init__(self, spark):
        self.spark = spark
    def Ethnic(self):
        """Find top Ethnic group for each body style"""
        df1 = self.spark.read.csv(r"C:\Users\Saurav\Desktop\Project\Data\Primary_Person_use.csv",header=True)
        df2 = self.spark.read.csv(r"C:\Users\Saurav\Desktop\Project\Data\Units_use.csv",header=True)
        df2 = df2.withColumnRenamed("CRASH_ID", "CRASH_ID_2")
        df_join = df1.join(df2, df1.CRASH_ID == df2.CRASH_ID_2, "inner")
        df_join = df_join.groupBy("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID").count()
        
        window = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        df_join = df_join.withColumn("seq", dense_rank().over(window)) \
                         .filter(col("seq") == 1)
        df_join.select('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID').show(truncate = False)