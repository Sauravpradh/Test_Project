from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

class Program3:
    def __init__(self, spark):
        self.spark = spark
    def State_number(self):
        """To find count for state have highest female involve in accident"""
        df = self.spark.read.csv(r"C:\Users\Saurav\Desktop\Project\Data\Primary_Person_use.csv",header=True)
        df = df.groupBy("DRVR_LIC_STATE_ID", "PRSN_GNDR_ID").count()
        windows = Window.partitionBy("DRVR_LIC_STATE_ID").orderBy(col("count").desc())
        df = df.withColumn("seq", dense_rank().over(windows)) \
               .filter((col("PRSN_GNDR_ID") == "FEMALE") & (col("seq") == 1))
        
        df.select('DRVR_LIC_STATE_ID').show()