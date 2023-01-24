from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class Accident:
    def __init__(self, spark):
        self.spark = spark
    def Number_accident(self):
        """"To find Male encouter with death"""
        df = self.spark.read.csv(r"C:\Users\Saurav\Desktop\Project\Data\Primary_Person_use.csv",header=True)
        df = df.filter((col("PRSN_GNDR_ID") == "MALE") & (col("DEATH_CNT") == 1)) \
               .dropDuplicates(["CRASH_ID"])
        val = df.count()
        print(val)