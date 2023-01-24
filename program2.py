from pyspark.sql.functions import col

class Crash:
    def __init__(self, spark):
        self.spark = spark
    def Number_crash(self):
        """Filtering MotorCycle"""
        df = self.spark.read.csv(r"C:\Users\Saurav\Desktop\Project\Data\Units_use.csv",header=True)
        df = df.filter((col("VEH_BODY_STYL_ID") == "MOTORCYCLE"))
        val = df.count()
        print(val)