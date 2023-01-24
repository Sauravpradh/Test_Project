from pyspark.sql.functions import col
from pyspark.sql.functions import lower, split

class Program7:
    def __init__(self, spark):
        self.spark = spark
    def No_damage(self):
        df1 = self.spark.read.csv(r"C:\Users\Saurav\Desktop\Project\Data\Damages_use.csv",header=True)
        df1 = df1.filter(col("DAMAGED_PROPERTY").contains("NO DAMAGE"))
        
        df2 = self.spark.read.csv(r"C:\Users\Saurav\Desktop\Project\Data\Units_use.csv",header=True)
        df2 = df2.withColumn("VEH_DMAG_SCL_1_ID", split(col("VEH_DMAG_SCL_1_ID"), " ")) \
                 .withColumn("VEH_DMAG_NEW", col("VEH_DMAG_SCL_1_ID").getItem(1)) \
                 .filter((col("VEH_DMAG_NEW") > 4) & (lower(col("VEH_BODY_STYL_ID")).contains('car')) & (col("FIN_RESP_TYPE_ID").contains("INSURANCE")) ) \
                 .withColumnRenamed("CRASH_ID", "CRASH_ID_2")
            
        df_join = df1.join(df2, df1.CRASH_ID == df2.CRASH_ID_2, "inner")
        df_join = df_join.dropDuplicates(['CRASH_ID'])
        df_join.select('CRASH_ID').show()
        
        