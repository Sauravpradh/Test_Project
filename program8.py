from pyspark.sql.functions import col

class Program8:
    def __init__(self, spark):
        self.spark = spark
    def Speed_offense(self):
        df1 = self.spark.read.csv(r"C:\Users\Saurav\Desktop\Project\Data\Charges_use.csv",header=True)
        df2 = self.spark.read.csv(r"C:\Users\Saurav\Desktop\Project\Data\Primary_Person_use.csv",header=True)
        df3 = self.spark.read.csv(r"C:\Users\Saurav\Desktop\Project\Data\Units_use.csv",header=True)
        
        """To Find the Driver having license with speed offence"""
        df_speed = df1.filter(col("CHARGE").contains("SPEED")).withColumnRenamed("CRASH_ID", "CRASH_ID_1")
        df_driver = df2.filter((col("PRSN_TYPE_ID") == 'DRIVER') & (col("DRVR_LIC_TYPE_ID") == 'DRIVER LICENSE'))
        df_speed_lic = df_driver.join(df_speed, df_driver.CRASH_ID == df_speed.CRASH_ID_1, "inner").drop('CRASH_ID_1')
        
        df_colour = df3.groupBy("VEH_COLOR_ID").count().orderBy(col("count").desc()).limit(10)
        
        """Car License with top State with offence"""
        df_top_state = df1.join(df3, df1.CRASH_ID == df3.CRASH_ID, "inner")
        df_top_state = df_top_state.groupBy("VEH_LIC_STATE_ID").count().withColumnRenamed("count","offense_count").orderBy(col("offense_count").desc()).limit(25)
        df_car_lic = df3.filter(col("VEH_BODY_STYL_ID").contains("CAR"))
        df_car_state  = df_car_lic.join(df_top_state, df_car_lic.VEH_LIC_STATE_ID == df_top_state.VEH_LIC_STATE_ID, "inner") \
                                  .withColumnRenamed("CRASH_ID", "CRASH_ID_1")
        
        df_combine = df_speed_lic.join(df_car_state, df_speed_lic.CRASH_ID == df_car_state.CRASH_ID_1, "inner").drop('CRASH_ID_1') \
                                 .select("CRASH_ID", "VEH_COLOR_ID", "VEH_MAKE_ID", "VEH_MOD_ID")
        df_final = df_combine.join(df_colour, df_combine.VEH_COLOR_ID == df_colour.VEH_COLOR_ID, "inner") \
                             .groupBy("VEH_MAKE_ID").count().orderBy(col("count").desc()) \
                             .select('VEH_MAKE_ID') \
                             .limit(5)
        
        df_final.show()