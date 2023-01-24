from pyspark.sql import SparkSession
from program1 import Accident
from program2 import Crash
from program3 import Program3
from program4 import Program4
from program5 import Program5
from program6 import Program6
from program7 import Program7
from program8 import Program8

spark = SparkSession \
    .builder \
    .appName("Vechicle Application") \
    .getOrCreate()

Object_a = Accident(spark)
Object_a.Number_accident()

Object_b = Crash(spark)
Object_b.Number_crash()

Object_c = Program3(spark)
Object_c.State_number()

Object_d = Program4(spark)
Object_d.top_vehicle()

Object_d = Program5(spark)
Object_d.Ethnic()

Object_e = Program6(spark)
Object_e.Car_crashed()

Object_f = Program7(spark)
Object_f.No_damage()

Object_g = Program8(spark)
Object_g.Speed_offense()


    