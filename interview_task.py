# Databricks notebook source
# MAGIC %fs
# MAGIC
# MAGIC  dbfs:/FileStore/mayuri_task

# COMMAND ----------

{
	"content": {
			"Fyear": "2024",
			"Srno": "1",
			"CatName": "Petrol",
			"Location": "MYS",
			"Uom": "KL",
			"Apr": "27.442",
			"May": "31.569",
			"June": "25.421",
			"July": "26.443",
			"Aug": "23.148",
			"Sep": "0.000",
			"Oct": "0.000",
			"Nov": "0.000",
			"Decm": "0.000",
			"Jan": "0.000",
			"Feb": "0.000",
			"March": "0.000",
			"Total": "53.000"
	}
}
 
Transformed data:
[
{
Location: MYS, 
Code: “Petrol”,
Month: 3, 
Year: 2023,
Value: 27.442,
Unit: “KL”
},
{
Location: MYS,
Code: “Petrol”,
Month: 4,
Year: 2023,
Value: 31.569,
Unit: “KL”
},
.
.
.
] 



# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

c_l  = df.columns
cf = dict([(field.name, field.dataType) for field in df.schema.fields if type(field.dataType)==StructType])
# print('cf is',cf)
col_name = list(cf.keys())[0]
print(col_name)
expanded = [col(col_name+'.'+k).alias(k) for k in [n.name for n in cf[col_name]]]
print('exapnded format is:',expanded)
expanded_df = df.select("*",*expanded).drop(col_name)
expanded_df = expanded_df.withColumnRenamed("CatName","Code").withColumnRenamed("Fyear","Year").withColumnRenamed("Uom","Unit")

months = ["Apr", "May", "June", "July", "Aug", "Sep", "Oct", "Nov", "Decm", "Jan", "Feb", "March"]

reshaped_df = expanded_df.selectExpr(
    "Location",
    "Code",
    "CAST(2023 AS INT) AS Year",  
    "stack(12, 'Apr', Apr, 'May', May, 'June', June, 'July', July, 'Aug', Aug, 'Sep', Sep, 'Oct', Oct, 'Nov', Nov, 'Decm', Decm, 'Jan', Jan, 'Feb', Feb, 'March', March) as (Month, Value)"
)
reshaped_df.display()

reshaped_df = reshaped_df.withColumn("Value", col("Value").cast(FloatType()))

reshaped_df.display()

result = reshaped_df.collect()
result_list = [row.asDict() for row in result]
for item in result_list:
    print(item,end="\n")


# COMMAND ----------


