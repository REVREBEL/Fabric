# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f266312a-87c6-47eb-aff9-fd34597688fb",
# META       "default_lakehouse_name": "DEVREBEL_Data",
# META       "default_lakehouse_workspace_id": "6fc7a30e-b793-4185-b9d5-2cbe481efae7",
# META       "known_lakehouses": [
# META         {
# META           "id": "f266312a-87c6-47eb-aff9-fd34597688fb"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import lit

# Replace with your Lakehouse schema (if using Unity Catalog, this includes catalog.db)
schema_name = "dbo"

# List of tables you want to include
tables = ["CoStar_DayStar", "Demand_Channel", "Demand_Segment", "Demand_Property",  "Pace_RoomType", "Pace_Segment", "Pace_Property"]  # You can automate this via catalog API if needed

metadata_list = []

for table in tables:
    df = spark.sql(f"DESCRIBE {schema_name}.{table}") \
              .withColumn("TABLE_NAME", lit(table)) \
              .withColumn("TABLE_SCHEMA", lit(schema_name))
    
    # Filter to retain only actual column metadata
    df = df.filter("col_name NOT LIKE '#%' AND col_name NOT IN ('', 'Partitioning')") \
           .selectExpr("TABLE_SCHEMA", "TABLE_NAME", "col_name as COLUMN_NAME", 
            "data_type as DATA_TYPE", 
            "row_number() OVER (PARTITION BY TABLE_NAME ORDER BY col_name) as ORDINAL_POSITION")
    
    metadata_list.append(df)

# Union all metadata
final_df = metadata_list[0]
for df in metadata_list[1:]:
    final_df = final_df.unionByName(df)

# Optionally create a view or table
final_df.createOrReplaceTempView("vw_TableColumnMetadata")  # Temporary view

# Or persist it
final_df.write.mode("overwrite").saveAsTable("vw_TableColumnMetadata")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
