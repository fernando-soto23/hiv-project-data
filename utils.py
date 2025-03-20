from pyspark.sql import DataFrame,SparkSession

def write_to_uc_table(
    input_df: DataFrame, 
    table_name: str, 
    mode: str = "overwrite", 
    catalog: str = "catalog_de", 
    schema: str = "bronze"
):
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    schema = schema 
    full_table_name = f"{catalog}.{schema}.{table_name}"
    input_df.write.format("delta") \
        .mode(mode) \
        .option("overwriteSchema", "true") \
        .saveAsTable(full_table_name)
    print(f"Successfully wrote data to {full_table_name}")

