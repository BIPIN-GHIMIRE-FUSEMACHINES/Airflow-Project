from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import os
from dotenv import load_dotenv
import os

load_dotenv()

pg_user = os.getenv("POSTGRES_USER")
pg_password = os.getenv("POSTGRES_PW")


spark = SparkSession.builder.appName("airflow").getOrCreate()

table_names = ["basketball_data"]
table_dataframes = {}

jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
connection_properties = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}

for table_name in table_names:
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
    table_dataframes[table_name] = df

main_df = table_dataframes["basketball_data"]
main_df.show()

filtered_df = result = df.groupBy("name").alias("team_name").agg(count("*").alias("Count_of_players"))

filtered_df.show()

filtered_df.write.parquet("/home/bipin/airflow/data/basketball_clean.parquet")


filtered_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'solution_df', user=pg_user,password=pg_password).mode('overwrite').save()
