# Databricks notebook source
from pyspark.sql.functions import *

# #configuration to source(adls)
spark.conf.set(
  'fs.azure.account.key.chaitanyacapastonelake.dfs.core.windows.net'
  , 'GQKn2LMcrK0dO3JAztLvuTV7EB8U882u0Fpm7PIGgvKztlNwssE22fhZ4piD0/5hHdwlpH2Zzb9W+ASt+nkXyA=='
)



# COMMAND ----------

#read the parquet file from adls
source_file="abfss://refined@chaitanyacapastonelake.dfs.core.windows.net"
df_source=spark.read\
    .format('parquet')\
    .option('inferschema',True)\
    .load(source_file)  

adf=df_source

# COMMAND ----------

# Apply transformations

target_df = adf.withColumn("date", when(col("date").isNull(), "2020-11-28").otherwise(col("date")))
target_df = target_df.filter(col("url") != "ERROR")
   

# COMMAND ----------

#webscraping to fill acronym in place of country

import requests
from bs4 import BeautifulSoup

url="https://travel.state.gov/content/travel/en/us-visas/visa-information-resources/fees/country-acronyms.html"
response1=requests.get(url)

# Check request was successful or not
if response1.status_code == 200:
  html_content = response1.content
else:
 print("Failed to fetch the website.")

# Parse HTML content with BeautifulSoup
soup = BeautifulSoup(html_content,'html.parser')

# we need to import table
table1=soup.find('table')

#make dictionary
country_acronym={}

#run the loop
for rows in table1.find_all('tr'):
  column=rows.find_all('td')
  if len(column)>=2:
    country_name=column[0].text.strip()
    acronym=column[1].text.strip()
    country_acronym[country_name]=acronym

#covert the column 'country' of transformed table to upper-case
target_adf=target_df.withColumn("country",upper(col("country")))

# Create a DataFrame from the acronym data
acronym_df = spark.createDataFrame(country_acronym.items(), ["country", "acronym"])

# Join the original DataFrame with the acronym DataFrame to replace values
adf1 = target_adf.join(acronym_df, "country", "left").select("*")

# Interchange the positions of two columns (e.g., swap "Age" and "Country") and then drop country table
result_df = adf1.withColumnRenamed("country", "temp").withColumnRenamed("acronym", "country").withColumnRenamed("temp", "acronym").drop("acronym")


# Show the updated DataFrame
display(result_df)

#result_df.printSchema()
#result_df.printSchema()

# COMMAND ----------

#authenticating ADB with SQLDB

# Details about connection string
Servername = "sqlserver-capstone.database.windows.net"
databaseName = "SqlDb-capstone"
tableName = "PROJ.HOSPITAL_ADMISSIONS"
userName = "capstone"
password = "Sql12345" # Please specify password here
jdbc_Url = "jdbc:sqlserver://{0}:{1};database={2}".format(Servername, 1433, databaseName)
connectionProperties = {
  "user" : userName,
  "password" : password,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

#write the table into sql-db
result_df.write.jdbc(jdbc_Url,  
                   mode ="append", 
                   table=tableName, 
                   properties=connectionProperties)



#show table
#display(adf)

# COMMAND ----------

#read data from sql-db
adf1= spark.read.jdbc(jdbc_Url,  
                   table=tableName, 
                   properties=connectionProperties)

# COMMAND ----------

display(adf1)

# COMMAND ----------


