#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, DateType, TimestampType
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


# ### Create config and load Neo4j Spark Connector jar

# In[7]:


neo4j_conf = {
    "user":"neo4j",
    "password":"password123",
    "url":"bolt://neodb:7687",
    "database":"testdb",
    "batch_size":5000
}

spark = SparkSession \
    .builder \
    .appName("testdb") \
    .config("spark.jars", "./lib/neo4j-connector-apache-spark_2.12-4.1.2_for_spark_3.jar") \
    .getOrCreate()



# ### Method to write to Neo4j

# In[8]:


def ingest_to_neo4j(df, query):
    """
    This function ingests a pyspark dataframe using the "query"
    Params
    df (pyspark.sql.DataFrame): data to ingest
    query (string): defines the cypher query for ingestion
    neo4j_conf (dict): neo4j configuration details
    schema_script (string): defines the neo4j schema if any to create before
        ingestion
    repartition (bool): if set to true the dataframe will be repartitioned with 
        1 partition to avoid locking issues that can result from parallel 
        execution of cypher queries against neo4j. parallelism can be 
        introduced, but the data must be suitable for it. 
    """
        
    # For the mode option, ErrorIfExists/Append will generate a CREATE query
    # Overwite will generate a MERGE query
    # this is not relevant here since we define a custom query
    df.write.format("org.neo4j.spark.DataSource")\
    .mode("Overwrite")\
    .option("url", neo4j_conf["url"])\
    .option("database", neo4j_conf["database"])\
    .option("authentication.type", "basic")\
    .option("authentication.basic.username", neo4j_conf["user"])\
    .option("authentication.basic.password", neo4j_conf["password"])\
    .option("batch.size", neo4j_conf["batch_size"])\
    .option("query", query)\
    .save()


# ### Create dataframes from parquet files

# In[9]:


parquet_df = spark.read.option("inferSchema", True, )\
    .parquet("./parquet/")
#parquet_df.show()


# ### Create queries and pass to method to write data with dataframe

# #### Create nodes

# In[10]:


customer_query = """
WITH event.Customer_ID AS customer_id,
event.Credit_Score AS credit_score,
event.Loan_ID AS loan_number

MERGE (m:Customer {id: customer_id}) 
SET 
    m.creditScore = credit_score,
    m.loanNumber = loan_number
"""


# In[11]:


ingest_to_neo4j(parquet_df, customer_query)


# In[9]:


loan_query = """
WITH event.Loan_ID AS loan_number,
event.Loan_Amount AS loan_amount

MERGE (m:Loan {loan_number: loan_number}) 
SET 
    m.loan_amount = loan_amount
"""


# In[10]:


ingest_to_neo4j(parquet_df, loan_query)


# #### Build relationships between nodes

# In[11]:


customer_to_loan_query = """
WITH event.Customer_ID AS customer_id,
event.Loan_ID AS loan_number

MATCH (c:Customer {id: customer_id})
MATCH (l:Loan {loan_number: loan_number}) 
MERGE (c)-[:HAS_LOAN]->(l)
"""


# In[13]:


ingest_to_neo4j(parquet_df, customer_to_loan_query)


# In[ ]:





