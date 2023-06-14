# Neo4j Spark Connector Notebook

This project comprises example code showing usage of the Neo4j Spark Connector. It includes:
- Reading data from a parquet file
- Creating two nodes
- Creating a relationship between those nodes

Once the notebook is installed in a spark environemnt (e.g. Databricks, EMR, etc.), update the neo4j credentials, hostname for the bolt url, and database name to connect to a test instance to run. These items are located under the neo4j_conf variable. One other item to update is the path to the neo4j spark connnector jar, which can be in an object store (e.g. s3), on disk with a relative or full path, or deployed as a library, if using Databricks.
