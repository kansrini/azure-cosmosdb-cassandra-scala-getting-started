---
services: Cassandra API On Cosmos-DB
platforms: scala
author: kansrini
---

# azure-cosmosdb-cassandra-scala-getting-started
Azure Cosmos DB is Microsoft’s globally distributed multi-model database service. You can quickly create and query document, key/value, graph databases, and cassandra databases, all of which benefit from the global distribution and horizontal scale capabilities at the core of Azure Cosmos DB.

One of the supported APIs on Azure Cosmos DB is the Cassandra API, which provides a document model and support for client drivers in many platforms. This sample shows you how to use the Azure Cosmos DB with the Cassandra DB API to store and access data through datastax spark-cassandra-connector using scala programs. It is transparent to the application that the data is stored in Azure Cosmos DB.

## Pre-requesits
- SBT (>= 1.0.2)
- JDK (>= 1.8)
- Git

## Running this sample

* Before you can run this sample, you must have the following prerequisites:

   * An active Azure account. Create a CosmosDB with Cassandra endpoint API
   * Then, clone this repository using `git clone https://github.com/kansrini/azure-cosmosdb-cassandra-scala-getting-started.git`.
   * Create the Keyspace in the CosmosDB Cassandra endpoint. Command details are given in azure-cosmosdb-cassandra-scala-examples/src/main/resources/cassandra-on-cosmosdb-setup.cql
   * Provide the following configuration in azure-cosmosdb-cassandra-scala-examples/src/main/resources/application.conf
	```
		{
		  "cosmosdb": {
			"cassandra": {
			  "schema": "<Cassandra keyspace name, default value: scalatest>",
			  "orderItemTable": "<OrderItem table name, default value: order_item>",
			  "cacheTable": "<Cache table name, default value: cache_data>"
			}
		  }
		  "spark": {
			"cassandra": {
			  "connection": {
				"host": "<Cassandra endpoint connection host>",
				"port": "<Cassandra endpoint connection port>",
			  }
			  "auth": {
				"username": "<Cassandra endpoint username>",
				"password": "<Cassandra endpoint password>",
				"ssl":
					  {
						"filepath": "<Keystore filepath>"
						"password": "<keystore password>"
						"algorithm": "TLS_RSA_WITH_AES_128_CBC_SHA"
					  }
			  }
			}
		  }
		}
	
	```

* Run the application by hitting the command 'sbt run' from azure-cosmosdb-cassandra-scala-getting-started directory

* This will show multiple options of example as follows
	```
		 Multiple main classes detected, select one to run:

		 [1] com.azure.cosmosdb.cassandra.scala.CassandraTableLevelTTLExample
		 [2] com.azure.cosmosdb.cassandra.scala.DeleteCassandraDataExample
		 [3] com.azure.cosmosdb.cassandra.scala.InsertCassandraDataExample
		 [4] com.azure.cosmosdb.cassandra.scala.SelectCassandraDataExample
		 [5] com.azure.cosmosdb.cassandra.scala.UpdateCassandraTableExample
	```
* Enter the example (number) you want to run and hit 'Enter'

## About the code
The code included in this sample is intended to get you quickly started with scala that connects to Azure Cosmos DB with the Cassandra API.

##Steps to configure HDInsight Spark Cluster to use Cassandra API on CosmosDB

You can run above examples on a Spark cluster also. Here are the steps to configure a HDInsight Spark cluster with spark-cassandra-connector

	1. Create a HDInsight Spark Cluster of version 3.6
	2. Go to Ambari UI -> Spark2 -> Configs -> Custom Spark2 Defaults
	3. Add a new property 
		```
		spark.jars.packages=datastax:spark-cassandra-connector:<version>
		```
		Example: spark.jars.packages=datastax:spark-cassandra-connector:2.0.5-s_2.11
	4. Save the Configuration
	5. Restart SPARK
Once this setup is done, you can run the above scala examples in that cluster


## Running queries using CQLSH (Optional)

These steps can be used to validate the Cosmos-DB APIs via CQLSH shell

1.	Install Python 2.7 on your box.

2.	Download Cassandra binary & install from http://cassandra.apache.org/download/.

3.	Navigate to the bin\ directory of your cassandra install directory (extracted above) 

4.	Run the following: 
    ```
	set SSL_VERSION=TLSv1_2
	set SSL_VALIDATE=false
	set SSL_CERTFILE=[path-to-ssl-cer] 
	set CQLSH_PORT=10350 
    	cqlsh -u [nameOfCosmosDBAccount] -p [accountKeyOfCosmosDBAccount] --ssl
	```
    On Windows, run the following:
	```
    $python\python.exe "{Path to} cassandra\bin\cqlsh.py" <IPADDRESS> <PORT> --ssl --connect-timeout=600 --request-timeout=600 -u <accountName> -p <password> 
    ```
5.	This will spin up a shell on which you can execute CQL. 
    Create the keyspace & tables using the cql script given at azure-cosmosdb-cassandra-scala-examples/src/main/resources/cassandra-on-cosmosdb-setup.cql

6. Run your queries you intended to teston CQLSH

## More information

- [Azure Cosmos DB](https://docs.microsoft.com/azure/cosmos-db/introduction)
- [DataStax spark-cassandra-connector Documentation](https://academy.datastax.com/resources/getting-started-apache-spark-and-cassandra)
- [DataStax spark-cassandra-connector Source](https://github.com/datastax/spark-cassandra-connector/)
- [Scala](https://www.scala-lang.org/)
