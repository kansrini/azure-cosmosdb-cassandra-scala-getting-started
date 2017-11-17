package com.azure.cosmosdb.cassandra.scala.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._

object AppConfig {

  val config: Config = ConfigFactory.load()
  val keyspace = getConf("cosmosdb.cassandra.schema")
  val orderTable = getConf("cosmosdb.cassandra.orderItemTable")
  val cacheTable = getConf("cosmosdb.cassandra.cacheTable")
  var cassandraHost = "127.0.0.1";
  var cassandraPort = "10350";
  var cassandraUsername = "localhost";
  var cassandraPassword = "defaultpassword";
  var trustStorePath = "/path/to/trust/store";
  var trustStorePassword = "changeit";
  var enabledAlgorithms = "TLS_RSA_WITH_AES_128_CBC_SHA";


  /**
    * Utility method to create SparkContext with Cassandra endpoint configuration    *
    *
    * @return
    */
  def getCassandraSparkContext(): SparkContext = {
    loadCassandraEndpointConfigurations()

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.connection.port", cassandraPort)
      .set("spark.cassandra.auth.username", cassandraUsername)
      .set("spark.cassandra.auth.password", cassandraPassword)
      .set("spark.cassandra.connection.ssl.enabled", "true")
      .set("spark.cassandra.connection.ssl.trustStore.path", trustStorePath)
      .set("spark.cassandra.connection.ssl.trustStore.password", trustStorePassword)
      .set("spark.cassandra.connection.ssl.enabledAlgorithms", enabledAlgorithms)
      .setMaster("local")
      .setAppName("cassandra-on-cosmosdb-scala-examples")

    new SparkContext(conf)
  }

  def loadCassandraEndpointConfigurations(): Unit = {
    cassandraHost = getConf("spark.cassandra.connection.host")
    cassandraPort = getConf("spark.cassandra.connection.port")
    cassandraUsername = getConf("spark.cassandra.auth.username")
    cassandraPassword = getConf("spark.cassandra.auth.password")
    //Load SSL Configurations
    enabledAlgorithms = getConf("spark.cassandra.auth.ssl.algorithm")
    trustStorePath = getConf("spark.cassandra.auth.ssl.filepath")
    trustStorePassword = getConf("spark.cassandra.auth.ssl.password")
  }

  /**
    * Utility method to get he property value  for a key configured in application.conf
    *
    * @param key property key
    * @return property value
    */
  def getConf(key: String): String = {
    config.getString(key)
  }
}
