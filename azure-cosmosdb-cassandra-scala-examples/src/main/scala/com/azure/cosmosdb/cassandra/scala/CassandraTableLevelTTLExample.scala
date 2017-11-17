package com.azure.cosmosdb.cassandra.scala

import org.apache.spark._
import com.datastax.spark.connector._
import com.azure.cosmosdb.cassandra.scala.util._

case class cacheDataWithTTL(key: String, value: String)

/**
  * This validates the table level TTL set on the cassandra table and ensure data gets deleted automatically
  */
object CassandraTableLevelTTLExample {

  def main(args: Array[String]): Unit = {
    var sc = AppConfig.getCassandraSparkContext()

    //Recreating the keyspace
    Utils.recreateKeyspace(sc, AppConfig.keyspace);

    //Recreating the table
    Utils.recreateTable(sc, AppConfig.keyspace, AppConfig.cacheTable)

    //insert data into a table where default TTL is given as 10 sec
    insertCacheDataWithTTL(sc)

    //wait for 10 sec
    Thread.sleep(10000)

    // Validate that inserted data got wiped out
    checkCacheDataWiped(sc)

    sc.stop()
  }

  def insertCacheDataWithTTL(sc: SparkContext): Unit= {
    val collection = sc.parallelize(Seq(cacheDataWithTTL("key1", "value1"),
      cacheDataWithTTL("key2", "value2")
    ))

    collection.saveToCassandra(AppConfig.keyspace, AppConfig.cacheTable, SomeColumns("key", "value"))
  }

  def checkCacheDataWiped(sc: SparkContext): Unit = {
    val cacheData = sc.cassandraTable(AppConfig.keyspace, AppConfig.cacheTable).where("key=?", "key1").collect()
    assert(cacheData.length == 0)
  }
}