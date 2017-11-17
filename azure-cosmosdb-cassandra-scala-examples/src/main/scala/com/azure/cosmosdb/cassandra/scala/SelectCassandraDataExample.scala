package com.azure.cosmosdb.cassandra.scala

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.ClusteringOrder
import com.azure.cosmosdb.cassandra.scala.util._

/**
  * This covers various select queries on the Azure CosmosDB Cassandra tables
  * - Select all rows from a partition
  * - Select few rows from a partition based on a condition on non partition columns
  * - Select data with limit
  * - Select data with ordering by clustering key
  * - Seclect limited rows from a partition
  */
object SelectCassandraDataExample {

  def main(args: Array[String]): Unit = {
    var sc = AppConfig.getCassandraSparkContext()

    try {
      //Recreating the keyspace
      Utils.recreateKeyspace(sc, AppConfig.keyspace);

      //Recreating the table
      Utils.recreateTable(sc, AppConfig.keyspace, AppConfig.orderTable)

      // insert rows in cassandra table
      insertOrderItemData(sc)

      //select all rows from a partition
      selectAllRowsFromPartition(sc)

      // select few rows from a partition
      selectPartialRowsFromPartition(sc)

      //Select with limit
      selectRowsFromPartitionWithLimit(sc)

      //Select with ordering
      selectRowsFromPartitionInOrder(sc)

      //Select rows from multiple partitions
      selectRowsFromMultiplePartitions(sc)

    } finally  {
      sc.stop()
    }
  }

  /**
    * Insert sample data to validate select queries
    * @param sc SparkContext
    */
  def insertOrderItemData(sc: SparkContext): Unit= {
    val collection = sc.parallelize(Seq(
      //partition 1
      (101, "Redmond", "Nixon Ray", "Apple", 1, 1.65, false),
      (101, "Redmond", "Nixon Ray", "Meat-1KG", 1, 20.2, true),
      (101, "Redmond", "Nixon Ray", "Windows Curtain", 5, 510, false),
      //parition 2
      (999, "Oslo", "Jussi Jane", "Vinegar-500ml", 1, 6.95, true),
      (999, "Oslo", "Jussi Jane", "Soap", 5, 18.5, false),
      (999, "Oslo", "Jussi Jane", "Bulb", 3, 31.4, true),
      (999, "Oslo", "Jussi Jane", "Washing Machine", 1, 954.0, true)
    ))
    collection.saveToCassandra(AppConfig.keyspace, AppConfig.orderTable, SomeColumns("id", "shop", "customer_name", "item", "quantity", "price", "coupon_used"))
  }

  /**
    * Validate select all rows from a partition
    * @param sc SparkContext
    */
  def selectAllRowsFromPartition(sc: SparkContext): Unit = {
    val rows = sc.cassandraTable(AppConfig.keyspace, AppConfig.orderTable).
      where("id=?", 101).where("shop=?", "Redmond")
      .collect()
    assert(rows.length == 3 )
  }

  /**
    * Validate select filtered rows from a partition.
    * Filtering is done based on a non-partitioned key
    * @param sc SparkContext
    */
  def selectPartialRowsFromPartition(sc: SparkContext): Unit = {
    val rows = sc.cassandraTable(AppConfig.keyspace, AppConfig.orderTable).
      where("id=?", 999).where("shop=?", "Oslo")
        .where("price >?",  70.5)
      .collect()

    assert(rows.length == 1 )
  }

  /**
    * Select rows from a partition with a limit set
    * @param sc SparkContext
    */
  def selectRowsFromPartitionWithLimit(sc: SparkContext): Unit = {
    //select only limited rows
    val rows = sc.cassandraTable(AppConfig.keyspace, AppConfig.orderTable).
      where("id=?", 999).where("shop=?", "Oslo")
      .limit(2).collect()

    assert(rows.length == 2)
  }

  /**
    * Select rows from a partition with order by set
    * @param sc SparkContext
    */
  def selectRowsFromPartitionInOrder(sc: SparkContext): Unit = {
    //select rows in desc order
    val descRows = sc.cassandraTable(AppConfig.keyspace, AppConfig.orderTable).
      where("id=?", 999).where("shop=?", "Oslo").clusteringOrder(ClusteringOrder.Descending)
      .collect()

    assert(descRows(0).getString("item").equals("Washing Machine"))

    //select rows in asc order
    val ascRows = sc.cassandraTable(AppConfig.keyspace, AppConfig.orderTable).
      where("id=?", 101).where("shop=?", "Redmond").clusteringOrder(ClusteringOrder.Ascending)
      .collect()

    //assert(ascRows(0).getString("item").equals("Apple"))

  }

  /**
    * Select rows from multiple partitions
    * @param sc SparkContext
    */
  def selectRowsFromMultiplePartitions(sc: SparkContext): Unit = {
    var connector = CassandraConnector.apply(sc.getConf)
    var session = connector.openSession()

    try {
      var resultSet = session.execute("select * from " + AppConfig.keyspace + "." + AppConfig.orderTable + " where quantity <= 1 ALLOW FILTERING")

      val iterator = resultSet.iterator()
      var rowCount = 0

      while (iterator.hasNext) {
        val row = iterator.next()
        if (row != null) {
          rowCount += 1
        }
      }
      //Ensure there are 4 rows with quantity =1
      assert(rowCount == 4)

    } finally {
      session.close()
    }
  }
}
