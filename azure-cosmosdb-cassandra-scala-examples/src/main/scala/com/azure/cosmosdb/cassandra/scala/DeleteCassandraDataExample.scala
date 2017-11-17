package com.azure.cosmosdb.cassandra.scala

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import com.azure.cosmosdb.cassandra.scala.util._

/**
  * Validates the deletion of rows or a particular column value on cassandra table
  */
object DeleteCassandraDataExample {

  def main(args: Array[String]): Unit = {
    var sc = AppConfig.getCassandraSparkContext()

    //Recreating the keyspace
    Utils.recreateKeyspace(sc, AppConfig.keyspace);

    //Recreating the table
    Utils.recreateTable(sc, AppConfig.keyspace, AppConfig.orderTable)

    // Test to delete the rows in cassandra table
    insertOrderItemData(sc)
    validateRowsCount(sc, 3)
    deleteOrderItemData(sc)
    validateRowsCount(sc, 0)

    //Test to delete column data in cassandra table
    insertOrderItemData(sc)
    validateRowsCount(sc, 3)
    deleteColumnData(sc)
    validateColumnDataDeletion(sc)
    deleteOrderItemData(sc)

    sc.stop()
  }

  /**
    * Insert sample data to validate delete operation
    * @param sc SparkContext
    */

  def insertOrderItemData(sc: SparkContext): Unit= {
    val collection = sc.parallelize(Seq(
      (101, "Redmond", "Nixon Ray", "Apple", 1, 1.65),
      (101, "Redmond", "Nixon Ray", "Meat-1KG", 1, 20.2),
      (101, "Redmond", "Nixon Ray", "Windows Curtain", 3, 510)
    ))
    collection.saveToCassandra(AppConfig.keyspace, AppConfig.orderTable, SomeColumns("id", "shop", "customer_name", "item", "quantity", "price"))
  }

  /**
    * Deletes data only on a coulmn for a given partition
    * @param sc SparkContext
    */
  def deleteColumnData(sc: SparkContext): Unit= {
    sc.parallelize(Seq((101, "Redmond", "Apple")))
      .deleteFromCassandra(AppConfig.keyspace, AppConfig.orderTable, SomeColumns("customer_name"))
  }

  /**
    * Deletes all the rows in a partition of a cassandra table
    * @param sc SparkContext
    */
  def deleteOrderItemData(sc: SparkContext): Unit= {
    sc.cassandraTable(AppConfig.keyspace, AppConfig.orderTable)
      .where("id=?", 101).where("shop=?", "Redmond")
      .deleteFromCassandra("scalatest", "order_item")
  }

  /**
    * Validate whether particular column data got deleted for a given partition
    * @param sc
    */
  def validateColumnDataDeletion(sc: SparkContext): Unit = {
    val rows = sc.cassandraTable(AppConfig.keyspace, AppConfig.orderTable).
      where("id=?", 101).where("shop=?", "Redmond").where("item=?", "Apple")
      .collect()
    //Assert that value got deleted
    assert(rows(0).getStringOption("customer_name") == None)
  }


  def validateRowsCount(sc: SparkContext, expectedRowCount: Int): Unit = {
    val rows = sc.cassandraTable(AppConfig.keyspace, AppConfig.orderTable).
      where("id=?", 101).where("shop=?", "Redmond")
      .collect()
    assert(rows.length == expectedRowCount)
  }
}
